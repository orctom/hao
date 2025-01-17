# -*- coding: utf-8 -*-
"""
####################################################
###########         dependency          ############
####################################################

####################################################
###########         config.yml          ############
####################################################
rmq:
  default:
    host: localhost
    port: 7001

####################################################
###########          usage              ############
####################################################
from hao.rmq import RMQ
rmq = RMQ()
queue_name = 'dummy'

# queue size
print(rabbit.queue_size(queue_name))

# publish, accepts string, dict, list
for i in range(0, 10):
    rabbit.publish(queue_name, f'hello-{i}')
print(rabbit.queue_size(queue_name))

# consume
for msg in rabbit.consume(queue_name, timeout=1):
    print(msg)
"""
import atexit
import enum
import select
import socket
import struct
import threading
import time
from dataclasses import dataclass
from typing import Dict, Optional, Union

from . import config, jsons, logs

LOGGER = logs.get_logger(__name__)


_MAGIC = b'<RMQ>'


def _int_2_bytes(i: int, size: int = 1):
    return i.to_bytes(size, 'big')


class Priority(enum.Enum):
    LOW = _int_2_bytes(0)
    MEDIUM = _int_2_bytes(1)
    HIGH = _int_2_bytes(2)

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_

    def __str__(self):
        return self.name[0]


class Event(enum.Enum):
    GET = _int_2_bytes(1)
    PUT = _int_2_bytes(2)
    ACK = _int_2_bytes(3)
    STAT = _int_2_bytes(4)

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class OK(enum.Enum):
    FALSE = _int_2_bytes(0)
    TRUE = _int_2_bytes(1)

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


class Msg:
    def __init__(self, event: Event, *, payload_size: int = 0, payload: bytes = None, ok: OK = OK.TRUE):
        self.event = event
        self.ok = ok
        self.payload_size = payload_size if payload is None else len(payload)
        self.payload = payload

    def __str__(self) -> str:
        return f"[{self.event.name}] <{self.ok.name}> {self.payload_size}"

    def __repr__(self) -> str:
        return self.__str__()

    def encode(self) -> bytes:
        assert self.payload is not None
        return struct.pack(
            '>5sccI',
            _MAGIC,
            self.event.value,
            self.ok.value,
            self.payload_size,
        ) + self.payload

    @classmethod
    def from_bytes(cls, data: bytes) -> 'Msg':
        if len(data) == 0:
            raise RMQError("Server lost")
        if len(data) != 11:
            raise RMQDataError(f"Invalid message size: {len(data)}")
        magic, event, ok, payload_size = struct.unpack('>5sccI', data[:11])
        if magic != _MAGIC:
            raise RMQDataError(f"Invalid magic number {magic}")
        if not Event.has_value(event):
            raise RMQDataError("Invalid event value")
        if not OK.has_value(ok):
            raise RMQDataError("Invalid OK value")
        msg = cls(Event(event), payload_size=payload_size)
        msg.ok = OK(ok)
        return msg


@dataclass
class Message:
    mid: int
    priority: Priority
    data: bytes

    def __str__(self):
        return f"[{self.mid}] <{self.priority.name}> data len: {len(self.data)}"


@dataclass(repr=False)
class Stats:
    low: int
    medium: int
    high: int
    ins: float
    outs: float

    def __str__(self) -> str:
        return f"low={self.low}, medium={self.medium}, high={self.high}, ins={self.ins:.1f}, outs={self.outs:.1f}"

    def __repr__(self) -> str:
        return self.__str__()


class RMQError(Exception):
    pass


class RMQDataError(RMQError):
    pass


class RMQ:

    def __init__(self, profile='default') -> None:
        super().__init__()
        self.profile = profile
        self.__conf = config.get(f"rmq.{self.profile}", {})
        assert len(self.__conf) > 0, f'rmq profile not configured `rmq.{self.profile}`'
        self._conn: Optional[socket.socket] = None
        self._timeout = self.__conf.get('timeout', 30)
        self.__lock__ = threading.Lock()
        atexit.register(self.close)

    def __str__(self) -> str:
        return f"{self.__conf.get('host')}:{self.__conf.get('port', 7001)}"

    def __repr__(self) -> str:
        return self.__str__()

    def __enter__(self):
        self.ensure_connection()
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        if self._conn is None:
            return
        LOGGER.debug('[rmq] close')
        try:
            with self.__lock__:
                self._conn.close()
        except Exception as e:
            LOGGER.warning(e)
        finally:
            self._conn = None

    def ensure_connection(self, force=False):
        if self._conn is None or force:
            with self.__lock__:
                LOGGER.debug('[rmq] connecting')
                self._conn = self._connect()

    def _connect(self) -> socket.socket:
        host, port = self.__conf.get('host'), self.__conf.get('port', 7001)
        return socket.create_connection((host, port), self._timeout)

    def reconnect(self):
        LOGGER.info('[rmq] reconnect')
        self.close()
        while True:
            try:
                self.ensure_connection()
                return
            except OSError as e:
                LOGGER.error(e)
                time.sleep(5)

    def _send(self, data: bytes):
        self._conn.sendall(data)

    def _recv(self, n_bytes: int, timeout: Optional[int] = None):
        self._conn.setblocking(0)
        ready = select.select([self._conn], [], [], timeout or self._timeout)
        if ready[0]:
            return self._conn.recv(n_bytes)

        raise socket.timeout()

    def pull(self, queue: str, ttl: int = 0) -> Optional[Message]:
        def build_payload():
            return struct.pack(f">I{len(queue)}sI", len(queue), queue.encode('utf-8'), ttl)
        def decode_payload():
            if len(msg.payload) < 13:
                return None, None, None
            mid, priority, size_data = struct.unpack(">QcI", msg.payload[:13])
            data = msg.payload[13:13 + size_data]
            return mid, Priority(priority), data

        try:
            payload = build_payload()
            request = Msg(event=Event.GET, payload=payload).encode()
            self._send(request)
            response = self._recv(11, ttl)
            msg = Msg.from_bytes(response)
            if msg.payload_size > 0:
                msg.payload = self._recv(msg.payload_size)
            else:
                return None
            if msg.event != Event.GET:
                LOGGER.error(f"[get] got invalid response {msg}")
                return None
            try:
                mid, priority, data = decode_payload()
                return None if mid is None else Message(mid=mid, priority=priority, data=data)
            except Exception as e:
                LOGGER.exception(e)
                return None
        except (socket.timeout, BlockingIOError):
            return None
        except (RMQError, OSError) as e:
            LOGGER.error(e)
            self.reconnect()

    def publish(self,
                data: Union[str, dict, bytes],
                queue: str,
                priority: Priority = Priority.LOW) -> Optional[str]:
        def build_payload():
            fmt = f">I{len(queue)}scI"
            return struct.pack(fmt, len(queue), queue.encode('utf-8'), priority.value, len(data)) + data
        def decode_payload():
            try:
                return msg.payload.decode('utf-8') if msg.payload else None
            except UnicodeDecodeError as e:
                return str(e)

        if data is None:
            return
        if isinstance(data, str):
            data = data.encode('utf-8')
        elif isinstance(data, dict):
            data = jsons.dumps(data).encode('utf-8')
        elif isinstance(data, bytes):
            pass
        else:
            raise ValueError(f"Unsupported type of message: {type(data)}")

        try:
            payload = build_payload()
            request = Msg(event=Event.PUT, payload=payload).encode()
            self._send(request)
            response = self._recv(11)
            msg = Msg.from_bytes(response)
            if msg.payload_size > 0:
                msg.payload = self._recv(msg.payload_size)
            err = decode_payload()
            if msg.event != Event.PUT:
                LOGGER.error(f"[put] got invalid response {msg}")
            if err:
                raise RMQDataError(err)
        except (socket.timeout, BlockingIOError) as e:
            return RMQDataError(e)
        except (RMQError, OSError) as e:
            LOGGER.error(e)
            self.reconnect()

    def ack(self, queue: str, priority: Priority, id: int):
        def build_payload():
            return struct.pack(f">I{len(queue)}scQ", len(queue), queue.encode('utf-8'), priority.value, id)
        def decode_payload():
            try:
                return msg.payload.decode('utf-8') if msg.payload else None
            except UnicodeDecodeError as e:
                return str(e)

        try:
            payload = build_payload()
            request = Msg(event=Event.ACK, payload=payload).encode()
            self._send(request)
            response = self._recv(11)
            msg = Msg.from_bytes(response)
            if msg.payload_size > 0:
                msg.payload = self._recv(msg.payload_size)
            err = decode_payload()
            if msg.event != Event.ACK:
                LOGGER.error(f"[ack] got invalid response {msg}")
            if err:
                raise RMQDataError(err)
        except (socket.timeout, BlockingIOError) as e:
            return RMQDataError(e)
        except (RMQError, OSError) as e:
            LOGGER.error(e)
            self.reconnect()

    def stats(self, queue: str = "") -> Dict[str, Dict[str, Union[int, float]]]:
        def build_payload():
            return struct.pack(f">I{len(queue)}s", len(queue), queue.encode('utf-8'))
        def decode_val(data: bytes):
            low, medium, high, ins, outs = struct.unpack('>qqqdd', data)
            return Stats(low=low, medium=medium, high=high, ins=ins, outs=outs)
        def decode_payload():
            if len(msg.payload) < 4:
                return stats
            try:
                n, = struct.unpack(">I", msg.payload[:4])
                p = 4
                for _ in range(n):
                    key_len, val_len = struct.unpack(">II", msg.payload[p:p + 8])
                    key = msg.payload[p+8:p+8 + key_len].decode('utf-8')
                    val = msg.payload[p+8 + key_len:p+8 + key_len + val_len]
                    stats[key] = decode_val(val)
                    p += 8 + key_len + val_len
                return stats
            except UnicodeDecodeError as e:
                LOGGER.error(e)
                return stats

        stats = {}
        try:
            payload = build_payload()
            request = Msg(event=Event.STAT, payload=payload).encode()
            self._send(request)
            response = self._recv(11)
            msg = Msg.from_bytes(response)
            if msg.payload_size > 0:
                msg.payload = self._recv(msg.payload_size)
            if msg.event != Event.STAT:
                LOGGER.error(f"[stats] got invalid response {msg}")
                return stats
            return decode_payload()
        except (socket.timeout, BlockingIOError):
            return stats
        except (RMQError, OSError) as e:
            LOGGER.error(e)
            self.reconnect()
