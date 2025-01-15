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
import socket
import struct
import threading
from dataclasses import dataclass
from typing import Dict, Optional, Union

from . import config, jsons, logs

LOGGER = logs.get_logger(__name__)


_MAGIC = b'<RMQ>'


def _int_2_bytes(i: int, size: int = 1):
    return i.to_bytes(size, 'big')


class Priority(enum.Enum):
    NORM = _int_2_bytes(0)
    HIGH = _int_2_bytes(1)
    URGENT = _int_2_bytes(2)

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_


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
        return f"[{self.event.name}] <{self.ok}> {self.payload_size}"

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
        if len(data) != 11:
            raise RMQDataError("Invalid message")
        magic, event, ok, payload_size = struct.unpack('>5sccI', data[:11])
        if magic != _MAGIC:
            raise RMQDataError(f"Invalid magic number {magic}")
        if not Event.has_value(event):
            raise RMQDataError("Invalid event value")
        if not OK.has_value(ok):
            raise RMQDataError("Invalid OK value")
        return cls(event, payload_size=payload_size)


@dataclass
class Message:
    mid: int
    priority: Priority
    data: bytes


@dataclass(repr=False)
class Stats:
    urgent: int
    high: int
    norm: int
    ins: float
    outs: float

    def __str__(self) -> str:
        return f"urgent={self.urgent}, high={self.high}, norm={self.norm}, ins={self.ins:.1f}, outs={self.outs:.1f}"

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
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        host, port = self.__conf.get('host'), self.__conf.get('port', 7001)
        conn.connect((host, port))
        return conn

    def reconnect(self):
        LOGGER.info('[rmq] reconnect')
        self.close()
        self.ensure_connection()

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
            self._conn.sendall(request)
            response = self._conn.recv(11)
            msg = Msg.from_bytes(response)
            if msg.payload_size == 0:
                return None
            msg.payload = self._conn.recv(msg.payload_size)
            mid, priority, data = decode_payload()
            if mid is None:
                return None
            return Message(mid=mid, priority=priority, data=data)
        except OSError as e:
            LOGGER.error(e)
            self.reconnect()
        except socket.timeout as e:
            LOGGER.error(e)
            raise RMQError(e)

    def publish(self,
                data: Union[str, dict, bytes],
                queue: str,
                priority: Priority = Priority.NORM) -> Optional[str]:
        def build_payload():
            fmt = f">I{len(queue)}scI"
            return struct.pack(fmt, len(queue), queue.encode('utf-8'), priority.value, len(data)) + data
        def decode_payload():
            return msg.payload.decode('utf-8') if msg.payload else None

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
            self._conn.sendall(request)
            response = self._conn.recv(11)
            msg = Msg.from_bytes(response)
            if msg.payload_size > 0:
                msg.payload = self._conn.recv(msg.payload_size)
            err = decode_payload()
            if err:
                raise RMQError(err)
        except OSError as e:
            LOGGER.error(e)
            self.reconnect()
        except socket.timeout as e:
            LOGGER.error(e)
            raise RMQError(e)

    def ack(self, queue: str, priority: Priority, id: int):
        def build_payload():
            return struct.pack(f">I{len(queue)}scQ", len(queue), queue.encode('utf-8'), priority.value, id)
        def decode_payload():
            return msg.payload.decode('utf-8') if msg.payload else None

        try:
            payload = build_payload()
            request = Msg(event=Event.ACK, payload=payload).encode()
            self._conn.sendall(request)
            response = self._conn.recv(11)
            msg = Msg.from_bytes(response)
            if msg.payload_size > 0:
                msg.payload = self._conn.recv(msg.payload_size)
            err = decode_payload()
            if err:
                raise RMQError(err)
        except OSError as e:
            LOGGER.error(e)
            self.reconnect()
        except socket.timeout as e:
            LOGGER.error(e)
            raise RMQError(e)

    def stats(self, queue: str = "") -> Dict[str, Dict[str, Union[int, float]]]:
        def build_payload():
            return struct.pack(f">I{len(queue)}s", len(queue), queue.encode('utf-8'))
        def decode_val(data: bytes):
            urgent, high, norm, ins, outs = struct.unpack('>qqqdd', data)
            return Stats(urgent=urgent, high=high, norm=norm, ins=ins, outs=outs)
        def decode_payload():
            n, = struct.unpack(">I", msg.payload[:4])
            stats = {}
            p = 4
            for _ in range(n):
                key_len, val_len = struct.unpack(">II", msg.payload[p:p + 8])
                key = msg.payload[p+8:p+8 + key_len].decode('utf-8')
                val = msg.payload[p+8 + key_len:p+8 + key_len + val_len]
                stats[key] = decode_val(val)
                p += 8 + key_len + val_len
            return stats

        try:
            payload = build_payload()
            request = Msg(event=Event.STAT, payload=payload).encode()
            self._conn.sendall(request)
            response = self._conn.recv(11)
            msg = Msg.from_bytes(response)
            msg.payload = self._conn.recv(msg.payload_size)
            return decode_payload()
        except OSError as e:
            LOGGER.error(e)
            self.reconnect()
        except socket.timeout as e:
            LOGGER.error(e)
            raise RMQError(e)
