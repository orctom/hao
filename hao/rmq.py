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
import time
import uuid
from dataclasses import dataclass
from typing import Dict, Optional, Union

from . import config, jsons, logs

LOGGER = logs.get_logger(__name__)


_MAGIC = b'<RMQ>'


def _int_2_bytes(i: int, size: int = 1):
    return i.to_bytes(size, 'big')


class Priority(enum.Enum):
    NORM = _int_2_bytes(0)
    PRIOR = _int_2_bytes(1)
    URGENT = _int_2_bytes(2)

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
    def __init__(self, event: Event, *, uid: str = None, payload_size: int = 0, payload: bytes = None, ok: OK = OK.TRUE):
        self.event = event
        self.ok = ok
        self.uid = uid or str(uuid.uuid4())
        self.payload_size = payload_size if payload is None else len(payload)
        self.payload = payload

    def __str__(self) -> str:
        return f"[{self.event.name}] <{self.ok.name}> {self.payload_size}"

    def __repr__(self) -> str:
        return self.__str__()

    def encode(self) -> bytes:
        assert self.payload is not None
        return struct.pack(
            '>5scc36sI',
            _MAGIC,
            self.event.value,
            self.ok.value,
            self.uid.encode('utf-8'),
            self.payload_size,
        ) + self.payload

    @classmethod
    def from_bytes(cls, data: bytes) -> 'Msg':
        if len(data) == 0:
            raise RMQError("Server lost")
        if len(data) != 47:
            raise RMQDataError(f"Invalid message size: {len(data)}")
        magic, event, ok, uid, payload_size = struct.unpack('>5scc36sI', data[:47])
        if magic != _MAGIC:
            raise RMQDataError(f"Invalid magic number {magic}")
        if not Event.has_value(event):
            raise RMQDataError("Invalid event value")
        if not OK.has_value(ok):
            raise RMQDataError("Invalid OK value")
        msg = cls(Event(event), uid=uid.decode('utf-8'), payload_size=payload_size)
        msg.ok = OK(ok)
        return msg


class Request:
    def __init__(self):
        self._event = threading.Event()
        self._data = None

    def set_data(self, data):
        self._data = data
        self._event.set()

    def get_data(self, timeout=0.1):
        self._event.wait(timeout)
        return self._data


@dataclass
class Message:
    mid: int
    priority: Priority
    data: bytes

    def __str__(self):
        return f"[{self.mid}] <{self.priority.name}> data len: {len(self.data)}"


@dataclass(repr=False)
class Stats:
    norm: int
    prior: int
    urgent: int
    ins: float
    outs: float

    def __str__(self) -> str:
        return f"norm={self.norm: >8}, prior={self.prior: >8}, urgent={self.urgent: >8}, ins={f'{self.ins:.1f}': >6}, outs={f'{self.outs:.1f}': >6}"

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
        self._requests: Dict[str, Request] = {}
        self.connected = False
        self.__lock__ = threading.Lock()
        self._receive_thread = threading.Thread(target=self._receive_loop, daemon=True)
        atexit.register(self.close)

    def __str__(self) -> str:
        return f"{self.__conf.get('host')}:{self.__conf.get('port', 7001)}"

    def __repr__(self) -> str:
        return self.__str__()

    def __enter__(self):
        LOGGER.info('[rmq] enter')
        self.ensure_connection()
        return self

    def __exit__(self, *args):
        self.close()
        LOGGER.info('[rmq] exit')

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
            self.connected = False

    def ensure_connection(self, force=False):
        if self._conn is None or force or not self.connected:
            with self.__lock__:
                if self.connected is True:
                    self.close()
                LOGGER.debug('[rmq] connecting')
                self._conn = self._connect()
                self.connected = True
                LOGGER.debug('[rmq] connected')

        if not self._receive_thread.is_alive():
            self._receive_thread.start()

    def _connect(self) -> socket.socket:
        host, port = self.__conf.get('host'), self.__conf.get('port', 7001)
        return socket.create_connection((host, port), self._timeout)

    def reconnect(self):
        LOGGER.info('[rmq] reconnect')
        while True:
            try:
                self.ensure_connection()
                return
            except OSError as e:
                self.connected = False
                LOGGER.error(e)
                time.sleep(5)

    def request(self, event: Event, payload: bytes, timeout = 1) -> bytes:
        self.ensure_connection()
        msg = Msg(event=event, payload=payload)
        request = self._requests[msg.uid] = Request()
        self._conn.sendall(msg.encode())
        return request.get_data(timeout)

    def _receive_loop(self):
        while True:
            try:
                if self._conn is None or not self.connected:
                    time.sleep(1)
                    continue
                response = self._conn.recv(47)
                if not response:
                    continue
                msg = Msg.from_bytes(response)
                try:
                    if msg.payload_size > 0 and self._conn is not None:
                        msg.payload = self._conn.recv(msg.payload_size)
                except (socket.timeout, TimeoutError, BlockingIOError):
                    pass
                except Exception as e:
                    LOGGER.exception(e)
                finally:
                    request = self._requests.get(msg.uid)
                    if request is not None:
                        request.set_data(msg)
                        del self._requests[msg.uid]
            except (socket.timeout, TimeoutError, BlockingIOError):
                pass
            except (RMQError, OSError) as e:
                if self._conn is None:
                    continue
                LOGGER.error(f"failed to receive message from RMQ: {e}")
                self.reconnect()
            except Exception as e:
                LOGGER.exception(e)

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
            msg = self.request(Event.GET, build_payload(), 1 + ttl)
            if msg is None:
                return None
            if msg.payload_size == 0:
                return None
            try:
                mid, priority, data = decode_payload()
                return None if mid is None else Message(mid=mid, priority=priority, data=data)
            except Exception as e:
                LOGGER.exception(e)
                return None
        except (socket.timeout, TimeoutError, BlockingIOError):
            return None
        except (RMQError, OSError) as e:
            LOGGER.error(e)
            if self._conn is not None:
                self.reconnect()

    def publish(self,
                data: Union[str, dict, bytes],
                queue: str,
                priority: Priority = Priority.NORM) -> Optional[str]:
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
            msg = self.request(Event.PUT, build_payload())
            if msg is None:
                raise RMQDataError("timed out")
            err = decode_payload()
            if err:
                raise RMQDataError(err)
        except (socket.timeout, TimeoutError, BlockingIOError) as e:
            raise RMQDataError(e)
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
            msg = self.request(Event.ACK, build_payload())
            if msg is None:
                return RMQDataError("timed out")
            err = decode_payload()
            if err:
                raise RMQDataError(err)
        except (socket.timeout, TimeoutError, BlockingIOError) as e:
            return RMQDataError(e)
        except (RMQError, OSError) as e:
            LOGGER.error(e)
            self.reconnect()

    def stats(self, queue: str = "") -> dict:
        def build_payload():
            return struct.pack(f">I{len(queue)}s", len(queue), queue.encode('utf-8'))
        def decode_val(data: bytes):
            norm, prior, urgent, ins, outs = struct.unpack('>qqqdd', data)
            return Stats(norm=norm, prior=prior, urgent=urgent, ins=ins, outs=outs)
        def decode_payload():
            if len(msg.payload) < 4:
                return stats
            try:
                mem, n = struct.unpack(">QI", msg.payload[:12])
                stats['$mem'] = mem
                p = 12
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
            msg = self.request(Event.STAT, build_payload())
            if msg is None:
                return stats
            return decode_payload()
        except (socket.timeout, TimeoutError, BlockingIOError):
            return stats
        except (RMQError, OSError) as e:
            LOGGER.error(e)
            self.reconnect()
