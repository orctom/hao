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
_EMPTY_PAYLOAD = bytes()


class RMQError(Exception):
    pass


class RMQDataError(RMQError):
    pass



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
    HEARTBEAT = _int_2_bytes(200)

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
        msg = cls(Event(event), uid=uid.decode('utf-8'), payload_size=payload_size)
        msg.ok = OK(ok)
        return msg


class Request:
    def __init__(self):
        self._event = threading.Event()
        self._data: Optional[Msg] = None
        self._error: Optional[Exception] = None

    def set_response(self, *, data: Optional[Msg] = None, error: Optional[Exception] = None):
        self._data = data
        self._error = error
        self._event.set()

    def get(self, timeout=0.1) -> (Optional[Msg], Optional[Exception]):
        self._event.wait(timeout)
        return self._data, self._error


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


class RMQ:

    def __init__(self, profile='default') -> None:
        super().__init__()
        self.profile = profile
        self.__conf = config.get(f"rmq.{self.profile}", {})
        assert len(self.__conf) > 0, f'rmq profile not configured `rmq.{self.profile}`'
        self._conn: Optional[socket.socket] = None
        self._timeout = self.__conf.get('timeout', 30)
        self._requests: Dict[str, Request] = {}
        self._stopped = threading.Event()
        self.__lock__ = threading.Lock()
        self._receive_thread = threading.Thread(target=self._receive_loop, daemon=True)
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        atexit.register(self._close)

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

    def _connect(self):
        host, port = self.__conf.get('host'), self.__conf.get('port', 7001)
        with self.__lock__:
            LOGGER.debug('[rmq] connecting')
            self._conn = socket.create_connection((host, port), self._timeout)
            LOGGER.debug('[rmq] connected')

    def reconnect(self):
        LOGGER.info('[rmq] reconnect')
        while not self._stopped.is_set():
            try:
                self.ensure_connection()
                LOGGER.info('[rmq] reconnected')
                return
            except OSError as e:
                self._conn = None
                LOGGER.exception(e)
                time.sleep(5)

    def _close(self):
        if self._conn is None:
            return
        LOGGER.debug('[rmq] close')
        try:
            with self.__lock__:
                self._conn.close()
            LOGGER.debug('[rmq] closed')
        except Exception as e:
            LOGGER.exception(e)
        finally:
            self._conn = None

    def close(self):
        self._stopped.set()
        self._close()

    def ensure_connection(self):
        if self._conn is not None:
            return
        self._close()
        self._connect()

        if not self._receive_thread.is_alive():
            self._receive_thread.start()
        if not self._heartbeat_thread.is_alive():
            self._heartbeat_thread.start()

    def request(self, event: Event, payload: bytes, timeout = 5) -> (Optional[Msg], Optional[Exception]):
        self.ensure_connection()
        msg = Msg(event=event, payload=payload)
        request = self._requests[msg.uid] = Request()
        tries = 3
        err = None
        while not self._stopped.is_set() and tries > 0:
            try:
                if tries != 3:
                    LOGGER.debug(f"retrying: {tries}")
                self._conn.sendall(msg.encode())
                res = request.get(timeout)
                self._requests.pop(msg.uid, None)
                return res
            except (socket.timeout, TimeoutError, BlockingIOError) as e:
                tries -= 1
                LOGGER.error(e)
                err = e
            except OSError as e:
                tries -= 1
                LOGGER.error(e)
                self.reconnect()
                err = e
        return None, err

    def _receive_loop(self):
        def set_response(uid, data=None, error=None):
            request = self._requests.pop(uid, None)
            if request is not None:
                request.set_response(data=data, error=error)

        while not self._stopped.is_set():
            try:
                if self._conn is None:
                    time.sleep(1)
                    continue
                response = self._conn.recv(47)
                if not response:
                    continue
                msg = Msg.from_bytes(response)
                try:
                    if msg.payload_size > 0 and self._conn is not None:
                        msg.payload = self._conn.recv(msg.payload_size)
                    set_response(msg.uid, data=msg)
                except (socket.timeout, TimeoutError, BlockingIOError) as e:
                    set_response(msg.uid, data=msg, error=RMQDataError(e))
                except Exception as e:
                    set_response(msg.uid, data=msg, error=RMQDataError(e))
                    LOGGER.exception(e)
            except (socket.timeout, TimeoutError, BlockingIOError):
                pass
            except RMQDataError as e:
                LOGGER.error(f"[rmq] invalid response: {e}")
                set_response(msg.uid, error=e)
            except (RMQError, OSError) as e:
                LOGGER.error(f"failed to receive message from RMQ: {e}")
                self.reconnect()
            except Exception as e:
                LOGGER.exception(e)

    def _heartbeat_loop(self):
        interval = 30
        while not self._stopped.is_set():
            try:
                self._stopped.wait(interval)
                if self._conn is None:
                    continue
                LOGGER.debug('[rmq] heartbeat')
                self.request(Event.HEARTBEAT, _EMPTY_PAYLOAD)
            except Exception as e:
                LOGGER.error(e)

    def pull(self, queue: str, ttl: int = 0) -> Optional[Message]:
        def build_payload():
            return struct.pack(f">I{len(queue)}sI", len(queue), queue.encode('utf-8'), ttl)
        def decode_payload():
            if len(msg.payload) < 13:
                return None, None, None
            mid, priority, size_data = struct.unpack(">QcI", msg.payload[:13])
            data = msg.payload[13:13 + size_data]
            return mid, Priority(priority), data

        msg, err = self.request(Event.GET, build_payload(), 1 + ttl)
        if msg is None or err is not None:  # only cares about if get the msg successful
            return None
        if msg.payload_size == 0:
            return None
        try:
            mid, priority, data = decode_payload()
            return None if mid is None else Message(mid=mid, priority=priority, data=data)
        except Exception as e:
            LOGGER.exception(e)
            return None

    def publish(self,
                data: Union[str, dict, bytes],
                queue: str,
                priority: Priority = Priority.NORM):
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

        msg, err = self.request(Event.PUT, build_payload())
        if err is not None:
            raise err
        if msg is None:
            raise RMQDataError("publish timed out")
        err = decode_payload()
        if err:
            raise RMQDataError(err)

    def ack(self, queue: str, priority: Priority, id: int):
        def build_payload():
            return struct.pack(f">I{len(queue)}scQ", len(queue), queue.encode('utf-8'), priority.value, id)
        def decode_payload():
            try:
                return msg.payload.decode('utf-8') if msg.payload else None
            except UnicodeDecodeError as e:
                return str(e)

        msg, err = self.request(Event.ACK, build_payload())
        if err is not None:
            raise err
        if msg is None:
            raise RMQDataError("ack timed out")
        err = decode_payload()
        if err:
            raise RMQDataError(err)

    def stats(self, queue: str = "") -> dict:
        def build_payload():
            return struct.pack(f">I{len(queue)}s", len(queue), queue.encode('utf-8'))
        def decode_val(data: bytes):
            norm, prior, urgent, ins, outs = struct.unpack('>qqqdd', data)
            return Stats(norm=norm, prior=prior, urgent=urgent, ins=ins, outs=outs)
        def decode_payload():
            stats = {}
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

        msg, err = self.request(Event.STAT, build_payload())
        if msg is None or err is not None:
            return {}
        return decode_payload()
