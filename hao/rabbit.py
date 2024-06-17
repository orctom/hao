# -*- coding: utf-8 -*-
"""
####################################################
###########         dependency          ############
####################################################
pip install kombu>=4.6.0

####################################################
###########         config.yml          ############
####################################################
rabbit:
  default:
    host: 39.107.231.229
    user: username
    password: password
    vhost: /
    queues:
      bidding-tasks:
        name: bidding-tasks-hao
        default: true
        opts:
          max_priority: 2
        args:
          x-queue-mode: lazy
      htmltk:
        name: htmltk
        args:
          x-queue-mode: lazy
      dummy:
        name: dummy-hao
        args:
          x-queue-mode: lazy
  some-other:
    host: some-other-host
    user: username
    password: password
    vhost: dummy-vhost
    queues:
      queue-id:
        name: queue-name


####################################################
###########          usage              ############
####################################################
from hao.rabbit import Rabbit
rabbit = Rabbit()
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
import threading
from typing import Generator, Optional, Tuple, Union

from amqp import UnexpectedFrame
from kombu import Connection
from kombu.simple import SimpleQueue
from kombu.transport.pyamqp import Message

from . import config, jsons, logs

LOGGER = logs.get_logger(__name__)

RETRY_POLICY = {'interval_start': 0, 'interval_step': 1, 'max_retries': 3}


class Rabbit(object):

    def __init__(self, profile='default', prefetch=1) -> None:
        super().__init__()
        self.profile = profile
        self.__conf = config.get(f"rabbit.{self.profile}", {})
        assert len(self.__conf) > 0, f'rabbit profile not configured `rabbit.{self.profile}`'
        self.prefetch = prefetch
        self._conn: Optional[Connection] = None
        self._queues = {}
        self._queue_options = {}
        self.__lock__ = threading.Lock()
        atexit.register(self.close)

    def __str__(self) -> str:
        return f"{self.__conf.get('user')}:***@{self.__conf.get('host')}:{self.__conf.get('port', 5672)}/{self.__conf.get('vhost', '')}"

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
        LOGGER.debug('[rabbit] close')
        try:
            with self.__lock__:
                self._conn.release()
        except Exception as e:
            LOGGER.warning(e)
        finally:
            self._conn = None

    def ensure_connection(self, force=False):
        if self._conn is None or force:
            with self.__lock__:
                LOGGER.debug('[rabbit] connecting')
                self._conn = self._connect()
                self._queues.clear()

    def _connect(self):
        self._queue_options = self.__conf.get('queues')
        n_queue_options = len(self._queue_options)
        if n_queue_options == 0:
            raise ValueError(f'no queues configured, expecting: `rabbit.{self.profile}.queues`')

        return Connection(
            self.__conf.get('host', 'localhost'),
            self.__conf.get('user', 'rabbit'),
            self.__conf.get('password', 'rabbit'),
            self.__conf.get('vhost', '/'),
            self.__conf.get('port', 5672),
            connect_timeout=self.__conf.get('timeout', 10),
            heartbeat=self.__conf.get('heartbeat', 0),
            login_method=self.__conf.get('login_method', 'PLAIN')
        )

    def reconnect(self):
        LOGGER.info('[rabbit] reconnect')
        self.close()
        self.ensure_connection()

    def get_queue(self, queue_id: str = None) -> Tuple[Optional[SimpleQueue], str]:
        self.ensure_connection()
        if queue_id is None:
            queue_id = list(self._queue_options)[0]
        elif queue_id not in self._queue_options:
            raise ValueError(f'[rabbit] invalid queue_id: {queue_id}')
        with self.__lock__:
            queue = self._queues.get(queue_id)
            if queue is not None:
                return queue, queue_id
            queue = self._simple_queue(queue_id)
            self._queues[queue_id] = queue
            return queue, queue_id

    def _simple_queue(self, queue_id) -> SimpleQueue:
        options = self._queue_options.get(queue_id, {})
        channel = self._get_channel(self.prefetch)
        queue_name = options.get('name', queue_id)
        LOGGER.debug(f'[rabbit] queue id: {queue_id} -> queue name: {queue_name}')
        return self._conn.SimpleQueue(
            queue_name,
            queue_opts=options.get('opts'),
            queue_args=options.get('args'),
            exchange_opts=options.get('exchange_opts'),
            channel=channel
        )

    def _get_channel(self, prefetch):
        channel = self._conn.channel()
        channel.basic_qos(prefetch_size=0, prefetch_count=prefetch, a_global=False)
        return channel

    def _is_support_priority(self, queue_id):
        options = self._queue_options.get(queue_id)
        if options is None or len(options) == 0:
            return False
        max_priority = options.get('opts', {}).get('max_priority')
        if max_priority is None or max_priority == 0:
            return False

        return True

    def publish(self,
                message: Union[str, dict, list],
                queue_id: str = None,
                prior: bool = False,
                retry: bool = True,
                verbose: bool = True,
                **kwargs):
        if message is None:
            if verbose:
                LOGGER.warning(f"[rabbit] empty message: {message}")
            return

        queue, queue_id = self.get_queue(queue_id)
        if queue is None:
            if verbose:
                LOGGER.warning(f"[rabbit] queue_id not in: rabbit.{self.profile}.queues")
            return
        is_support_priority = self._is_support_priority(queue_id)
        priority = (1 if prior else 0) if is_support_priority else None
        try:
            if isinstance(message, list):
                for msg in message:
                    msg = msg if isinstance(msg, str) else jsons.dumps(msg)
                    queue.put(msg, retry=retry, retry_policy=RETRY_POLICY, priority=priority, **kwargs)
                if verbose:
                    LOGGER.info(f'[rabbit] [{queue_id}] added: {len(message)} tasks, prior: {prior}, priority: {priority}')
            else:
                msg = message if isinstance(message, str) else jsons.dumps(message)
                queue.put(msg, retry=retry, retry_policy=RETRY_POLICY, priority=priority, **kwargs)

                if verbose:
                    LOGGER.info(f'[rabbit] [{queue_id}] added: {msg}, prior: {prior}, priority: {priority}')
        except AttributeError as e:
            self.ensure_connection(True)
            raise e
        except (BrokenPipeError, ConnectionResetError, OSError):
            self.reconnect()

    def pull(self, queue_id: str = None, timeout=5, block=True) -> Optional[Message]:
        queue, queue_id = self.get_queue(queue_id)
        if queue is None:
            return None
        try:
            return queue.get(block=block, timeout=timeout)
        except (queue.Empty, UnexpectedFrame, AttributeError):
            pass
        except (BrokenPipeError, ConnectionResetError, OSError):
            self.reconnect()

    def consume(self, queue_id: str = None, timeout=1, block=True) -> Generator[Message, None, None]:
        queue, queue_id = self.get_queue(queue_id)
        if queue is None:
            return None
        try:
            msg = queue.get(block=block, timeout=timeout)
            while msg is not None:
                yield msg
                msg = queue.get(block=block, timeout=timeout)
        except (queue.Empty, UnexpectedFrame, AttributeError):
            pass
        except (BrokenPipeError, ConnectionResetError, OSError):
            self.reconnect()

    def queue_size(self, queue_id: str = None) -> int:
        queue, queue_id = self.get_queue(queue_id)
        return queue.qsize() if queue else -1
