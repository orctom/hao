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
import typing

from amqp import UnexpectedFrame
from kombu import Connection
from kombu.simple import SimpleQueue
from kombu.transport.pyamqp import Message

from . import logs, config, jsons

LOGGER = logs.get_logger(__name__)

RETRY_POLICY = {'interval_start': 0, 'interval_step': 1, 'max_retries': 3}


class Rabbit(object):

    def __init__(self, profile='default') -> None:
        super().__init__()
        self.profile = profile
        self._conn: typing.Optional[Connection] = None
        self._queues = {}
        self._queue_options = {}
        self.__lock__ = threading.Lock()
        atexit.register(self.close)

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
        conf = config.get(f"rabbit.{self.profile}")
        if conf is None:
            raise ValueError(f'no config found for mongodb, expecting: `rabbit.{self.profile}`')

        self._queue_options = conf.get('queues')
        n_queue_options = len(self._queue_options)
        if n_queue_options == 0:
            raise ValueError(f'no queues configured, expecting: `rabbit.{self.profile}.queues`')

        return Connection(
            conf.get('host', 'localhost'),
            conf.get('user', 'rabbit'),
            conf.get('password', 'rabbit'),
            conf.get('vhost', '/'),
            conf.get('port', 5672),
            connect_timeout=conf.get('timeout', 10),
            heartbeat=conf.get('heartbeat', 0),
            login_method=conf.get('login_method', 'PLAIN')
        )

    def reconnect(self):
        LOGGER.info('[rabbit] reconnect')
        self.close()
        self.ensure_connection()

    def get_queue(self, queue_id: str = None) -> (typing.Optional[SimpleQueue], str):
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
        channel = self._get_channel(options.get('prefetch', 1))
        queue_name = options.get('name', queue_id)
        LOGGER.debug(f'[rabbit] queue id: {queue_id} -> queue name: {queue_name}')
        return self._conn.SimpleQueue(
            queue_name,
            queue_opts=options.get('opts'),
            queue_args=options.get('args'),
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
                message: typing.Union[str, dict, list],
                queue_id: str = None,
                prior: bool = False,
                retry: bool = True,
                verbose: bool = True):
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
                    queue.put(msg, retry=retry, retry_policy=RETRY_POLICY, priority=priority)
                if verbose:
                    LOGGER.info(f'[rabbit] [{queue_id}] added: {len(message)} tasks, prior: {prior}, priority: {priority}')
            else:
                msg = message if isinstance(message, str) else jsons.dumps(message)
                queue.put(msg, retry=retry, retry_policy=RETRY_POLICY, priority=priority)

                if verbose:
                    LOGGER.info(f'[rabbit] [{queue_id}] added: {msg}, prior: {prior}, priority: {priority}')
        except AttributeError as e:
            self.ensure_connection(True)
            raise e

    def pull(self, queue_id: str = None, timeout=5, block=True) -> typing.Optional[Message]:
        queue, queue_id = self.get_queue(queue_id)
        if queue is None:
            return None
        try:
            return queue.get(block=block, timeout=timeout)
        except (queue.Empty, UnexpectedFrame, AttributeError):
            pass
        except (BrokenPipeError, ConnectionResetError, OSError):
            self.reconnect()
        except Exception as e:
            LOGGER.exception(e)

    def consume(self, queue_id: str = None, timeout=1, block=True, prefetch=1) -> typing.Generator[Message, None, None]:
        queue, queue_id = self.get_queue(queue_id, prefetch)
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
        except Exception as e:
            LOGGER.exception(e)

    @staticmethod
    def queue_size(queue_id: str = None):
        with Rabbit() as rabbit:
            queue, queue_id = rabbit.get_queue(queue_id)
            queue_size = queue.qsize() if queue else -1
        return str(queue_size)
