# -*- coding: utf-8 -*-
"""
####################################################
###########         dependency          ############
####################################################
pip install aio-pika>=8.0

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

queue size
print(await rabbit.queue_size(queue_name))

publish, accepts string, dict, list
for i in range(0, 10):
    await rabbit.publish(queue_name, f'hello-{i}')
print(await rabbit.queue_size(queue_name))

consume
async for msg in rabbit.consume(queue_name, timeout=1):
    print(msg)
"""
import asyncio

import aio_pika
from aio_pika import Message, Queue
from aio_pika.abc import AbstractChannel, AbstractConnection, AbstractQueue

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
        self._conn: AbstractConnection | None = None
        self._channel: AbstractChannel | None = None
        self._queues: dict[str, AbstractQueue] = {}
        self._queue_options = {}
        self.__lock__ = asyncio.Lock()

    def __str__(self) -> str:
        return f"{self.__conf.get('user')}:***@{self.__conf.get('host')}:{self.__conf.get('port', 5672)}/{self.__conf.get('vhost', '')}"

    def __repr__(self) -> str:
        return self.__str__()

    async def __aenter__(self):
        await self.ensure_connection()
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def close(self):
        if self._conn is None:
            return
        LOGGER.debug('[rabbit] close')
        try:
            async with self.__lock__:
                if self._channel:
                    await self._channel.close()
                await self._conn.close()
        except Exception as e:
            LOGGER.warning(e)
        finally:
            self._conn = None
            self._channel = None

    async def ensure_connection(self, force=False):
        if self._conn is None or force:
            async with self.__lock__:
                if self._conn is not None and not force:
                    return
                LOGGER.debug('[rabbit] connecting')
                await self._connect()
                self._queues.clear()

    async def _connect(self):
        self._queue_options = self.__conf.get('queues')
        n_queue_options = len(self._queue_options)
        if n_queue_options == 0:
            raise ValueError(f'no queues configured, expecting: `rabbit.{self.profile}.queues`')

        self._conn = await aio_pika.connect_robust(
            host=self.__conf.get('host', 'localhost'),
            port=self.__conf.get('port', 5672),
            login=self.__conf.get('user', 'rabbit'),
            password=self.__conf.get('password', 'rabbit'),
            virtualhost=self.__conf.get('vhost', '/'),
            timeout=self.__conf.get('timeout', 10),
            heartbeat=self.__conf.get('heartbeat', 0),
        )
        self._channel = await self._conn.channel()
        await self._channel.set_qos(prefetch_count=self.prefetch)

    async def reconnect(self):
        LOGGER.info('[rabbit] reconnect')
        await self.close()
        await self.ensure_connection()

    async def get_queue(self, queue_id: str = None) -> tuple[AbstractQueue | None, str]:
        await self.ensure_connection()
        if queue_id is None:
            queue_id = list(self._queue_options)[0]
        elif queue_id not in self._queue_options:
            raise ValueError(f'[rabbit] invalid queue_id: {queue_id}')
        async with self.__lock__:
            queue = self._queues.get(queue_id)
            if queue is not None:
                return queue, queue_id
            queue = await self._simple_queue(queue_id)
            self._queues[queue_id] = queue
            return queue, queue_id

    async def _simple_queue(self, queue_id) -> AbstractQueue:
        options = self._queue_options.get(queue_id, {})
        queue_name = options.get('name', queue_id)
        LOGGER.debug(f'[rabbit] queue id: {queue_id} -> queue name: {queue_name}')
        queue_opts = options.get('opts', {})
        queue_args = options.get('args', {})
        return await self._channel.declare_queue(
            queue_name,
            durable=True,
            **queue_opts,
            arguments=queue_args,
        )

    def _is_support_priority(self, queue_id):
        options = self._queue_options.get(queue_id)
        if options is None or len(options) == 0:
            return False
        max_priority = options.get('opts', {}).get('max_priority')
        if max_priority is None or max_priority == 0:
            return False
        return True

    async def publish(self,
                      message: str | dict | list,
                      queue_id: str = None,
                      prior: bool = False,
                      verbose: bool = True,
                      **kwargs):
        if message is None:
            if verbose:
                LOGGER.warning(f"[rabbit] empty message: {message}")
            return

        queue, queue_id = await self.get_queue(queue_id)
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
                    await queue.publish(Message(msg.encode(), priority=priority), **kwargs)
                if verbose:
                    LOGGER.info(f'[rabbit] [{queue_id}] added: {len(message)} tasks, prior: {prior}, priority: {priority}')
            else:
                msg = message if isinstance(message, str) else jsons.dumps(message)
                await queue.publish(Message(msg.encode(), priority=priority), **kwargs)
                if verbose:
                    LOGGER.info(f'[rabbit] [{queue_id}] added: {msg}, prior: {prior}, priority: {priority}')
        except Exception as e:
            LOGGER.exception(e)
            await self.reconnect()
            raise e

    async def pull(self, queue_id: str = None, timeout=5) -> Message | None:
        queue, queue_id = await self.get_queue(queue_id)
        if queue is None:
            return None
        try:
            return await queue.get(timeout=timeout)
        except asyncio.TimeoutError:
            pass
        except Exception as e:
            LOGGER.exception(e)
            await self.reconnect()

    async def consume(self, queue_id: str = None, timeout=1):
        queue, queue_id = await self.get_queue(queue_id)
        if queue is None:
            return
        try:
            msg = await queue.get(timeout=timeout)
            while msg is not None:
                yield msg
                msg = await queue.get(timeout=timeout)
        except asyncio.TimeoutError:
            pass
        except Exception as e:
            LOGGER.exception(e)
            await self.reconnect()

    async def queue_size(self, queue_id: str = None) -> int:
        queue, queue_id = await self.get_queue(queue_id)
        return queue.declaration_result.message_count if queue else -1
