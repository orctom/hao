# -*- coding: utf-8 -*-
import asyncio
import logging
import traceback
from collections import defaultdict
from datetime import datetime

import aiohttp

from . import config, jsons, paths, singleton, versions

LOGGER = logging.getLogger(__name__)


_URL_TOKEN = 'https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal'
_URL_NOTIFY = 'https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id'
_HEADERS_TOKEN = {'Content-Type': 'application/json'}


class Feishu(metaclass=singleton.Singleton):
    def __init__(self):
        cfg = config.get('feishu', {})
        self._secrets = cfg.get('secrets')
        self._ids = cfg.get('ids')
        self._identifier = f'[{config.HOSTNAME}-{paths.project_name()}-{paths.program_name()}]'
        self._aaccess_token = None
        self._messages = defaultdict(list)
        self._send_timer = None
        self._last = None
        self._session: aiohttp.ClientSession | None = None
        self._token_task: asyncio.Task | None = None
        if cfg:
            asyncio.create_task(self._refresh_access_token())

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _refresh_access_token(self):
        payload = {'app_id': self._secrets.get('app_id'), 'app_secret': self._secrets.get('app_secret')}
        session = await self._get_session()
        async with session.post(_URL_TOKEN, headers=_HEADERS_TOKEN, json=payload) as response:
            response.raise_for_status()
            data = await response.json()
            expire, self._aaccess_token = data.get('expire'), data.get('tenant_access_token')
            LOGGER.info(f"[feishu] token refreshed: {self._aaccess_token}, expire: {expire}")

        await asyncio.sleep(expire - 10)
        await self._refresh_access_token()

    async def _send_messages(self):
        if len(self._messages) == 0:
            return
        messages, self._messages = self._messages, defaultdict(list)
        for topic, msgs in messages.items():
            await self._send(msgs, topic)

    async def _send(self, messages: list[str], topic: str):
        receive_id = self._ids.get(topic)
        if receive_id is None:
            LOGGER.info(f"[feishu] topic not found: {topic}")
            return

        try:
            headers = {'Content-Type': 'application/json', 'Authorization': f"Bearer {self._aaccess_token}"}
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            if len(messages) > 20:
                messages = messages[:10] + [f"... {len(messages) - 20} items ..."] + messages[-10:]

            blocks = [{'tag': 'code_block', 'text': message} for message in messages]
            content = {
                'zh_cn': {
                    'title': f"{self._identifier} {timestamp}\tversion: {versions.get_version() or 'dev'}",
                    'content': [blocks]
                }
            }

            data = {
                'content': jsons.dumps(content),
                'msg_type': 'post',
                'receive_id': receive_id,
            }
            session = await self._get_session()
            async with session.post(_URL_NOTIFY, json=data, headers=headers) as response:
                response.raise_for_status()
        except Exception as e:
            LOGGER.exception(e)

    async def notify(self, message: str, topic='default'):
        if self._ids is None:
            return

        now = datetime.now().timestamp()
        try:
            self._messages[topic].append(message)
            if self._last is None or now - self._last >= 5:
                await self._send_messages()
                return

            if self._send_timer is not None and not self._send_timer.done():
                return

            self._send_timer = asyncio.create_task(asyncio.sleep(5, loop=None))
            await self._send_timer
            await self._send_messages()
        finally:
            self._last = now

    async def notify_exception(self, e: Exception, data: str | dict | None = None, topic='default'):
        if self._ids is None:
            return
        if isinstance(data, dict):
            text = jsons.dumps(data)
        else:
            text = str(data)
        receive_id = self._ids.get(topic)
        if receive_id is None:
            LOGGER.debug(f"[feishu] topic not found: {topic}")
            LOGGER.debug(f'[feishu] notify_exception...: {text}')
            return
        if text is not None:
            message = f"{e}\n{traceback.format_exc()}\n{text}"
        else:
            message = f"{e}\n{traceback.format_exc()}"
        await self.notify(message, topic)


async def notify(message: str, topic='default'):
    await Feishu().notify(message, topic)


async def notify_exception(e: Exception, data: str | dict | None = None, topic='default'):
    await Feishu().notify_exception(e, data, topic)
