# -*- coding: utf-8 -*-
import logging
import threading
import traceback
import typing
from datetime import datetime

import requests

from . import config, decorators, jsons, paths, singleton, versions

LOGGER = logging.getLogger(__name__)


_URL_TOKEN = 'https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal'
_URL_NOTIFY = 'https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id'
_HEADERS_TOKEN = {'Content-Type': 'application/json'}


class Feishu(metaclass=singleton.Singleton):
    def __init__(self):
        cfg = config.get('feishu')
        self._secrets = cfg.get('secrets')
        self._ids = cfg.get('ids')
        self._identifier = f'[{config.HOSTNAME}-{paths.project_name()}-{paths.program_name()}]'
        self._aaccess_token = None
        self._refresh_access_token()

    def _refresh_access_token(self):
        payload = {'app_id': self._secrets.get('app_id'), 'app_secret': self._secrets.get('app_secret')}
        response = requests.post(_URL_TOKEN, headers=_HEADERS_TOKEN, json=payload)
        response.raise_for_status()
        data = response.json()
        expire, self._aaccess_token = data.get('expire'), data.get('tenant_access_token')
        LOGGER.info(f"[feishu] token refreshed: {self._aaccess_token}, expire: {expire}")
        t = threading.Timer(expire - 10, self._refresh_access_token)
        t.daemon = True
        t.start()

    @decorators.background
    def notify(self, message: str, topic='default'):
        receive_id = self._ids.get(topic)
        if receive_id is None:
            LOGGER.debug(f"[feishu] topic not found: {topic}")
            LOGGER.info(message)
            return
        try:
            headers = {'Content-Type': 'application/json', 'Authorization': f"Bearer {self._aaccess_token}"}
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            content = {
            	"zh_cn": {
            		"title": f"{self._identifier} {timestamp}\tversion: {versions.get_version() or 'dev'}",
            		"content": [
            			[{
            				"tag": "code_block",
            				"text": message,
            			}]
            		]
            	}
            }
            data = {
                'content': jsons.dumps(content),
                'msg_type': 'post',
                'receive_id': receive_id,
            }
            response = requests.post(_URL_NOTIFY, json=data, headers=headers)
            response.raise_for_status()
        except Exception as e:
            LOGGER.debug(e)

    def notify_exception(self, e: Exception, data: typing.Union[str, dict] = None, topic='default'):
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
        self.notify(message, topic)


def notify(message: str, topic='default'):
    Feishu().notify(message, topic)


def notify_exception(e: Exception, data: typing.Union[str, dict] = None, topic='default'):
    Feishu().notify_exception(e, data, topic)
