# -*- coding: utf-8 -*-
import json
import traceback
from datetime import datetime

import requests

from . import logs, versions, config, paths

LOGGER = logs.get_logger(__name__)

identifier = f'[{config.HOSTNAME}-{paths.project_name()}-{paths.program_name()}]'
version = versions.get_version()

_SLACK_TOKENS = config.get('slack')


def _get_token(channel):
    try:
        return _SLACK_TOKENS.get(channel)
    except (AttributeError, ValueError):
        return None


def notify(message: str, channel='default'):
    token = _get_token(channel)
    if token is None:
        LOGGER.debug(f"channel not found: {channel}")
        LOGGER.info(message)
        return
    try:
        url = f'https://hooks.slack.com/services/{token}'
        headers = {'content-type': 'application/json'}
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        payload = {'text': f"{identifier} {timestamp}\tversion: {version}\n```{message}```"}
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        LOGGER.info(response.text)
    except Exception as e:
        LOGGER.error(e)


def notify_exception(e: Exception, text: str = None, channel='default'):
    token = _get_token(channel)
    if token is None:
        LOGGER.debug(f"channel not found: {channel}")
        LOGGER.debug(f'notify_exception...: {text}')
        return
    if text is not None:
        message = f"{e}\n{traceback.format_exc()}\n{text}"
    else:
        message = f"{e}\n{traceback.format_exc()}"
    notify(message)
