# -*- coding: utf-8 -*-
import json
import logging
import traceback
import typing
from datetime import datetime

import requests

from . import config, decorators, jsons, paths, versions

LOGGER = logging.getLogger(__name__)


_SLACK_TOKENS = config.get('slack')
_IDENTIFIER = None


def slack_token(channel):
    try:
        return _SLACK_TOKENS.get(channel)
    except (AttributeError, ValueError):
        return None


def identifier():
    global _IDENTIFIER
    if _IDENTIFIER is None:
        _IDENTIFIER = f'[{config.HOSTNAME}-{paths.project_name()}-{paths.program_name()}]'
    return _IDENTIFIER


@decorators.background
def notify(message: str, channel='default'):
    token = slack_token(channel)
    if token is None:
        LOGGER.debug(f"channel not found: {channel}")
        LOGGER.info(message)
        return
    try:
        url = f'https://hooks.slack.com/services/{token}'
        headers = {'content-type': 'application/json'}
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        payload = {'text': f"{identifier()} {timestamp}\tversion: {versions.get_version()}\n```{message}```"}
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        LOGGER.info(response.text)
    except Exception as e:
        LOGGER.debug(e)


def notify_exception(e: Exception, data: typing.Union[str, dict] = None, channel='default'):
    token = slack_token(channel)
    if isinstance(data, dict):
        text = jsons.dumps(data)
    else:
        text = str(data)
    if token is None:
        LOGGER.debug(f"channel not found: {channel}")
        LOGGER.debug(f'notify_exception...: {text}')
        return
    if text is not None:
        message = f"{e}\n{traceback.format_exc()}\n{text}"
    else:
        message = f"{e}\n{traceback.format_exc()}"
    notify(message)
