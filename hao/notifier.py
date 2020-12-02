# -*- coding: utf-8 -*-
import json
import os
import traceback
import warnings
from datetime import datetime

import requests

from . import logs, versions, config

LOGGER = logs.get_logger(__name__)

app_name = os.environ.get("app_name", '')
identifier = f'[{config.HOSTNAME}-{app_name}]'
version = versions.get_version()

SLACK_TOKEN = config.get('SLACK_TOKEN')


def notify(message: str):
    warnings.warn('please use hao.slack.notify(msg, channel)', DeprecationWarning)
    if config.is_not_production() or SLACK_TOKEN is None:
        LOGGER.info(message)
        return
    try:
        url = f'https://hooks.slack.com/services/{SLACK_TOKEN}'
        headers = {'content-type': 'application/json'}
        job_id = os.environ.get('SCRAPY_JOB', '')
        payload = {'text': f"{identifier} {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\t{job_id}\tversion: {version}\n```{message}```"}
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        LOGGER.info(response.text)
    except Exception as e:
        LOGGER.error(e)


def notify_exception(e: Exception, text: str = None):
    if config.is_not_production():
        LOGGER.debug(f'notify_exception...: {text}')
        return
    if text is not None:
        message = f"{e}\n{traceback.format_exc()}\n{text}"
    else:
        message = f"{e}\n{traceback.format_exc()}"
    notify(message)
