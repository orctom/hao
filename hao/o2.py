# -*- coding: utf-8 -*-
import base64
import json
import logging
import traceback
from datetime import datetime
from typing import Union

import requests

from . import config, decorators, paths, versions

LOGGER = logging.getLogger(__name__)

_O2 = config.get('o2')
_ENDPOINT, _HEADERS, _PROJECT_NAME, _PROGRAM_NAME = None, None, None, None
if _O2 is not None:
    _host = _O2.get('host')
    _user = _O2.get('user')
    _password = _O2.get('password')
    _org = _O2.get('org')
    _stream = _O2.get('stream')
    _credential = base64.b64encode(bytes(f"{_user}:{_password}", 'utf-8')).decode('utf-8')
    _ENDPOINT = f"{_host}/api/{_org}/{_stream}/_json"
    _HEADERS = {'Content-type': 'application/json', 'Authorization': f"Basic {_credential}"}
    _PROJECT_NAME = paths.project_name()
    _PROGRAM_NAME = paths.program_name()
_VERSION = versions.get_version() or 'dev'


@decorators.background
def notify(message: Union[str, dict]):
    if message is None:
        return

    if _ENDPOINT is None:
        LOGGER.debug('o2 not configured')
        LOGGER.info(message)
        return

    try:
        if isinstance(message, str):
            message = {'msg': message}
        message.update({
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'host': config.HOSTNAME,
            'project_name': _PROJECT_NAME,
            'program_name': _PROGRAM_NAME,
            'version': _VERSION,
        })
        response = requests.post(_ENDPOINT, headers=_HEADERS, data=json.dumps(message))
        LOGGER.debug(response.text)
    except Exception as e:
        LOGGER.debug(e)


def notify_exception(e: Exception, message: Union[str, dict] = None):
    if message is None:
        message = {}
    elif isinstance(message, str):
        message = {'msg': message}
    message['exception'] = f"{e}\n{traceback.format_exc()}"
    notify(message)
