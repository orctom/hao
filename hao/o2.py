# -*- coding: utf-8 -*-
import base64
import json
import logging
import traceback
from datetime import datetime

import aiohttp

from . import config, paths, versions

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

_session: aiohttp.ClientSession | None = None


async def _get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession()
    return _session


async def notify(message: str | dict):
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
        session = await _get_session()
        async with session.post(_ENDPOINT, headers=_HEADERS, data=json.dumps(message)) as response:
            LOGGER.debug(await response.text())
    except Exception as e:
        LOGGER.debug(e)


async def notify_exception(e: Exception, message: str | dict = None):
    if message is None:
        message = {}
    elif isinstance(message, str):
        message = {'msg': message}
    message['exception'] = f"{e}\n{traceback.format_exc()}"
    await notify(message)
