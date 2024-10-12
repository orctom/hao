# -*- coding: utf-8 -*-
import socket

from . import logs

LOGGER = logs.get_logger(__name__)


def is_open(host, port, timeout = 0.2):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(timeout)
            result = sock.connect_ex((host, port))
            LOGGER.debug(f'{host}:{port} -> {result}')
            return 0 == result
    except socket.gaierror:
        return False
