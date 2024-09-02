# -*- coding: utf-8 -*-
import socket

from . import logs

LOGGER = logs.get_logger(__name__)


def is_open(host, port):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex((host, port))
        LOGGER.debug(f'{host}:{port} -> {result}')
        return 0 == result
    except socket.gaierror:
        return False
