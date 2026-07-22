# -*- coding: utf-8 -*-
"""
####################################################
###########          usage              ############
####################################################
from hao.networks import is_open, is_port_open, get_local_ip
await is_open('www.baidu.com', 80)
await is_port_open(8080)
"""
import asyncio
import logging
import socket

LOGGER = logging.getLogger(__name__)


async def is_open(host, port, timeout=1):
    try:
        _, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
        writer.close()
        await writer.wait_closed()
        return True
    except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
        return False


async def is_port_open(port: int) -> bool:
    return await is_open('127.0.0.1', port)


def get_local_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('10.255.255.255', 1))
        return s.getsockname()[0]
    except Exception:
        return '127.0.0.1'
    finally:
        s.close()
