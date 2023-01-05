# -*- coding: utf-8 -*-
import os
import time

from . import config
from .decorators import synchronized
from .logs import get_logger
from .singleton import Singleton

LOGGER = get_logger(__name__)

EPOCH_TIMESTAMP = 1577836800000  # 2020-01-01T00:00:00


class UUID(object, metaclass=Singleton):
    """
    host bits: 4, (0 - 15)
    pid bits : 22, (0 - 4194303)
    seq bits : 12, (0 - 4095)
    """

    def __init__(self) -> None:
        host_id = config.get('uuid.host_id', 0)
        assert 0 <= host_id <= 15, 'host_id must be in range of 0 - 15'
        LOGGER.info(f"[uuid] host id: {host_id}")
        pid = os.getpid()
        self._instance_id = ((host_id & 15) << 22) | (pid & 4194303)
        self._last_timestamp = EPOCH_TIMESTAMP
        self._sequence = 0

    @synchronized
    def get(self):
        now = int(time.time() * 1000)

        if now < self._last_timestamp:
            raise ValueError(f"Clock went backwards! {now} < {self._last_timestamp}")

        if now > self._last_timestamp:
            self._sequence = 0
            self._last_timestamp = now

        self._sequence += 1

        if self._sequence > 4095:
            self.sequence_overload += 1
            time.sleep(0.001)
            return self.get()

        return ((now - EPOCH_TIMESTAMP) << 38) | (self._instance_id << 12) | self._sequence
