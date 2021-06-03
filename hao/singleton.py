# -*- coding: utf-8 -*-
from multiprocessing import Lock

from . import logs

LOGGER = logs.get_logger(__name__)


class Singleton(type):
    _instances = {}
    _lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            instance = cls._instances.get(cls)
            if not instance:
                _instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
            return instance


class Multiton(type):
    _instances = {}
    _lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            key = (list(args) + list(kwargs.values()) + [None])[0]
            instance = cls._instances.get((cls, key))
            if instance is None:
                instance = super().__call__(*args, **kwargs)
                cls._instances[(cls, key)] = instance
            return instance
