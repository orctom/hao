# -*- coding: utf-8 -*-
from multiprocessing import Lock


class Singleton(type):
    _instance = None

    _lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if not cls._instance:
                cls._instance = super().__call__(*args, **kwargs)
            return cls._instance
