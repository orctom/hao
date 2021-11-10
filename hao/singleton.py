# -*- coding: utf-8 -*-
import collections
from multiprocessing import Lock

_SINGLETON_INSTANCES = {}
_SINGLETON_LOCKS = collections.defaultdict(Lock)
_MULTITON_INSTANCES = {}
_MULTITON_LOCKS = collections.defaultdict(Lock)


class Singleton(type):

    def __call__(cls, *args, **kwargs):
        instance = _SINGLETON_INSTANCES.get(cls)
        if instance is None:
            with _SINGLETON_LOCKS[cls]:
                instance = _SINGLETON_INSTANCES.get(cls)
                if instance is None:
                    instance = super().__call__(*args, **kwargs)
                    _SINGLETON_INSTANCES[cls] = instance
        return instance


class Multiton(type):

    def __call__(cls, *args, **kwargs):
        key = (list(args) + list(kwargs.values()) + [None])[0]
        instance = _MULTITON_INSTANCES.get((cls, key))
        if instance is None:
            with _MULTITON_LOCKS[cls]:
                instance = _MULTITON_INSTANCES.get((cls, key))
                if instance is None:
                    instance = super().__call__(*args, **kwargs)
                    _MULTITON_INSTANCES[(cls, key)] = instance
        return instance
