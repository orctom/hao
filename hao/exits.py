# -*- coding: utf-8 -*-
import abc
import signal

_handlers = []


def _on_exit(*a, **kw):
    while _handlers:
        handler, args, kwargs = _handlers.pop()
        handler(*args, **kwargs)


signal.signal(signal.SIGTERM, _on_exit)


def on_exit(func, *a, **kw):
    _handlers.append((func, a, kw))
    print('added func', func)
    return func


class OnExit(abc.ABC):

    def __init__(self) -> None:
        super().__init__()
        on_exit(self.on_exit)

    @abc.abstractmethod
    def on_exit(self):
        raise NotImplementedError()
