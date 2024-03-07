# -*- coding: utf-8 -*-
import abc
import signal

_handlers = []


def _on_exit(*a, **kw):
    while _handlers:
        handler, args, kwargs = _handlers.pop()
        try:
            handler(*args, **kwargs)
        except Exception:
            pass


def on_exit(func, *a, **kw):
    _handlers.append((func, a, kw))
    return func


class OnExit(abc.ABC):

    def __init__(self) -> None:
        super().__init__()
        on_exit(self.on_exit)

    @abc.abstractmethod
    def on_exit(self):
        raise NotImplementedError()


def register_handler():
    for sig in (signal.SIGINT, signal.SIGTERM):
        if callable((_handler := signal.getsignal(sig))):
            _handlers.append((_handler, [], {}))

        signal.signal(sig, _on_exit)


register_handler()
