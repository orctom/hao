# -*- coding: utf-8 -*-
import typing


def throw(e: typing.Type[Exception], msg=None):
    if msg:
        raise e(msg)
    else:
        raise e()


class ConfigError(Exception):
    pass
