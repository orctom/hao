# -*- coding: utf-8 -*-
import inspect

from . import logs

LOGGER = logs.get_logger(__name__)


def invoke(function, **kwargs):
    valid_params = inspect.signature(function).parameters.keys()
    params = {k: v for k, v in kwargs.items() if k in valid_params or k == 'kwargs'}
    return function(**params)
