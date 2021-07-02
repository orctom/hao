# -*- coding: utf-8 -*-
import inspect


def invoke(function, **kwargs):
    valid_params = inspect.signature(function).parameters.keys()
    params = {k: v for k, v in kwargs.items() if k in valid_params or k == 'kwargs'}
    return function(**params)
