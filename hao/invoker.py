# -*- coding: utf-8 -*-
import inspect


def invoke(function, **kwargs):
    spec = inspect.getfullargspec(function)
    args, varargs, varkw, defaults, kwonlyargs, kwonlydefaults, ann = spec
    if varargs or varkw:
        return function(**kwargs)

    valid_params = set(args)
    params = {k: v for k, v in kwargs.items() if k in valid_params}
    return function(**params)
