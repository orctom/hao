# -*- coding: utf-8 -*-
import contextlib
from importlib.util import module_from_spec, spec_from_file_location
import os
import sys
import time

TYPE_MODULE = type(os)


def fqdn(module):
    mod = getattr(module, '__module__', None)
    if mod in ('__builtin__', ):
        mod = None

    if not isinstance(module, type) and (cls := getattr(module, '__class__', None)) not in (type, TYPE_MODULE, None):
        target = cls
    else:
        target = module
    name = getattr(target, '__qualname__', None) or getattr(target, '__name__', None)

    return '.'.join(filter(None, [mod, name]))


def import_from_file(filepath: str, silent: bool = False):
    now = int(time.time())
    module_name = f"hao.import.{os.path.dirname(filepath)}{now}".replace('/', '.')
    s = spec_from_file_location(module_name, filepath)
    m = module_from_spec(s)
    sys.modules[module_name] = m
    if silent:
        with open(os.devnull, 'w') as devnull:
            with contextlib.redirect_stdout(devnull):
                try:
                    s.loader.exec_module(m)
                finally:
                    sys.modules.pop(module_name, None)
    else:
        try:
            s.loader.exec_module(m)
        finally:
            sys.modules.pop(module_name, None)
    return m
