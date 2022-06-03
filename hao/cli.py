# -*- coding: utf-8 -*-
import argparse
import importlib
import inspect
import sys

from . import logs
from .paths import project_root_path
from .stopwatch import Stopwatch

LOGGER = logs.get_logger(__name__)


def run():
    argv = sys.argv
    n_args = len(argv)
    if n_args == 1:
        LOGGER.warn(f'Usage: spanner-cli module.func [params]')
        return

    try:
        func = _get_function(argv)
        parser = argparse.ArgumentParser(prog=f"spanner-run {argv[1]}")
        signature = inspect.signature(func)
        for name, param in signature.parameters.items():
            params = {}
            if param.annotation != param.empty :
                params['type'] = param.annotation
            if param.default != param.empty:
                params['default'] = param.default
            params['default'] = param.default
            parser.add_argument(f"--{name}", **params)

        if n_args >= 3 and argv[2] in ('-h', '--help'):
            parser.print_help()
            print(f"\nsignature:\n  {func.__name__}{str(signature)}")
            if func.__doc__:
                print(func.__doc__)
        else:
            args, _ = parser.parse_known_args(argv[2:])
            sw = Stopwatch()
            out = func(**vars(args))
            LOGGER.info(f"[out] {out}, took: {sw.took()}")
    except Exception as e:
        LOGGER.exception(e)


def _get_function(args: list):
    sys.path.insert(0, project_root_path())
    module_and_func = args[1]
    splits = module_and_func.split('.')
    if len(splits) == 1:
        module = globals()
        func_name = module_and_func
    else:
        try:
            module = importlib.import_module('.'.join(splits[:-1]))
        except ModuleNotFoundError as e:
            raise ValueError(e.msg)
        func_name = splits[-1]
    if not hasattr(module, func_name):
        raise ValueError(f"No function named: {func_name}")
    return getattr(module, func_name)
