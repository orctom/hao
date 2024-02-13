# -*- coding: utf-8 -*-
import argparse
import sys
from typing import Callable, List, Optional, Union

import regex

_PARSER = argparse.ArgumentParser(formatter_class=argparse.MetavarTypeHelpFormatter, add_help=False, conflict_handler='resolve')


def get_arg(name: str, type: type = str, help=None, required=False, default=None):
    parser = _PARSER.add_argument_group('logs')
    desc = ' '.join(filter(None, [
        '[required]' if required else '[optional]',
        help,
        f"(default: {default})" if default else None
    ]))
    parser.add_argument(f"--{name}", type=type, help=desc, required=required, default=default)
    ns, _ = _PARSER.parse_known_args()
    return getattr(ns, name.replace('-', '_'), default)


def add_argument_group(*args, **kwargs):
    return _PARSER.add_argument_group(*args, **kwargs)


def parse_known_args(args=None, namespace=None):
    return _PARSER.parse_known_args(args, namespace)


def add_by_function(addon_fn: Optional[Union[List[Callable], Callable]]):
    if addon_fn is None:
        return {}
    _names_before = set([action.dest for action in getattr(_PARSER, '_actions')])
    if isinstance(addon_fn, list):
        for add_fn in addon_fn:
            add_fn(_PARSER)
    elif callable(addon_fn):
        addon_fn(_PARSER)
    return {action.dest: action for action in getattr(_PARSER, '_actions') if action.dest not in _names_before}


def args_as_dict() -> dict:
    args_str = ' '.join(sys.argv[1:])
    args = [regex.compile(r'(?:\s*=\s*|\s+)').split(item) for item in regex.compile(r'\s*\-{2}').split(args_str) if item]
    return {a[0]: a[1] if len(a) == 2 else None for a in args}


def print_help():
    _PARSER.print_help()


def print_usage():
    _PARSER.print_usage()
