# -*- coding: utf-8 -*-
import argparse

_PARSER = argparse.ArgumentParser(formatter_class=argparse.MetavarTypeHelpFormatter, add_help=False)


def get_arg(name: str, type: type = str, help=None, required=False, default=None):
    parser = _PARSER.add_argument_group('logs')
    desc = ' '.join(filter(None, [
        '[required]' if required else '[optional]',
        help,
        f"(default: {default})" if default else None
    ]))
    parser.add_argument(f"--{name}", type=type, help=desc, required=required, default=default)
    ns, _ = _PARSER.parse_known_args()
    return getattr(ns, name, default)


def add_argument_group(*args, **kwargs):
    return _PARSER.add_argument_group(*args, **kwargs)


def parse_known_args(args=None, namespace=None):
    return _PARSER.parse_known_args(args, namespace)


def print_help():
    _PARSER.print_help()


def print_usage():
    _PARSER.print_usage()
