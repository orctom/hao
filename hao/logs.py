# -*- coding: utf-8 -*-
import argparse
import logging
import os
import sys
from logging import handlers as logging_handlers
import typing

from . import paths, config, invoker

LOGGER_FORMAT = config.get('logger.format', "%(asctime)s %(levelname)-7s %(name)s:%(lineno)-4d - %(message)s")
LOGGER_FORMATTER = logging.Formatter(LOGGER_FORMAT)

_LOGGING_LEVELS = config.get('logging', {})
_LOGGING_LEVEL_ROOT = _LOGGING_LEVELS.get('root', 'INFO')

_HANDLERS: typing.List[logging.Handler] = []

_LOGGERS = {}
_LOGGER_DIR = config.get_path('logger.dir', 'data/logs')


def _get_handlers():
    global _HANDLERS
    if len(_HANDLERS) > 0:
        return _HANDLERS

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(LOGGER_FORMATTER)
    _HANDLERS.append(stream_handler)

    log_filename_arg = _get_logger_filename_arg()
    log_filename_fallback = _get_logger_filename_fallback()

    logger_handlers = config.get('logger.handlers')
    if logger_handlers:
        for handler_name, handler_args in logger_handlers.items():
            handler_cls = _get_handler_cls(handler_name)
            if handler_cls is None:
                print('[logger] skip invalid logger handler: ', handler_name)
                continue
            handler_args = _updated_handler_args(handler_args, log_filename_arg, log_filename_fallback)
            handler = invoker.invoke(handler_cls, **handler_args)
            handler.setFormatter(LOGGER_FORMATTER)
            _HANDLERS.append(handler)
            log_path = handler_args.get('filename')
            if log_path:
                print(f'logging to: {log_path} [{handler_name}]')
    else:
        if log_filename_arg:
            from datetime import datetime
            params = {
                'date': datetime.now().strftime('%y%m%d'),
                'datehour': datetime.now().strftime('%y%m%d-%H'),
                'datetime': datetime.now().strftime('%y%m%d-%H%M%S')
            }
            log_filename = log_filename_arg.format(**params)
            log_path = paths.get(_LOGGER_DIR, log_filename)
            handler = logging.FileHandler(log_path)
            handler.setFormatter(LOGGER_FORMATTER)
            _HANDLERS.append(handler)
            print(f'logging to: {log_path} [FileHandler]')

    return _HANDLERS


def _get_handler_cls(handler_name):
    if hasattr(logging, handler_name):
        return getattr(logging, handler_name)
    if hasattr(logging_handlers, handler_name):
        return getattr(logging_handlers, handler_name)
    return None


def _updated_handler_args(handler_args: dict, log_filename_arg: str, log_filename_fallback: str):
    if handler_args is None:
        handler_args = {}
    filename = handler_args.get('filename')
    if filename:
        if filename[0] in ('/', '~', '$'):
            filename = paths.get(filename)
        else:
            filename = paths.get(_LOGGER_DIR, filename)
    else:
        filename = paths.get(_LOGGER_DIR, log_filename_arg or log_filename_fallback)

    handler_args['filename'] = filename
    paths.make_parent_dirs(filename)

    return handler_args


def _get_logger_filename_arg():
    parser = argparse.ArgumentParser(prog='logs', add_help=False)
    parser.add_argument('--log_filename', required=False)
    args, _ = parser.parse_known_args()
    return args.log_filename


def _get_logger_filename_fallback():
    return f"{paths.program_name()}.log"


def get_logger(name=None, level=None):
    global _LOGGERS
    if name is None:
        name = paths.who_called_me()

    logger = _LOGGERS.get(name)
    if logger is None:
        logger = _get_logger(name, level)
        _LOGGERS[name] = logger
    elif level is not None:
        logger.setLevel(level)

    # update other modules that newly imported
    for _name in logging.root.manager.loggerDict:
        if _name not in _LOGGERS:
            _LOGGERS[_name] = _get_logger(_name)

    return logger


def _get_logger(name, level=None):
    _logger = logging.getLogger(name)
    _logger.setLevel(level or _get_logging_level(name))
    _logger.handlers.clear()

    handlers = _get_handlers()
    _logger.propagate = False
    if os.environ.get('SCRAPY_PROJECT') is not None:
        if len(handlers) > 0:
            _logger.addHandler(handlers[0])
    else:
        for handler in handlers:
            _logger.addHandler(handler)

    if level:
        _LOGGING_LEVELS[name] = level

    return _logger


def _get_logging_level(name):
    while True:
        level = _LOGGING_LEVELS.get(name)
        if level is not None:
            return level
        end = name.rfind('.')
        if end <= 0:
            return _LOGGING_LEVEL_ROOT
        name = name[:end]


def update_logger_levels(logging_levels: dict):
    if logging_levels is None or len(logging_levels) == 0:
        return
    for module, level in sorted(logging_levels.items()):
        update_logger_level(module, level)


def update_logger_level(module: str, level):
    _logger = _LOGGERS.get(module)
    if _logger:
        _logger.setLevel(level)

    for _module, _logger in _LOGGERS.items():
        if _module.startswith(module):
            _logger.setLevel(level)


def _config_base_logger():
    logging.basicConfig(**{
        'handlers': _get_handlers(),
        'format': LOGGER_FORMAT,
        'level': _LOGGING_LEVEL_ROOT
    })


_config_base_logger()
