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
_LOGGER_FILENAME: typing.Optional[str] = None


def _get_handlers():
    global _HANDLERS
    if len(_HANDLERS) > 0:
        return _HANDLERS

    handlers = []
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(LOGGER_FORMATTER)
    handlers.append(stream_handler)

    for handler_name, handler_config in config.get('logger.handlers', {}).items():
        handler_cls = getattr(logging, handler_name) or getattr(logging_handlers, handler_name)
        if handler_cls is None:
            print('[logger] skip invalid logger handler: ', handler_name)
            continue
        handler = invoker.invoke(handler_cls, **_updated_handler_config(handler_config))
        handler.setFormatter(LOGGER_FORMATTER)
        handlers.append(handler)
    return handlers


def _updated_handler_config(handler_config: dict):
    if handler_config is None:
        handler_config = {}
    filename = handler_config.get('filename')
    log_file_dir = config.get_path('logger.dir', 'data/logs')
    if filename:
        if filename[0] in ('/', '~', '$'):
            filename = paths.get(filename)
        else:
            filename = paths.get(log_file_dir, filename)
    else:
        filename = _get_logger_filename()

    handler_config['filename'] = filename
    paths.make_parent_dirs(filename)

    return handler_config


def _get_logger_filename():
    global _LOGGER_FILENAME
    if _LOGGER_FILENAME is None:
        parser = argparse.ArgumentParser()
        parser.add_argument('--log_filename', required=False)
        args, _ = parser.parse_known_args()
        log_file_dir = config.get_path('logger.dir', 'data/logs')
        filename = paths.get(log_file_dir, args.log_filename or f"{paths.program_name()}.log")
        print('logging to:', filename)
        _LOGGER_FILENAME = filename
    return _LOGGER_FILENAME


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
    _LOGGING_LEVELS.update(logging_levels)
    for module, level in sorted(_LOGGING_LEVELS.items()):
        update_logger_level(module, level)


def update_logger_level(module: str, level):
    _logger = _LOGGERS.get(module)
    if _logger:
        _logger.setLevel(level)
        return

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
