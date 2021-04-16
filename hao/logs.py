# -*- coding: utf-8 -*-
import fnmatch
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

import typing

from . import paths, config, envs

LOGGER_FORMAT = config.get('logger.format', "%(asctime)s %(levelname)-7s %(name)s:%(lineno)-4d - %(message)s")
LOGGER_FORMATTER = logging.Formatter(LOGGER_FORMAT)

_LOGGING_LEVELS = config.get('logging', {})
_LOGGING_LEVEL_ROOT = _LOGGING_LEVELS.get('root', 'INFO')

_STREAM_HANDLER: typing.Optional[logging.Handler] = None
_FILE_HANDLER: typing.Optional[logging.Handler] = None

_LOGGERS = {}


def get_stream_handler():
    global _STREAM_HANDLER
    if _STREAM_HANDLER is not None:
        return _STREAM_HANDLER
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(LOGGER_FORMATTER)
    _STREAM_HANDLER = stream_handler
    return stream_handler


def get_file_handler(log_file_path: str = None):
    global _FILE_HANDLER
    if log_file_path is None:
        return _FILE_HANDLER
    file_handler = TimedRotatingFileHandler(
        log_file_path,
        when=config.get('logger.file.rotate.when', 'd'),
        encoding='utf8',
        backupCount=config.get('logger.file.rotate.count', 7)
    )
    file_handler.setFormatter(LOGGER_FORMATTER)
    _FILE_HANDLER = file_handler
    return file_handler


def config_logger(log_file_path: str = None):
    if log_file_path:
        handlers = [get_stream_handler(), get_file_handler(log_file_path)]
    else:
        handlers = [get_stream_handler()]
    logging.basicConfig(handlers=handlers, format=LOGGER_FORMAT, level=_LOGGING_LEVEL_ROOT)


def get_log_file_path():
    if envs.is_in_docker():
        return None
    log_file_dir = config.get_path('logger.file.dir')
    log_file_enabled = config.get('logger.file.enabled', True)
    if log_file_dir is None or not log_file_enabled:
        return None
    path = paths.get(log_file_dir, f"{paths.program_name()}.log")
    paths.make_parent_dirs(path)
    return path


def config_base_logger():
    try:
        log_file_path = get_log_file_path()
        if log_file_path:
            print('logging to file:', log_file_path)
            config_logger(log_file_path)
        else:
            config_logger()
    except ModuleNotFoundError as err:
        logging.warn(f"Logging config not loaded, due to: {err}")
        config_logger()


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
    _logger.setLevel(level or get_logging_level(name))
    _logger.handlers.clear()
    _logger.addHandler(get_stream_handler())
    if level:
        _LOGGING_LEVELS[name] = level
    if os.environ.get('SCRAPY_PROJECT') is None:
        _logger.propagate = False
        file_handler = get_file_handler()
        if file_handler is not None:
            _logger.addHandler(file_handler)

    return _logger


def get_logging_level(name):
    while True:
        level = _LOGGING_LEVELS.get(name)
        if level is not None:
            return level
        end = name.rfind('.')
        if end <= 0:
            return _LOGGING_LEVEL_ROOT
        name = name[:end]


def update_logger_levels(logging_levels=None):
    if logging_levels is None or len(logging_levels) == 0:
        return
    for module, level in logging_levels.items():
        update_logger_level(module, level)


def update_logger_level(module: str, level):
    _logger = _LOGGERS.get(module)
    if _logger:
        _logger.setLevel(level)
        _LOGGING_LEVELS[module] = level
        return

    if '*' in module:
        for _module, _logger in _LOGGERS.items():
            fnmatch.fnmatch(_module, module)
            if not fnmatch.fnmatch(_module, module):
                continue
            _logger.setLevel(level)
            _LOGGING_LEVELS[_module] = level
    else:
        _LOGGERS[module] = _get_logger(module, level)


config_base_logger()
