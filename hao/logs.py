# -*- coding: utf-8 -*-
import fnmatch
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

from . import paths, config, envs

LOGGER_FORMAT = config.get('logger.format', "%(asctime)s %(levelname)-7s %(name)s:%(lineno)-4d - %(message)s")
LOGGER_FILE_ROTATE_WHEN = os.environ.get('logger.file.rotate.when', 'd')
LOGGER_FILE_ROTATE_COUNT = os.environ.get('logger.file.rotate.count', 3)

LOGGING_LEVEL_ROOT = config.get('logging.root', 'INFO')

PROJECT_NAME = paths.project_name()
PROGRAM_NAME = paths.program_name()

LOGGER_FORMATTER = logging.Formatter(LOGGER_FORMAT)

_stream_handler = None
_file_handler = None

_loggers = {}


def get_stream_handler():
    global _stream_handler
    if _stream_handler is not None:
        return _stream_handler
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(LOGGER_FORMATTER)
    _stream_handler = stream_handler
    return stream_handler


def get_file_handler():
    global _file_handler
    if _file_handler is not None:
        return _file_handler
    file_handler = TimedRotatingFileHandler(
        LOG_FILE_PATH,
        when=LOGGER_FILE_ROTATE_WHEN,
        encoding='utf8',
        backupCount=LOGGER_FILE_ROTATE_COUNT
    )
    file_handler.setFormatter(LOGGER_FORMATTER)
    _file_handler = file_handler
    return file_handler


def config_logger(log_to_file=False):
    if log_to_file:
        handlers = [get_stream_handler(), get_file_handler()]
    else:
        handlers = [get_stream_handler()]
    logging.basicConfig(handlers=handlers, format=LOGGER_FORMAT, level=LOGGING_LEVEL_ROOT)


try:
    DATA_DIR = config.get_path('DATA_DIR')
    LOGGING_LEVELS = config.get('logging', {})
    if DATA_DIR is not None and not envs.is_in_docker():
        LOGGER_FILE_ENABLED = os.environ.get('logger.file.enabled', True)
        LOG_FILE_PATH = os.path.join(DATA_DIR, 'logs', PROJECT_NAME, PROGRAM_NAME + '.log')
        paths.make_parent_dirs(LOG_FILE_PATH)
        print('logging to file:', LOG_FILE_PATH)
        config_logger(True)
    else:
        DATA_DIR = None
        LOGGER_FILE_ENABLED = False
        LOG_FILE_PATH = None
        config_logger(False)
except ModuleNotFoundError as err:
    logging.warn(f"Logging config not loaded, due to: {err}")
    DATA_DIR = None
    LOGGER_FILE_ENABLED = False
    LOG_FILE_PATH = None
    config_logger(False)


def get_logger(name=None, level=None):
    global _loggers
    if name is None:
        name = paths.who_called_me()

    for _name in logging.root.manager.loggerDict:
        if _name not in _loggers:
            _loggers[_name] = _get_logger(_name)

    if name in _loggers:
        return _loggers.get(name)

    _logger = _get_logger(name, level)
    _loggers[name] = _logger
    return _logger


def _get_logger(name, level=None):
    _logger = logging.getLogger(name)
    _logger.setLevel(level or get_logging_level(name))
    _logger.handlers.clear()
    _logger.addHandler(get_stream_handler())
    LOGGING_LEVELS[name] = level
    if os.environ.get('SCRAPY_PROJECT') is None:
        _logger.propagate = False
        if LOGGER_FILE_ENABLED:
            _logger.addHandler(get_file_handler())

    return _logger


def get_logging_level(name):
    while True:
        level = LOGGING_LEVELS.get(name)
        if level is not None:
            return level
        end = name.rfind('.')
        if end <= 0:
            return LOGGING_LEVEL_ROOT
        name = name[:end]


def update_logger_levels(logging_levels=None):
    if logging_levels is None or len(logging_levels) == 0:
        return
    for module, level in logging_levels.items():
        update_logger_level(module, level)


def update_logger_level(module: str, level):
    _logger = _loggers.get(module)
    if _logger:
        _logger.setLevel(level)
        LOGGING_LEVELS[module] = level
        return

    if '*' in module:
        for _module, _logger in _loggers.items():
            fnmatch.fnmatch(_module, module)
            if not fnmatch.fnmatch(_module, module):
                continue
            _logger.setLevel(level)
            LOGGING_LEVELS[_module] = level
    else:
        _loggers[module] = _get_logger(module, level)
