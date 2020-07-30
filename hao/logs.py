# -*- coding: utf-8 -*-
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

from . import paths, config, envs

LOGGER_FORMAT = config.get('logger.format', "%(asctime)s %(levelname)-7s %(name)s:%(lineno)-4d - %(message)s")
LOGGER_FILE_ROTATE_WHEN = config.get('logger.file.rotate.when', 'd')
LOGGER_FILE_ROTATE_COUNT = config.get('logger.file.rotate.count', 3)

LOGGING_LEVEL_ROOT = config.get('logging.root', 'WARNING')

PROJECT_NAME = paths.project_name()
PROGRAM_NAME = paths.program_name()

LOGGER_FORMATTER = logging.Formatter(LOGGER_FORMAT)

_stream_handler = None
_file_handler = None


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


def get_logger(name=None, level=LOGGING_LEVEL_ROOT):
    if name is None:
        name = paths.who_called_me()
    _logger = logging.getLogger(name)
    _logger.setLevel(get_logging_level(name, level))

    if len(_logger.handlers) > 0:
        return _logger

    _logger.addHandler(get_stream_handler())
    if os.environ.get('SCRAPY_PROJECT') is None:
        _logger.propagate = False
        if LOGGER_FILE_ENABLED:
            _logger.addHandler(get_file_handler())

    return _logger


def get_logging_level(name, default=logging.INFO):
    while True:
        level = LOGGING_LEVELS.get(name)
        if level is not None:
            return level
        end = name.rfind('.')
        if end <= 0:
            return default
        name = name[:end]


def update_loggers(_logging_levels=None, all_modules=False):
    if all_modules:
        for module in set([item.split('.')[0] for item in sys.modules.keys() if not item.startswith('_')]):
            update_logger(module, LOGGING_LEVEL_ROOT)

    if _logging_levels is None:
        _logging_levels = LOGGING_LEVELS

    logging.getLogger().setLevel(logging.INFO)
    for module, level in _logging_levels.items():
        update_logger(module, level)


def update_logger(module, level):
    logger = logging.getLogger(module)
    logger.setLevel(level)
    for handler in logger.handlers:
        handler.setLevel(level)
        handler.setFormatter(LOGGER_FORMATTER)


update_loggers()
