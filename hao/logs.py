# -*- coding: utf-8 -*-
import logging
import os
import sys
from datetime import datetime
from logging import handlers as logging_handlers
from typing import Dict, List, Optional

from . import args, config, invoker, paths

LOGGER_FORMAT = config.get('logger.format', '%(asctime)s %(levelname)-7s %(name)s:%(lineno)-4d - %(message)s')
LOGGER_FORMATTER = logging.Formatter(LOGGER_FORMAT)
LOGGER_DIR = config.get_path('logger.dir', 'data/logs')


class Handlers:
    def __init__(self) -> None:
        self._app_name = paths.program_name()
        self._logger_filename_arg = args.get_arg('log-to', help='abstract path or relative path to `{project-root}/data/logs`')
        self._handlers: Dict[str, logging.Handler] = self._load()
        self._default_handlers = self.get_handlers(['stdout', 'log-to'])

    def add_handler(self, name: str, handler: logging.Handler):
        self._handlers[name] = handler

    def get_handler(self, name: str):
        handler = self._handlers.get(name)
        if handler is None and name not in ('stdout', 'log-to'):
            print(f"[logger] handler not found in configure: {name}")
        return handler

    def get_handlers(self, names: Optional[List[str]] = None):
        if not names:
            return self._default_handlers
        return list(filter(None, [self.get_handler(name) for name in set(names) if name]))

    @staticmethod
    def get_formatter(fmt: Optional[str] = None):
        return logging.Formatter(fmt) if fmt else LOGGER_FORMATTER

    @staticmethod
    def _get_handler_cls(handler_name):
        if not handler_name:
            return None
        if hasattr(logging, handler_name):
            return getattr(logging, handler_name)
        if hasattr(logging_handlers, handler_name):
            return getattr(logging_handlers, handler_name)
        return None

    def _updated_handler_args(self, handler_args: dict):
        if handler_args is None:
            handler_args = {}
        handler_args['filename'] = filename = self._get_log_path(handler_args.get('filename'))
        if filename:
            paths.make_parent_dirs(filename)
        return handler_args

    def _get_log_path(self, filename: str):
        filename = filename or f"{self._app_name}.log"
        if filename[0] in ('/', '~', '$'):
            return paths.get(filename)
        else:
            return paths.get(LOGGER_DIR, filename)

    def _load(self) -> Dict[str, logging.Handler]:
        def load_default():
            if 'stdout' in handlers:
                return
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(LOGGER_FORMATTER)
            handlers['stdout'] = handler

        def load_from_arg():
            if not self._logger_filename_arg or 'log-to' in handlers:
                return
            now = datetime.now()
            params = {
                'app': self._app_name,
                'date': now.strftime('%y%m%d'),
                'datehour': now.strftime('%y%m%d-%H'),
                'datetime': now.strftime('%y%m%d-%H%M%S'),
            }
            log_filename = self._logger_filename_arg.format(**params)
            log_path = self._get_log_path(log_filename)
            paths.make_parent_dirs(log_path)
            handler = logging.FileHandler(log_path)
            handler.setFormatter(LOGGER_FORMATTER)
            handlers['log-to'] = handler
            print(f'[logger] [log-to] -> {log_path}')

        def load_from_config():
            for handler_name, handler_config in config.get('logger.handlers', {}).items():
                try:
                    if not handler_config:
                        print(f"[logger] empty handler config: {handler_name}")
                        continue
                    if not isinstance(handler_config, dict):
                        print(f"[logger] dict config expected for handler: {handler_name}, found: {type(handler_config)}")
                        continue
                    if handler_name in ('stdout', 'log-to'):
                        if (fmt := handler_config.get('format')) is not None:
                            handlers['stdout'].setFormatter(self.get_formatter(fmt))
                        continue

                    handler_cls = self._get_handler_cls(handler_config.get('handler'))
                    if handler_cls is None:
                        print(f"[logger] class not found for handler: {handler_name}, {handler_config.get('handler')}")
                        continue
                    handler_cls_args = self._updated_handler_args(handler_config.get('args'))
                    handler = invoker.invoke(handler_cls, **handler_cls_args)
                    handler.setFormatter(self.get_formatter(handler_config.get('format')))
                    handlers[handler_name] = handler
                    log_path = handler_cls_args.get('filename')
                    if log_path:
                        print(f'[logger] [{handler_name}] -> {log_path}')
                except Exception as e:
                    print(f"[logger] Failed to create handler: {handler_name}, {e}")
                    break

        handlers: Dict[str, logging.Handler] = {}
        load_default()
        load_from_arg()
        load_from_config()
        return handlers


class Loggers:
    def __init__(self) -> None:
        self._loggers_config = {
            'root': {'level': 'INFO'},
            **{
                logger_name: {'level': logger_config} if isinstance(logger_config, str) else logger_config
                for logger_name, logger_config in config.get('logging', {}).items()
            }
        }
        self._default_level = self._loggers_config.get('root', {}).get('level', 'INFO')
        self._default_logger_config = {
            'level': self._default_level,
            'handlers': self._loggers_config.get('root', {}).get('handlers', ['stdout', 'log-to']),
        }
        self._loggers: Dict[str, logging.Logger] = self._load()
        self.update_imported_modules()

    def _get_logger_config(self, name):
        while True:
            logger_config = self._loggers_config.get(name)
            if logger_config:
                return {**self._loggers_config, **logger_config}
            end = name.rfind('.')
            if end <= 0:
                return self._default_logger_config
            name = name[:end]

    def _get_handlers(self, name):
        self._loggers_config.get(name)

    def _get_logger(self, name, level=None):
        _logger_config = self._get_logger_config(name)
        _logger = logging.getLogger(name)
        _logger.setLevel(level or _logger_config.get('level'))
        _logger.handlers.clear()
        _logger.propagate = False

        handlers = _HANDLERS.get_handlers(_logger_config.get('handlers'))
        if os.environ.get('SCRAPY_PROJECT') is not None:
            if len(handlers) > 0:
                _logger.addHandler(handlers[0])
        else:
            for handler in handlers:
                _logger.addHandler(handler)

        if level:
            self._loggers_config[name]['level'] = level

        return _logger

    def _load(self) -> Dict[str, logging.Logger]:
        loggers: Dict[str, logging.Logger] = {}
        for name in self._loggers_config:
            loggers[name] = self._get_logger(name)
        return loggers

    def get_logger(self, name: str, level: Optional[str] = None):
        logger = self._loggers.get(name)
        if logger and level:
            logger.setLevel(level)
            return logger

        logger = self._get_logger(name, level)
        self.update_imported_modules()
        return logger

    def update_imported_modules(self):
        """update other modules that newly imported"""
        for _name in list(logging.root.manager.loggerDict):
            if _name not in self._loggers:
                self._loggers[_name] = self._get_logger(_name)

    def update_levels(self, levels: dict):
        if levels is None or len(levels) == 0:
            return
        for name, level in sorted(levels.items()):
            self.update_level(name, level)

    def update_level(self, name: str, level):
        for _name, _logger in self._loggers.items():
            if _name == name or _name.startswith(name):
                _logger.setLevel(level)


_HANDLERS = Handlers()
_LOGGERS = Loggers()


def get_logger(name=None, level=None):
    if name is None:
        name = paths.who_called_me()

    return _LOGGERS.get_logger(name, level)


def update_logger_levels(logging_levels: dict):
    _LOGGERS.update_levels(logging_levels)


def update_level(module: str, level):
    _LOGGERS.update_level(module, level)


def _config_base_logger():
    logging.basicConfig(**{
        'handlers': _HANDLERS._default_handlers,
        'format': LOGGER_FORMAT,
        'level': _LOGGERS._default_level,
    })


_config_base_logger()
