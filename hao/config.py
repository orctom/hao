# -*- coding: utf-8 -*-
import os
import socket
import traceback
from importlib.resources import open_text
from typing import Optional, Union

import yaml

from . import paths, singleton

ENV = os.environ.get("env")
HOSTNAME = socket.gethostname()


class Config(object, metaclass=singleton.Multiton):

    def __init__(self, config_name='config', module=None) -> None:
        super().__init__()
        self.config_name = config_name or 'config'  # force not none
        self.module = module
        self.config_dir = get_config_dir() or os.getcwd()
        self.conf = self.read_conf()

    def read_conf(self):
        try:
            if self.module is not None:
                return self._conf_from_module()

            if self.config_name.endswith('.yml'):
                return self._conf_from(self.config_name)

            if ENV is not None:
                for config_file_name in [f"{self.config_name}-{ENV}.yml", f"{self.config_name}.yml"]:
                    conf = self._conf_from(config_file_name)
                    if conf is not None:
                        return conf

            else:
                for config_file_name in [f"{self.config_name}-{HOSTNAME}.yml", f"{self.config_name}.yml"]:
                    conf = self._conf_from(config_file_name)
                    if conf is not None:
                        return conf

            return None
        except ModuleNotFoundError:
            print("[config] expecting a 'conf' module in current directory")
        except Exception as e:
            print(f"[config] {e}")
            traceback.print_exc()

    def _conf_from(self, config_file):
        if config_file[0] in ('/', '~', '$'):
            config_file = paths.expand(config_file)
        else:
            config_file = os.path.join(self.config_dir, config_file)

        if not os.path.exists(config_file):
            print(f"[config] from: {config_file}, not exist")
            return None
        with open(config_file, 'rb') as stream:
            try:
                conf = yaml.safe_load(stream)
                print(f"[config] from: {config_file}, loaded")
                return conf or {}
            except yaml.YAMLError as e:
                print(f"[config] failed to load from: {config_file}, due to: {e}")
                traceback.print_exc()
                return {}

    def _conf_from_module(self):
        local_config_file = os.path.join(self.config_dir, self.module, self.config_name)
        if os.path.exists(local_config_file):
            return self._conf_from(local_config_file)

        try:
            conf = yaml.safe_load(open_text(self.module, self.config_name))
            print(f"[config] from: {self.module}/{self.config_name}, loaded")
            return conf or {}
        except yaml.YAMLError as e:
            print(f"[config] failed to load from: {self.module}/{self.config_name}, due to: {e}")
            traceback.print_exc()
            return {}

    def get(self, name, default_value=None, ):
        if name is None:
            return default_value
        cfg = self.conf
        if cfg is None:
            return default_value
        for _key in name.split('.'):
            if isinstance(cfg, str):
                return default_value
            cfg = cfg.get(_key)
            if cfg is None:
                return default_value
        return cfg

    def get_path(self, name, default_value=None):
        if name is None:
            return default_value
        cfg = self.conf
        if cfg is None:
            return paths.get_path(default_value) if default_value else None
        for _key in name.split('.'):
            if isinstance(cfg, str):
                return paths.get_path(default_value) if default_value else None
            cfg = cfg.get(_key)
            if cfg is None:
                return default_value
        return paths.get_path(cfg)


def is_production():
    return ENV == 'prod'


def is_not_production():
    return not is_production()


def get_config_dir():
    config_dir = os.environ.get("CONFIG_DIR")
    if config_dir is not None:
        if not os.path.exists(config_dir):
            print(f'[config] CONFIG_DIR: {config_dir} DOES NOT EXIST, trying from default path')
        else:
            return config_dir

    root_path = paths.project_root_path()
    if root_path is None:
        root_path = os.getcwd()
    program_path = os.environ.get('_')
    if program_path and '/usr/bin/env' != program_path:
        os.environ['program_name'] = os.path.basename(program_path)
    return os.path.join(root_path, 'conf')


def config_from(config_file_name, module: Optional[str] = None):
    return get_config(config_file_name, module)


def get_config(config: Optional[Union[str, Config]] = None, module: Optional[str] = None):
    if config is None:
        cfg = Config()
    elif isinstance(config, str):
        cfg = Config(config, module)
    elif isinstance(config, Config):
        cfg = config
    else:
        raise ValueError(f'Unsupported cfg type: {type(config)}')
    return cfg


def get(name,
        default_value=None,
        config: Optional[Union[str, Config]] = None,
        module: Optional[str] = None) -> Optional[Union[str, int, float, list, map, dict]]:
    return get_config(config, module).get(name, default_value)


def get_path(name,
             default_value=None,
             config: Optional[Union[str, Config]] = None,
             module: Optional[str] = None):
    return get_config(config, module).get_path(name, default_value)
