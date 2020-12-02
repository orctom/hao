# -*- coding: utf-8 -*-
import logging
import os
import typing

LOGGER = logging.getLogger(__name__)


def is_in_docker():

    def has_docker_env():
        return os.path.exists('/.dockerenv')

    def has_docker_cgroup():
        path = '/proc/self/cgroup'
        if not os.path.exists(path):
            return False
        with open(path, 'r') as f:
            for line in f:
                if 'docker' in line:
                    return True
        return False

    return has_docker_env() or has_docker_cgroup()


def is_in_aliyun():
    return os.path.exists('/usr/sbin/aliyun-service')


def get_str(key: str, default_value: str = None) -> typing.Optional[str]:
    return os.getenv(key) or default_value


def get_int(key: str, default_value: int = None) -> typing.Optional[int]:
    return get_of_type(key, int, default_value)


def get_float(key: str, default_value: float = None) -> typing.Optional[float]:
    return get_of_type(key, float, default_value)


def get_bool(key: str, default_value: str = None) -> typing.Optional[bool]:
    return get_of_type(key, bool, default_value)


def get_complex(key: str, default_value: complex = None) -> typing.Optional[complex]:
    return get_of_type(key, complex, default_value)


def get_of_type(key: str, of_type: type, default_value):
    value = os.getenv(key)
    if value is not None:
        try:
            return of_type(value)
        except ValueError as err:
            LOGGER.warning(str(err))
    return default_value
