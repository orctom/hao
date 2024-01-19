# -*- coding: utf-8 -*-
import ctypes
import logging
import os
from typing import Optional

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


def is_in_qcloud():
    return os.path.exists('/usr/local/qcloud')


def is_gpu_available():
    try:
        ctypes.CDLL('libcuda.so')
        return True
    except OSError:
        return False


def cuda_version():
    try:
        cuda = ctypes.CDLL('libcuda.so')
        ver = ctypes.c_int()
        if cuda.cuDriverGetVersion(ctypes.byref(ver)) == 0:
            version = ver.value
            major = version // 1000
            minor = version % 1000 // 10
            return f"{major}.{minor}"
        return None
    except OSError:
        return None
    except Exception as e:
        LOGGER.exception(e)
        return None


def get_str(key: str, default: str = None) -> Optional[str]:
    return os.getenv(key) or default


def get_int(key: str, default: int = None) -> Optional[int]:
    return get_of_type(key, int, default)


def get_float(key: str, default: float = None) -> Optional[float]:
    return get_of_type(key, float, default)


def get_bool(key: str, default: str = None) -> Optional[bool]:
    return get_of_type(key, bool, default)


def get_complex(key: str, default: complex = None) -> Optional[complex]:
    return get_of_type(key, complex, default)


def get_of_type(key: str, of_type: type, default=None):
    value = os.getenv(key)
    if value is not None:
        try:
            return of_type(value)
        except ValueError as err:
            LOGGER.warning(str(err))
    return default
