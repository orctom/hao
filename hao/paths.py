# -*- coding: utf-8 -*-
import contextlib
import inspect
import os
import sys
import types
import typing
from glob import glob
from pathlib import Path

import regex

FILES_IN_ROOT = ('pyproject.toml', 'requirements.txt', 'setup.py', 'LICENSE', '.git', '.idea', '.vscode', '.venv', 'venv')
_ROOT_PATH = None


def expand(path):
    if path is None:
        return None
    return os.path.expanduser(os.path.expandvars(path))


def package_root(module: types.ModuleType):
    root, _ = os.path.split(os.path.abspath(module.__file__))
    return root


def project_root_path():
    global _ROOT_PATH
    if _ROOT_PATH is not None:
        return _ROOT_PATH
    path = Path(os.getcwd())
    stops = ('/', home_path())
    while True:
        if str(path) == str(path.parent).replace('-', '_'):
            _ROOT_PATH = str(path.parent)
            break

        if any(path.joinpath(f).exists() for f in FILES_IN_ROOT):
            _ROOT_PATH = str(path)
            break

        path = path.parent

        if str(path) in stops:
            _ROOT_PATH = os.getcwd()
            break
    return _ROOT_PATH


def root_path():
    return project_root_path()


def home_path():
    return str(Path.home())


def whoami():
    caller = inspect.currentframe().f_back
    return caller.f_globals['__name__']


def who_called_me():
    stack = inspect.stack()
    frame_info = stack[2]
    filename = frame_info.filename
    filename = os.path.relpath(filename, root_path())
    name, ext = os.path.splitext(filename)
    name = regex.sub(r'^/', '', name)
    name = regex.sub(r'/', '.', name)
    return name


def function_called_me():
    stack = inspect.stack()
    frame_info = stack[2]
    return frame_info.function


def program_name():
    name = os.environ.get('program_name')
    if name is not None:
        return name
    main_module = sys.modules['__main__']
    if hasattr(main_module, '__file__'):
        return Path(main_module.__file__).stem
    return project_name()


def project_name():
    return Path(project_root_path() or os.getcwd()).stem


def make_parent_dirs(file_path, exist_ok=True):
    parent = Path(file_path).parent
    if parent.exists():
        return
    os.makedirs(parent, exist_ok=exist_ok)


def set_temp_dir(path: str = '~/.temp') -> bool:
    if path is None:
        return False
    path = path.strip()
    if len(path) == 0:
        return False
    if 'TMPDIR' in os.environ:
        return False
    path = expand(path)
    os.makedirs(path, exist_ok=True)
    os.environ['TMPDIR'] = path
    return True


def get(*paths, search_paths: typing.Optional[typing.Union[str, typing.List[str]]] = None):
    if paths is None or len(paths) == 0:
        return ''
    paths = list(filter(None, paths))
    if len(paths) == 0:
        return ''
    if paths[0][0] in ('/', '~', '$'):
        return expand(os.path.join(*paths))

    path_project = expand(os.path.join(project_root_path(), *paths))
    if search_paths:
        if isinstance(search_paths, list):
            for search_path in search_paths:
                path = os.path.join(get(search_path, *paths))
                if os.path.exists(path):
                    return path
            return path_project if os.path.exists(path_project) else None
        if isinstance(search_paths, str):
            path = os.path.join(get(search_paths, *paths))
            if os.path.exists(path):
                return path
            return path_project if os.path.exists(path_project) else None
    return path_project


def get_path(*paths):
    return get(*paths)


def delete(path):
    for filename in glob(path):
        with contextlib.suppress(FileNotFoundError, OSError):
            os.remove(filename)
