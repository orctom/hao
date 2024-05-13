# -*- coding: utf-8 -*-
import argparse
import copy
import sys
from distutils.util import strtobool
from pprint import pformat
from typing import Callable, Optional, Union

from . import args, envs
from .config import Config, get_config

_CACHE = {}


class Attr(object):
    def __init__(self, type=None, default=None, required=False, env=None, key=None, help=None, secret=False, **kwargs):
        super().__init__()
        self.type = type or str
        self.default = default
        self.required = required
        self.env = env
        self.key = key
        self.help = help
        self.secret = secret
        self.kwargs = kwargs

    def __call__(self, *args, **kwargs):
        pass


attr = Attr


def from_args(_cls=None,
              prefix=None,
              adds=None,
              config: Optional[Union[str, Config]] = None,
              module: Optional[str] = None,
              key: Optional[str] = None,
              loader: Optional[Callable] = None,
              cache: bool = False):
    """
    resolves args from: command line / constructor / env / config / loader / defaults (by order).

    :param _cls: do not pass in this param
    :param prefix: optional prefix for command line: {prefix}_attribute
    :param adds: optional one or more function to add more args to ArgumentParser()<br/>
        e.g. <pre> @from_args(adds=pytorch_lightning.Trainer.add_argparse_args) </pre>
    :param config: optional yaml config filename (path not included)
    :param module: optional module name, will try to load from conf/{{module}}/config.yml or site-packages/{{module}}/config.yml
    :param key: optional key name in config file, if config is specified
    :param loader: optional function, which should return a dict populated with values
    :param cache: save to cache forever if True
    :return: the object with value populated
    """

    def __init__(self, *a, **kw):
        def from_loader():
            if loader is None:
                return None
            return loader()

        assert len(a) == 0 or _cls is None, '@from_args() not allowed arg: "_cls"'

        cfg = get_config(config, module)
        if cfg and key:
            cfg = cfg.get(key, {})
        parser = args.add_argument_group(self.__class__.__name__)

        # add any action/parser defaults that aren't present
        fields_adds = {
            **{
                action.dest: action.default for action in getattr(parser, '_actions')
                if not hasattr(self, action.dest) and action.dest != argparse.SUPPRESS and action.default != argparse.SUPPRESS
            },
            **{
                _attr: _default for _attr, _default in getattr(parser, '_defaults').items()
                if not hasattr(self, _attr)
            }
        }
        # fields_adds = {**parser._defaults}
        for action in getattr(parser, '_actions'):
            if not hasattr(self, action.dest) and action.dest != argparse.SUPPRESS and action.default != argparse.SUPPRESS:
                if hasattr(action, 'default'):
                    action.default = None
        getattr(parser, '_defaults').clear()

        fields, attrs = {}, {}
        for cls in reversed(self.__class__.__mro__):
            fields.update({
                k: v for k, v in cls.__dict__.items()
                if not k.startswith('__') and not k.endswith('__') and not isinstance(v, (Attr, property)) and not callable(v)
            })
            attrs.update({
                k: v for k, v in cls.__dict__.items()
                if not k.startswith('__') and not k.endswith('__') and isinstance(v, Attr)
            })

        for _name, _attr in attrs.items():
            arg_name = f'--{self._get_arg_name(_name)}'

            if _attr.type is list or isinstance(_attr.type, list):
                _attr.kwargs['nargs'] = '*'
                _attr.type = str
            if hasattr(_attr.type, '__args__') and getattr(_attr.type, '__origin__') in (list, set, tuple):
                _attr.kwargs['nargs'] = '*'
                _attr.type = getattr(_attr.type, '__args__')[0]

            desc = ' '.join(filter(None, [
                '[required]' if _attr.required else '[optional]',
                _attr.help,
                f"(default: {_attr.default})" if _attr.default is not None else None
            ]))
            if 'action' in _attr.kwargs:
                parser.add_argument(arg_name, help=desc, **_attr.kwargs)
            elif _attr.type == bool:
                parser.add_argument(arg_name, type=lambda x: bool(strtobool(x)), help=desc, **_attr.kwargs)
            else:
                attr_type = _attr.type
                parser.add_argument(arg_name, type=attr_type, help=desc, **_attr.kwargs)

        ns, _ = args.parse_known_args()
        loaded_values = from_loader()
        values = {}

        # static fields
        for _name, _value in fields.items():
            setattr(self, _name, _value)
            values[_name] = _value

        # constructor
        for _name, _value in kw.items():
            _val = _value or values[_name]
            setattr(self, _name, _val)
            values[_name] = _val

        # attrs
        messages = []
        for _name, _attr in attrs.items():
            arg_name = self._get_arg_name(_name)
            _value = getattr(ns, arg_name, None)  # namespace
            if _value is None and _name in kw:
                _value = kw.get(_name)
            if _value is None and _attr.env:
                _value = envs.get_of_type(_attr.env, _attr.type)
            if _value is None:  # 0 or 2 -> 2
                if _attr.key:
                    _value = cfg.get(_attr.key.format(**values), _attr.default)
                elif key:
                    _value = cfg.get(_name, _attr.default)
                elif loaded_values:
                    _value = loaded_values.get(_name, _attr.default)
                else:
                    _value = _attr.default
            if _value is None and _attr.required:
                messages.append(f'MISSING: --{self._get_arg_name(_name)}')
                continue

            try:
                if _value is not None and isinstance(_value, str):
                    _value = _value.format(**values)
            except KeyError:
                pass
            setattr(self, _name, _value)
            values[_name] = _value

        # adds
        _adds = args.add_by_function(adds)
        for _name, _default in fields_adds.items():
            if _name in values:
                continue
            _attr = attrs.get(_name) or _adds.get(_name)
            if _attr is None:
                continue
            arg_name = self._get_arg_name(_name)
            _value = getattr(ns, arg_name, None)  # namespace
            if _value is None:  # constructor
                _value = kw.get(_name)
            if _value is None and _attr.env:  # env / default
                _value = envs.get_of_type(_attr.env, _attr.type) or _default
            setattr(self, _name, _value)
            values[_name] = _value

        del values

        for k, v in list(vars(self).items()):
            if type(v) == classmethod:
                delattr(self, k)

        if len(messages) > 0:
            args.print_help()
            for msg in messages:
                print(msg)
            sys.exit(0)

        post_init_fn = getattr(self, '__post__init__', None)
        if post_init_fn:
            post_init_fn()

        if cache:
            _CACHE[fqdn(self)] = self

    def _get_arg_name(self, _name):
        return f'{prefix}_{_name}' if prefix else _name

    def _secret_fields(self):
        fields = set()
        for cls in reversed(self.__class__.__mro__):
            fields.update([
                k for k, v in cls.__dict__.items()
                if isinstance(v, Attr) and v.secret is True
            ])
        return fields

    def prettify(self, align='<', fill=' ', width=125):
        def fmt_kv(_k, _v):
            key = f"{_k:{fill}{align}{indent}}"
            if _k in secret_fields and _v:
                return f"{key}: ********"
            if isinstance(_v, dict):
                formatted = pformat(_v, compact=True, width=width, sort_dicts=False)
                val = f"\n\t{' ' * (indent + 2)}".join([line for line in formatted.splitlines()])
                return f"{key}: {val}"
            return f"{key}: {_v}"

        attributes = self.to_dict()
        secret_fields = self._secret_fields()
        headline = f"[{self.__class__.__name__}] (prefix='{self.__PREFIX__}')" if self.__PREFIX__ else f"[{self.__class__.__name__}]"
        if len(attributes) == 0:
            return f"{headline}\t[-]"
        indent = max([len(k) for k, _ in attributes.items()]) + 1
        values = '\n\t'.join([fmt_kv(k, v) for k, v in attributes.items()])
        if len(attributes) <= 1:
            return f"{headline}\t{values}"
        else:
            return f"{headline}\n\t{values}"

    def __repr__(self) -> str:
        return self.prettify()

    def __str__(self) -> str:
        return self.__repr__()

    def to_dict(self):
        return {
            k: v for k, v in vars(self).items()
            if not k.startswith('_') and type(v) != classmethod
        }

    def from_dict(cls, data: dict):
        def populate(_o, _d):
            for k, v in _d.items():
                if isinstance(v, dict):
                    setattr(_o, k, populate(type(k, (), {})(), v))
                else:
                    setattr(_o, k, v)
            return _o

        def convert(_d):
            if isinstance(_d, dict):
                _o = type('', (), {})()
                for k, v in _d.items():
                    setattr(_o, k, convert(v))
                return _o
            return _d

        values = {k: convert(v) for k, v in data.items()}
        return cls(**values)

    def _method(cls, method):
        try:
            method.__module__ = cls.__module__
        except AttributeError:
            pass

        try:
            method.__qualname__ = ".".join((cls.__qualname__, method.__name__))
        except AttributeError:
            pass

        return method

    def wrapper(cls):
        if getattr(cls, "__class__", None) is None:
            raise TypeError("Only works with new-style classes.")
        setattr(cls, '__PREFIX__', prefix)
        for method in [_get_arg_name, _secret_fields, __init__, to_dict, __repr__, __str__, prettify]:
            setattr(cls, method.__name__, _method(cls, method))
        for method in [from_dict]:
            setattr(cls, method.__name__, classmethod(method))
        return cls

    if _cls is None:
        return wrapper
    else:
        return wrapper(_cls)


def get_cached() -> dict:
    return copy.copy(_CACHE)


def fqdn(obj: object) -> str:
    module = obj.__class__.__module__
    name = obj.__class__.__qualname__
    return name if module in (None, str.__class__.__module__) else f"{module}.{name}"
