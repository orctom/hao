# -*- coding: utf-8 -*-
import argparse
import sys
from typing import Optional, Union

from . import args, envs, strings
from .config import Config, get_config


class Attr(object):
    def __init__(self, type=None, default=None, required=False, key=None, help=None, **kwargs):
        super().__init__()
        self.type = type or str
        self.default = default
        self.required = required
        self.key = key
        self.help = help
        self.kwargs = kwargs

    def __call__(self, *args, **kwargs):
        pass


attr = Attr


def from_args(_cls=None,
              prefix=None,
              adds=None,
              env: bool = True,
              config: Optional[Union[str, Config]] = None):
    """
    resolves args from: command line / constructor / env / config / defaults (by order).
    also supports arg resolving according attributes declared upper

    :param _cls: do not pass in this param
    :param prefix: optional prefix for command line: {prefix}_attribute
    :param adds: optional one or more function to add more args to ArgumentParser()<br/>
        e.g. <pre> @from_args(adds=pytorch_lightning.Trainer.add_argparse_args) </pre>
    :param env: will try to resolve args from environment properties if True
    :param config: None (default) / yml filename / Config
    :return: the object with value populated
    """

    def __init__(self, *a, **kw):
        if len(a) > 0 and _cls is not None:
            raise ValueError('@from_args() not allowed arg: "_cls"')
        cfg = get_config(config)
        parser = args.add_argument_group(self.__class__.__name__)

        if adds is not None:
            if isinstance(adds, list):
                for add in adds:
                    parser = add(parser)
            elif callable(adds):
                parser = adds(parser)

        # add any action/parser defaults that aren't present
        fields_adds = {
            **{action.dest: action.default for action in parser._actions
               if not hasattr(self, action.dest) and action.dest != argparse.SUPPRESS and action.default != argparse.SUPPRESS},
            **{_attr: _default for _attr, _default in parser._defaults.items()
               if not hasattr(self, _attr)}
        }
        # fields_adds = {**parser._defaults}
        for action in parser._actions:
            if not hasattr(self, action.dest) and action.dest != argparse.SUPPRESS and action.default != argparse.SUPPRESS:
                if hasattr(action, 'default'):
                    action.default = None
        parser._defaults.clear()

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
                f"(default: {_attr.default})" if _attr.default else None
            ]))
            if 'action' in _attr.kwargs:
                parser.add_argument(arg_name, help=desc, default=_attr.default, **_attr.kwargs)
            else:
                attr_type = _attr.type if _attr.type != bool else lambda x: strings.boolean(x)
                parser.add_argument(arg_name, type=attr_type, help=desc, default=_attr.default, **_attr.kwargs)

        ns, _ = args.parse_known_args()
        values = {}

        # constructor
        for _name, _value in kw.items():
            setattr(self, _name, _value)

        # adds
        for _name, _default in fields_adds.items():
            _attr = attrs.get(_name)
            if _attr is None:
                continue
            arg_name = self._get_arg_name(_name)
            _value = getattr(ns, arg_name)                   # namespace
            if _value is None:                                 # constructor
                _value = kw.get(_name)
            if _value is None:                                 # env / default
                _value = envs.get_of_type(arg_name, _attr.type) if env else _default
            setattr(self, _name, _value)
            values[_name] = _value

        # static fields
        for _name, _value in fields.items():
            setattr(self, _name, _value)
            values[_name] = _value

        # attrs
        for _name, _attr in attrs.items():
            arg_name = self._get_arg_name(_name)
            _value = getattr(ns, arg_name, None)  # namespace
            if _value is None and _name in kw:
                _value = kw.get(_name)
            if _value is None and env:
                _value = envs.get_of_type(arg_name, _attr.type)
            if _value is None:  # 0 or 2 -> 2
                if _attr.key:
                    _value = kw.get(_name, cfg.get(_attr.key.format(**values), _attr.default))
                else:
                    _value = _attr.default
            if _value is None and _attr.required:
                args.print_help()
                print(f'MISSING: --{self._get_arg_name(_name)}')
                sys.exit(0)

            try:
                if _value is not None and isinstance(_value, str):
                    _value = _value.format(**values)
            except KeyError:
                pass
            setattr(self, _name, _value)
            values[_name] = _value

        del values

        for k, v in list(vars(self).items()):
            if type(v) == classmethod:
                delattr(self, k)

    def _get_arg_name(self, _name):
        return f'{prefix}_{_name}' if prefix else _name

    def prettify(self, align='<', fill=' '):
        attributes = self.as_dict()
        width = max([len(k) for k, _ in attributes.items()]) + 1
        values = '\n\t'.join([f"{k:{fill}{align}{width}}: {v}" for k, v in attributes.items()])
        if len(attributes) <= 1:
            return f"[{self.__class__.__name__}]\t{values}"
        else:
            return f"[{self.__class__.__name__}]\n\t{values}"

    def __repr__(self) -> str:
        return self.prettify()

    def as_dict(self):
        return {
            k: v for k, v in vars(self).items()
            if not k.startswith('_') and type(v) != classmethod
        }

    def from_dict(cls, data: dict):
        def populate(_o, _d):
            for k, v in _d.items():
                if type(v) == dict:
                    setattr(_o, k, populate(type(k, (), {})(), v))
                else:
                    setattr(_o, k, v)
            return _o

        def convert(_d):
            if type(_d) == dict:
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
        for method in [_get_arg_name, __init__, as_dict, __repr__, prettify]:
            setattr(cls, method.__name__, _method(cls, method))
        for method in [from_dict]:
            setattr(cls, method.__name__, classmethod(method))
        return cls

    if _cls is None:
        return wrapper
    else:
        return wrapper(_cls)
