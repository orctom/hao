# -*- coding: utf-8 -*-
import argparse

from . import config, logs, strings

LOGGER = logs.get_logger(__name__)


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


def from_args(_cls=None, prefix=None, adds=None):
    """
    populate args from: command line / constructor / config / defaults.
    also supports arg resolving according attributes declared upper
    :param _cls: do not pass in this param
    :param prefix: optional prefix for command line: {prefix}_attribute
    :param adds: optional one or more function to add more args to ArgumentParser()
    :return: the object with value populated
    """

    def __init__(self, *args, **kwargs):
        if len(args) > 0 and _cls is not None:
            raise ValueError('@from_args() not allowed arg: "_cls"')
        parser = argparse.ArgumentParser(conflict_handler='resolve')

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
            LOGGER.debug(f"[from_args] add argument: {arg_name}")

            if _attr.type is list or isinstance(_attr.type, list):
                _attr.kwargs['nargs'] = '*'
                _attr.type = str

            if 'action' in _attr.kwargs:
                parser.add_argument(arg_name, help=_attr.help, **_attr.kwargs)
            else:
                attr_type = _attr.type if _attr.type != bool else lambda x: strings.boolean(x)
                parser.add_argument(arg_name, type=attr_type, help=_attr.help, **_attr.kwargs)

        args, _ = parser.parse_known_args()
        values = {}
        for _name, _value in kwargs.items():
            setattr(self, _name, _value)
        for _name, _default in fields_adds.items():
            _value = getattr(args, self._get_arg_name(_name))
            if _value is None:
                _value = kwargs.get(_name)
            if _value is None:
                _value = _default
            setattr(self, _name, _value)
            values[_name] = _value

        for _name, _value in fields.items():
            setattr(self, _name, _value)
            values[_name] = _value

        for _name, _attr in attrs.items():
            _value = getattr(args, self._get_arg_name(_name), None)
            if _value is None and _name in kwargs:
                _value = kwargs.get(_name)
            if _value is None:  # 0 or 2 -> 2
                if _attr.key:
                    _value = kwargs.get(_name, config.get(_attr.key.format(**values), _attr.default))
                else:
                    _value = _attr.default
            if _value is None and _attr.required:
                parser.print_usage()
                raise ValueError(f'missing arg: --{self._get_arg_name(_name)}')

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

    def prettify(self, align='>', width=38, fill=' '):
        values = '\n\t'.join([
            "{k:{fill}{align}{width}}: {v}".format(k=k, fill=fill, align=align, width=width, v=v)
            for k, v in self.as_dict().items()
        ])
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
            print('populating', _o, 'x with x', _d)
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
