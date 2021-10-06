# -*- coding: utf-8 -*-
import functools


def get(data: dict, attribute, *fallback_attributes, default_value=None):
    if data is None:
        return default_value
    try:
        value = functools.reduce(getattr, attribute.split('.'), data)
        if value is not None:
            return value
    except AttributeError:
        pass
    if fallback_attributes:
        for fallback in fallback_attributes:
            value = data.get(fallback)
            if value is not None:
                return value
    return default_value


def remove_fields(data: dict, fields: list = None, copy=False, remove_empty: bool = False, remove_private: bool = False):
    try:
        if data is None:
            return None

        if remove_empty and data in [None, '', [], {}, ()]:
            return None

        if isinstance(data, (list, set, tuple)):
            return [remove_fields(item, fields, copy, remove_empty, remove_private) for item in data]

        if isinstance(data, dict):
            new_data = {}
            for k, v in data.items():
                if fields and k in fields:
                    continue
                if remove_empty and v in [None, '', [], {}, ()]:
                    continue
                if remove_private and k.startswith('_'):
                    continue
                new_data[k] = remove_fields(v, fields, copy, remove_empty, remove_private)
            return new_data

        return data
    except RecursionError:
        return None
