# -*- coding: utf-8 -*-

def get(data: dict, attribute, *fallback_attributes, default_value=None):
    if data is None:
        return default_value
    value = data.get(attribute)
    if value is not None:
        return value
    for fallback in fallback_attributes:
        value = data.get(fallback)
        if value is not None:
            return value
    return default_value


def remove_fields(data: dict, fields: list, copy=False):
    if data is None or fields is None:
        return None

    if isinstance(data, (list, set, tuple)):
        return [remove_fields(item, fields, copy) for item in data]

    if isinstance(data, dict):
        return {k: remove_fields(v, fields, copy) for k, v in data.items() if k not in fields}

    return data


def remove_empty_fields(data):
    try:
        if data in [None, '', [], {}, ()]:
            return None

        if isinstance(data, (list, set, tuple)):
            return list(map(remove_empty_fields, data))

        if isinstance(data, dict):
            new_data = {}
            for k, v in data.items():
                if v in [None, '', [], {}, ()]:
                    continue
                new_data[k] = remove_empty_fields(v)
            return new_data

        return data
    except RecursionError:
        return None
