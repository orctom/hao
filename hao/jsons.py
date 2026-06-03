# -*- coding: utf-8 -*-
import datetime
import json
import logging
from enum import Enum

from . import regexes

LOGGER = logging.getLogger(__name__)
P_JSON_NORM = regexes.re_compile(['json', '```', '\\n', '\n'])


try:
    import demjson3
except ImportError:
    demjson3 = None

try:
    import pyjson5
except ImportError:
    pyjson5 = None

try:
    import json_repair
except ImportError:
    json_repair = None


def json_default(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()
    if isinstance(o, Enum):
        return o.value
    if hasattr(o, '__dict__'):
        return getattr(o, '__dict__')
    if hasattr(o, '__str__'):
        return str(o)

    try:
        from bson import ObjectId
        if isinstance(o, ObjectId):
            return str(o)
    except ImportError:
        pass
    return o


def dumps(data):
    return json.dumps(data, ensure_ascii=False, default=json_default)


def prettify(data, sort_keys=False, indent=2, separators=(',', ': ')):
    if isinstance(data, dict):
        return json.dumps(
            data,
            sort_keys=sort_keys,
            indent=indent,
            separators=separators,
            ensure_ascii=False,
            default=json_default
        )
    elif hasattr(data, '__dict__'):
        return json.dumps(
            data.__dict__,
            sort_keys=sort_keys,
            indent=indent,
            separators=separators,
            ensure_ascii=False,
            default=json_default
        )
    else:
        try:
            return json.dumps(
                data,
                sort_keys=sort_keys,
                indent=indent,
                separators=separators,
                ensure_ascii=False,
                default=json_default
            )
        except Exception as err:
            LOGGER.error(err)
            LOGGER.error(f'not supported type: {type(data)}: {data}')
            return None


def remove_empties(d):
    return {k: v for k, v in d.iteritems() if v is not None and len(v) > 0}


def dump(data, path):
    with open(path, 'w') as f:
        json.dump(data, f, ensure_ascii=False, default=json_default)


def load(path):
    with open(path) as f:
        return json.load(f)


def loads(text: str) -> dict | list | str:
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError) as e:
        original_error = e

    cleaned_text = P_JSON_NORM.sub('', text)
    try:
        return json.loads(cleaned_text)
    except (json.JSONDecodeError, ValueError):
        pass

    fallback_parsers = [
        (demjson3, 'decode'),
        (pyjson5, 'decode'),
        (json_repair, 'loads'),
    ]

    for module, method_name in fallback_parsers:
        if not module:
            continue
        try:
            parser_method = getattr(module, method_name)
            return parser_method(cleaned_text)
        except Exception:
            continue

    raise original_error
