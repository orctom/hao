# -*- coding: utf-8 -*-
import datetime
import json

from . import logs

LOGGER = logs.get_logger(__name__)


def json_default(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()
    if hasattr(o, '__dict__'):
        return getattr(o, '__dict__')
    try:
        from bson import ObjectId
        if isinstance(o, ObjectId):
            return str(o)
    except ImportError:
        pass


def dumps(data):
    return json.dumps(data, ensure_ascii=False, default=json_default)


def prettify(data, sort_keys=False, indent=2, separators=(',', ': ')):
    data_type = type(data)
    if data_type == dict:
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
            LOGGER.error(f'not supported type: {data_type}: {data}')
            return None


def remove_empties(d):
    return {k: v for k, v in d.iteritems() if v is not None and len(v) > 0}
