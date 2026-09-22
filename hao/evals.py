import re
from typing import Any

from RestrictedPython import Eval, Guards, compile_restricted, limited_builtins, safe_builtins, utility_builtins


def safe_float(value):
    if value == "" or value is None:
        return 0.0
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0


SAFE_GLOBALS: dict[str, Any] = {
    '__builtins__': {**safe_builtins, **limited_builtins, **utility_builtins},
    '_getitem_': Eval.default_guarded_getitem,
    'getattr': Guards.safer_getattr,
    'setattr': Guards.guarded_setattr,
    'delattr': Guards.guarded_delattr,
    '_getiter_': Eval.default_guarded_getiter,
    '_iter_unpack_sequence_': Guards.guarded_iter_unpack_sequence,
    "enumerate": enumerate,
    "dict": dict,
    "list": list,
    "range": range,
    "min": min,
    "max": max,
    "all": all,
    "any": any,
    "sum": sum,
    "len": len,
    "re": re,
    "float": safe_float,
}


def eval(code: str, vars: dict = None):
    byte_code = compile_restricted(
        code,
        filename='<inline code>',
        mode='eval'
    )
    globals = {"__builtins__": SAFE_GLOBALS}
    if vars is not None:
        globals.update(vars)
    return eval(byte_code, globals)
