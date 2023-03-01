# -*- coding: utf-8 -*-

class OptionsMixin:
    def __new__(cls, label, value):
        obj = object.__new__(cls)
        obj.label = label
        obj._value_ = value
        return obj

    @classmethod
    def _missing_(cls, value):
        if not isinstance(value, str):
            return None
        if value.isdecimal():
            return cls(int(value))
        for _, e in enumerate(cls):
            if e.label == value or e.value == value:
                return e

    def __str__(self) -> str:
        return str(self.value)

    @classmethod
    def options(cls):
        return [(e.label, e.value) for _, e in enumerate(cls)]
