# -*- coding: utf-8 -*-
import mmap

import regex
import typing


P_BLANK_LINE = regex.compile(r'^(?:\s|\r|\n)*$')


def count_lines(file: str):
    with open(file, "r+") as f:
        buf = mmap.mmap(f.fileno(), 0)
        n_lines = 0
        while buf.readline():
            n_lines += 1
        return n_lines


def count_blank_lines(file):
    return count_matched_lines(file, P_BLANK_LINE)


def count_matched_lines(file: str, p: typing.Pattern):
    if isinstance(p, str):
        p = regex.compile(p)
    with open(file, "r+") as f:
        buf = mmap.mmap(f.fileno(), 0)
        n_lines = 0
        while True:
            line = buf.readline()
            if line is None or len(line) == 0:
                break
            if p.search(line.decode()) is not None:
                n_lines += 1
        return n_lines
