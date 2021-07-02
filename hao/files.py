# -*- coding: utf-8 -*-
import base64
import hashlib
import mmap
import os
import typing

import regex


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


def md5(file_name, hexical: bool = True, block_size=64 * 1024):
    if not os.path.exists(file_name):
        return None
    with open(file_name, 'rb') as f:
        digest = hashlib.md5()
        while True:
            data = f.read(block_size)
            if not data:
                break
            digest.update(data)

    if hexical:
        return digest.hexdigest()
    else:
        return base64.b64encode(digest.digest()).decode()
