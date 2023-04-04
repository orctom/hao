# -*- coding: utf-8 -*-
import base64
import hashlib
import logging
import math
import mmap
import os
import sys
from typing import Optional, Pattern

import regex

LOGGER = logging.getLogger(__name__)

P_BLANK_LINE = regex.compile(r'^(?:\s|\r|\n)*$')


def count_lines(file: str):
    with open(file, "r+") as f:
        n_lines = 0
        try:
            buf = mmap.mmap(f.fileno(), 0)
            while buf.readline():
                n_lines += 1
        except ValueError:
            pass  # cannot mmap an empty file
        return n_lines


def count_blank_lines(file):
    return count_matched_lines(file, P_BLANK_LINE)


def count_matched_lines(file: str, p: Pattern):
    if isinstance(p, str):
        p = regex.compile(p)
    with open(file, "r+") as f:
        n_lines = 0
        try:
            buf = mmap.mmap(f.fileno(), 0)
            while True:
                line = buf.readline()
                if line is None or len(line) == 0:
                    break
                if p.search(line.decode()) is not None:
                    n_lines += 1
        except ValueError:
            pass  # cannot mmap an empty file
        return n_lines


def checksum(filename, algorithm='md5', hexical: bool = True, block_size=64 * 1024):
    if not os.path.exists(filename):
        return None
    with open(filename, 'rb') as f:
        digest = getattr(hashlib, algorithm)()
        while True:
            data = f.read(block_size)
            if not data:
                break
            digest.update(data)

    if hexical:
        return digest.hexdigest()
    else:
        return base64.b64encode(digest.digest()).decode()


def md5(filename, hexical: bool = True, block_size=64 * 1024):
    return checksum(filename, algorithm='md5', hexical=hexical, block_size=block_size)


def sha1(filename, hexical: bool = True, block_size=64 * 1024):
    return checksum(filename, algorithm='sha1', hexical=hexical, block_size=block_size)


def sha256(filename, hexical: bool = True, block_size=64 * 1024):
    return checksum(filename, algorithm='sha256', hexical=hexical, block_size=block_size)


def split(filename, n_split: int, split_prefix: str = '-split-'):
    assert os.path.exists(filename), f"File not exist: {filename}"
    assert n_split > 1, 'n_split should be larger than 1'

    n_lines = count_lines(filename)
    lines_per_split = math.ceil(n_lines / n_split)
    prefix, suffix = os.path.splitext(filename)
    i = 0
    with open(filename) as f_in:
        f_out = open(f"{prefix}{split_prefix}{i}{suffix}", 'w')
        n_lines = 0
        for line in f_in:
            f_out.write(line)
            n_lines += 1
            if n_lines >= lines_per_split:
                f_out.close()
                i += 1
                f_out = open(f"{prefix}{split_prefix}{i}{suffix}", 'w')
        f_out.close()


def split_cli():
    args = sys.argv
    if len(args) not in (3, 4):
        LOGGER.warn('Usage: h-split path/to/file {n_split} [optional_split_prefix]')
        return
    filename = args[1]
    n_split = int(args[2])
    if len(args) == 4:
        split_prefix = args[3]
        split(filename, n_split, split_prefix)
    else:
        split(filename, n_split)
