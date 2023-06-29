# -*- coding: utf-8 -*-
import typing

import regex

P_CHINESE = regex.compile(u'[\u4e00-\u9fa5]+')
P_HTML_TAGS = regex.compile('<[^>]*>')
P_NON_CHAR = regex.compile('[^a-zA-Z0-9]')


def re_compile(items, prefix='', suffix='', flags=regex.I):
    if items is None or len(items) == 0:
        return None
    pattern = join_items(items, prefix, suffix)
    return regex.compile(pattern, flags)


def join_items(items, prefix='', suffix=''):
    return r'|'.join([f'(?:{prefix}{_item.strip()}{suffix})' for _item in items if len(_item) > 0])


def split_with_sep(text: str, p: typing.Pattern, sep_as_ending: bool = True) -> typing.Generator[str, None, None]:
    if text is None or p is None:
        return None
    i = 0
    cache = ''
    for m in p.finditer(text):
        start, end = m.span()
        if sep_as_ending:
            item = (text[i:start] + m.group()).strip()
        else:
            item = (cache + text[i:start]).strip()
            cache = m.group()

        if len(item) == 0:
            continue
        yield item
        i = end
    if sep_as_ending:
        item = text[i:].strip()
    else:
        item = (cache + text[i:]).strip()
    if len(item) > 0:
        yield item


def remove_html_tags(text: str):
    return P_HTML_TAGS.sub('', text) if text else None


def remove_non_char(text: str):
    return P_NON_CHAR.sub('', text) if text else None
