# -*- coding: utf-8 -*-
import itertools

from . import dicts


def uniquify(sequence, min_size=0):
    if sequence is None:
        return None
    seen = set()
    return list(filter(
        None,
        [_strip_list_item(x) for x in sequence
         if x is not None and len(x) >= min_size and not (x in seen or seen.add(x))]
    ))


def _strip_list_item(item):
    if item is None:
        return None
    if callable(getattr(item, 'strip', None)):
        item = item.strip()
    if len(item) == 0:
        return None
    return item


def is_empty(items):
    if items is None or len(items) == 0:
        return True
    for item in items:
        if item and len(items) > 0:
            return False
    return True


def is_not_empty(items):
    return not is_empty(items)


def contains(items, items_checking):
    if is_empty(items) or is_empty(items_checking):
        return False
    return len(set(items).intersection(set(items_checking))) > 0


def cycle(iterable):
    saved = []
    for element in iterable:
        yield element
        saved.append(element)
    while saved:
        for element in saved:
            yield element


def batchify(iterable, n=1):
    size = len(iterable)
    for ndx in range(0, size, n):
        yield iterable[ndx:min(ndx + n, size)]


def sliding(iterable, window_size):
    iterators = itertools.tee(iterable, window_size)
    iterators = [itertools.islice(iterator, i, None) for i, iterator in enumerate(iterators)]
    yield from zip(*iterators)


def remove_from_list(target_list: list, removal):
    if target_list is None or len(target_list) == 0 or removal is None or len(removal) == 0:
        return target_list
    type_removal = type(removal)
    if type_removal in (set, list, tuple):
        removal_set = set(removal)
    else:
        removal_set = [removal]
    return list(filter(lambda x: x not in removal_set, target_list))


def remove_sub_items(items):
    items_copy = items.copy()
    items_copy.sort(key=len)
    removal_list = list()
    for i, item in enumerate(items_copy):
        for item_target in items_copy[i + 1:]:
            if item != item_target and item in item_target:
                removal_list.append(item)
    return remove_from_list(items, removal_list)


def add_to_list(items, adding):
    if adding is None:
        return False
    adding_type = type(adding)
    if adding_type in (list, tuple, set):
        adding = list(filter(None, adding))
        if len(adding) > 0:
            items.extend(adding)
    else:
        items.append(adding)
    return True


def remove_fields(items: list, fields: list, copy=False, remove_empty: bool = False, remove_private: bool = False) -> list:
    if items is None or fields is None or len(fields) == 0:
        return items or []
    results = list()
    for item in items:
        _item = dicts.remove_fields(item, fields, copy, remove_empty, remove_private)
        if _item is None:
            continue
        results.append(_item)
    return results


def padding(items: list, size, padding_item):
    n_items = size - len(items)
    if n_items <= 0:
        return
    items.extend([padding_item] * n_items)
