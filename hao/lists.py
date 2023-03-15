# -*- coding: utf-8 -*-
import itertools

from . import dicts


def uniquify(items: list, min_size=0):
    def strip_item(item):
        if item is None:
            return None
        if isinstance(item, str):
            item = item.strip()
            if len(item) == 0:
                return None
        return item

    def is_qualified(item):
        if item is None:
            return False
        if isinstance(item, str) and len(item) < min_size:
            return False
        if item in seen:
            return False
        seen.add(item)
        return True

    if items is None:
        return None
    seen = set()
    return list(filter(None, [strip_item(x) for x in items if is_qualified(x)]))


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
        adding = [a for a in adding if a is not None]
        if len(adding) > 0:
            items.extend(adding)
    else:
        items.append(adding)
    return True


def add_tuple_to_list(target: list, adding: tuple):
    if not isinstance(adding, tuple):
        raise ValueError(f"expecting `adding` to be a tuple, actual: {type(adding)}")
    if len(target) == 0:
        target.extend([[] for _ in range(len(adding))])
    for items_target, items_adding in zip(target, adding):
        add_to_list(items_target, items_adding)


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
