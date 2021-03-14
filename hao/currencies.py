# -*- coding: utf-8 -*-

CN_NUM = {
    '〇': 0, '一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9,
    '零': 0, '壹': 1, '贰': 2, '叁': 3, '肆': 4, '伍': 5, '陆': 6, '柒': 7, '捌': 8, '玖': 9,
    '貮': 2, '两': 2, '俩': 2, '幺': 1,
}

CN_UNIT = {
    '分': 0.01,
    '角': 0.1,
    '毛': 0.1,
    '块': 1,
    '元': 1,
    '十': 10,
    '拾': 10,
    '百': 100,
    '佰': 100,
    '千': 1000,
    '仟': 1000,
    '万': 10000,
    '萬': 10000,
    '亿': 100000000,
    '億': 100000000,
    '兆': 1000000000000,
}


def chinese_to_digits(text: str) -> int:
    unit = 0   # current
    digits = []  # digest
    n_skips = 0
    for c in reversed(text):
        if c in CN_UNIT:
            unit = CN_UNIT.get(c)
            if unit in [10000, 100000000]:
                digits.append(unit)
                unit = 1
        else:
            dig = CN_NUM.get(c)
            if dig is None:
                n_skips += 1
                if n_skips >= 3:
                    break
                continue
            if unit:
                dig *= unit
                unit = 0
            digits.append(dig)
    if unit == 10:
        digits.append(10)
    val, tmp = 0, 0
    for x in reversed(digits):
        if x is None:
            continue
        if x in [10000, 100000000]:
            val += tmp * x
            tmp = 0
        else:
            tmp += x
    val += tmp
    return val
