# -*- coding: utf-8 -*-
import regex

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


def _clear(raw_str):
    if not isinstance(raw_str, str):
        raw_str = str(raw_str)

    # 1. 移除“人民币”前缀
    cleaned = regex.sub(r'^人民币', '', raw_str)

    # 2. 移除整个括号及其内容（如 （¥17,000.00） 或 ($123)）
    cleaned = regex.sub(r'[（(][^）)]*[）)]', '', cleaned)

    # 3. 移除币种符号（使用明确的替换，绝对不用 [] 字符集，防止误删数字）
    for symbol in ['¥', '￥', '$', '€', '£']:
        cleaned = cleaned.replace(symbol, '')

    # 4. 移除所有的空格
    cleaned = regex.sub(r'\s+', '', cleaned)

    # 5. 处理异常的小数点：保留第一个小数点，将后续的小数点视为千分位逗号或直接移除
    parts = cleaned.split('.')
    if len(parts) > 2:
        cleaned = parts[0] + ''.join(parts[1:-1]) + '.' + parts[-1]

    return cleaned


def parse_chinese_amount(cn_str):
    """
    第二步：解析中文金额（支持大小写混合）
    """
    if not cn_str:
        return 0.0

    # 分离整数部分和小数部分
    if '点' in cn_str:
        int_part, dec_part = cn_str.split('点', 1)
    else:
        int_part, dec_part = cn_str, ""

    # 处理小数部分
    decimal_val = 0.0
    if dec_part:
        dec_str = ""
        for ch in dec_part:
            if ch.isdigit():
                dec_str += ch
            elif ch in CN_NUM:
                dec_str += str(CN_NUM[ch])
        if dec_str:
            decimal_val = float("0." + dec_str)

    # 处理整数部分
    total = 0
    current_section = 0  # 当前万/亿以内的累加值
    current_num = 0      # 当前正在读取的数字

    i = 0
    while i < len(int_part):
        ch = int_part[i]

        if ch.isdigit():
            current_num = current_num * 10 + int(ch)
        elif ch in CN_NUM:
            current_num = current_num * 10 + CN_NUM[ch]
        elif ch in CN_UNIT:
            unit = CN_UNIT[ch]
            if unit >= 10000:  # 万、亿
                if current_num == 0 and current_section == 0:
                    current_section = 1
                else:
                    current_section += current_num
                total += current_section * unit
                current_section = 0
                current_num = 0
            else:  # 十、百、千
                if current_num == 0:
                    current_num = 1
                current_section += current_num * unit
                current_num = 0
        # 遇到无法识别的字符（如“元”、“整”、“圆”），直接跳过
        i += 1

    total += current_section + current_num
    return total + decimal_val


def parse_amount(raw_str) -> float:
    # 1. 基础清洗
    cleaned = _clear(raw_str)

    # 2. 移除中文单位后缀并提取乘数（注意顺序：长的后缀优先匹配）
    multiplier = 1.0
    suffix_pattern = r'(亿元整|亿元|万元整|万元|万圆整|万圆|亿|万|元整|圆整|元|圆|整)$'
    match = regex.search(suffix_pattern, cleaned)

    if match:
        suffix = match.group(1)
        cleaned = cleaned[:match.start()]

        if '亿' in suffix:
            multiplier = 100000000.0
        elif '万' in suffix:
            multiplier = 10000.0

    # 3. 判断是否包含中文数字或单位
    has_chinese = bool(regex.search(r'[零壹贰叁肆伍陆柒捌玖拾百千万亿圆元整点一二三四五六七八九十两]', cleaned))

    # 4. 分支处理
    if has_chinese:
        value = parse_chinese_amount(cleaned)
    else:
        cleaned = cleaned.replace(',', '')
        try:
            value = float(cleaned) if cleaned else 0.0
        except ValueError:
            value = 0.0

    # 5. 应用后缀乘数
    final_value = value * multiplier
    return final_value
