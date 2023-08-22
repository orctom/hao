# -*- coding: utf-8 -*-
import base64
import codecs
import hashlib
import json
import random as rand
import string
import unicodedata
from difflib import SequenceMatcher
from typing import List, Optional, Pattern

import regex

from . import jsons, lists

RE_BACKSPACES = regex.compile("\b+")
PUNCTUATION_ZH = '＂＃＄％＆＇（）＊＋，－／：；＜＝＞＠［＼］＾＿｀｛｜｝～｟｠｢｣､\u3000、〃〈〉《》「」『』【】〔〕〖〗〘〙〚〛〜〝〞〟〰〾〿–—‘’‛“”„‟…‧﹏﹑﹔·！？｡。'

RE_NORMALIZE = [
    (regex.compile(r'<200d>'), ''),
    (regex.compile(r'\u200d'), ''),
    (regex.compile(r'\ufeff'), ''),
    (regex.compile(r'&?nbsp;?'), ''),
    (regex.compile(r'\\t'), '  '),
    (regex.compile(r'\t'), '  '),
    (regex.compile(r'\\r'), '\n'),
    (regex.compile(r'\r'), '\n'),
]

P_EMOJI = regex.compile(
    "["
    u"\U0001F600-\U0001F64F"  # emoticons
    u"\U0001F300-\U0001F5FF"  # symbols & pictographs
    u"\U0001F680-\U0001F6FF"  # transport & map symbols
    u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
    u"\U00002702-\U000027B0"
    # u"\U000024C2-\U0001F251"
    u"\U0001f926-\U0001f937"
    u'\U00010000-\U0010ffff'
    u"\u200d"
    u"\u2640-\u2642"
    u"\u2600-\u2B55"
    u"\u23cf"
    u"\u23e9"
    u"\u231a"
    u"\u3030"
    u"\ufe0f"
    "]+",
    flags=regex.UNICODE
)


def normalize(text: str,
              controls: bool = True,
              specials: bool = True,
              emojis: bool = True,
              unicodes: bool = False,
              encoding: str = None):
    if text is None:
        return None

    if '{' in text and '}' in text:
        try:
            obj = json.loads(text)
            obj_normalized = normalize_obj(obj)
            return jsons.dumps(obj_normalized)
        except json.JSONDecodeError:
            pass

    text = text.strip()
    text = normalize_chars(text)

    try:
        val = json.loads(f'"{text}"')
        if isinstance(val, str):
            text = val
    except json.JSONDecodeError:
        pass

    if controls:
        text = remove_controls(text)
    if specials:
        text = remove_specials(text)
    if emojis:
        text = remove_emojis(text)
    if unicodes:
        text = remove_unicodes(text)
    if encoding:
        text = fix_encoding(text, encoding)
    return text


def trim(text: str):
    if text is None:
        return None
    return text.strip()


def normalize_obj(obj,
                  controls: bool = True,
                  specials: bool = True,
                  emojis: bool = True,
                  unicodes: bool = False,
                  encoding: str = None):
    if obj is None:
        return None
    if isinstance(obj, str):
        return normalize(obj, controls, specials, emojis, unicodes, encoding)
    elif isinstance(obj, list):
        return [normalize_obj(e) for e in obj]
    elif not isinstance(obj, dict):
        return obj
    return {k: normalize_obj(v) for k, v in obj.items()}


def normalize_chars(text) -> Optional[str]:
    if text is None:
        return None
    for p, sub in RE_NORMALIZE:
        text = p.sub(sub, text)
    return text


def remove_controls(text: str, trn: bool = False) -> Optional[str]:
    if text is None:
        return None
    return ''.join([ch for ch in text if not is_char_control(ch, trn=trn)])


def remove_specials(text: str) -> Optional[str]:
    if text is None:
        return None
    return unicodedata.normalize('NFD', text)


def remove_emojis(text: str) -> Optional[str]:
    if text is None:
        return None
    return P_EMOJI.sub('', text)


def remove_unicodes(text: str) -> Optional[str]:
    if text is None:
        return None
    try:
        text = codecs.decode(codecs.encode(text, 'latin-1', 'backslashreplace'), 'unicode-escape')
        text = remove_controls(text)
    except UnicodeDecodeError:
        pass
    return text


def fix_encoding(text: str, encoding: str) -> Optional[str]:
    if text is None:
        return None
    try:
        text = text.encode(encoding, 'ignore').decode(encoding, 'ignore')
    except LookupError:
        pass
    return text


def strip_to_empty(text: str):
    if text is None:
        return ''
    text = str(text)
    text = text.strip()
    text = RE_BACKSPACES.sub('', text)
    if text == 'None':
        return ''
    return text


def strip_to_none(text: str):
    if text is None:
        return None
    text = str(text)
    text = text.strip()
    text = RE_BACKSPACES.sub('', text)
    if len(text) == 0:
        return None
    if text in ('None', 'none', 'NONE'):
        return None
    return text


def boolean(value, default=False):
    if value is None:
        return default
    value_type = type(value)
    if value_type == bool:
        return value
    if value_type == bytes:
        value = value.decode()
    value = strip_to_none(value)
    if value is None:
        return default
    return value in ['true', 'True', '1', 't', 'T', 'y', 'Y', 'yes', 'YES', 'Yes']


def md5(text: str, hexical: bool = True):
    text = strip_to_none(text)
    if text is None:
        return None
    unique_key = text.encode('utf-8')
    if hexical:
        return hashlib.md5(unique_key).hexdigest()
    else:
        return base64.b64encode(hashlib.md5(unique_key).digest()).decode()


def sha1(text: str, hexical: bool = True):
    text = strip_to_none(text)
    if text is None:
        return None
    unique_key = text.encode('utf-8')
    if hexical:
        return hashlib.sha1(unique_key).hexdigest()
    else:
        return base64.b64encode(hashlib.sha1(unique_key).digest()).decode()


def sha256(text: str, hexical: bool = True):
    text = strip_to_none(text)
    if text is None:
        return None
    unique_key = text.encode('utf-8')
    if hexical:
        return hashlib.sha256(unique_key).hexdigest()
    else:
        return base64.b64encode(hashlib.sha256(unique_key).digest()).decode()


def sim(a, b):
    return SequenceMatcher(None, a, b).ratio()


def any_sim(items: List[str], item: str, threshold=0.75):
    if items is None or len(items) == 0 or item is None:
        return False
    for _item in items:
        if _item == item:
            return True
        if sim(_item, item) >= threshold:
            return True
    return False


def split_to_two(text, sep=None, default_value=None):
    return split_to_n(text, 2, sep=sep, default_value=default_value)


def split_to_three(text, sep=None, default_value=None):
    return split_to_n(text, 3, sep=sep, default_value=default_value)


def split_to_n(text, n, sep=None, default_value=None):
    if text is None:
        return [default_value] * n
    if sep is None:
        items = text.split()
    else:
        if isinstance(sep, Pattern):
            items = sep.split(text)
        else:
            items = regex.split(sep, text)
    items = [strip_to_none(item) or default_value for item in items]
    lists.padding(items, n, default_value)
    if len(items) == n:
        return items
    items = lists.uniquify(items)
    return [*items[:n-1], ', '.join(items[n-1:])]


def is_char_chinese(uchar):
    """
    :param uchar: input char in unicode
    :return: whether the input char is a Chinese character.
    """
    if u'\u3400' <= uchar <= u'\u4db5':  # CJK Unified Ideographs Extension A, release 3.0
        return True
    elif u'\u4e00' <= uchar <= u'\u9fa5':  # CJK Unified Ideographs, release 1.1
        return True
    elif u'\u9fa6' <= uchar <= u'\u9fbb':  # CJK Unified Ideographs, release 4.1
        return True
    elif u'\uf900' <= uchar <= u'\ufa2d':  # CJK Compatibility Ideographs, release 1.1
        return True
    elif u'\ufa30' <= uchar <= u'\ufa6a':  # CJK Compatibility Ideographs, release 3.2
        return True
    elif u'\ufa70' <= uchar <= u'\ufad9':  # CJK Compatibility Ideographs, release 4.1
        return True
    elif u'\u20000' <= uchar <= u'\u2a6d6':  # CJK Unified Ideographs Extension B, release 3.1
        return True
    elif u'\u2f800' <= uchar <= u'\u2fa1d':  # CJK Compatibility Supplement, release 3.1
        return True
    elif u'\uff00' <= uchar <= u'\uffef':  # Full width ASCII, full width of English punctuation, half width Katakana, half wide half width kana, Korean alphabet
        return True
    elif u'\u2e80' <= uchar <= u'\u2eff':  # CJK Radicals Supplement
        return True
    elif u'\u3000' <= uchar <= u'\u303f':  # CJK punctuation mark
        return True
    elif u'\u31c0' <= uchar <= u'\u31ef':  # CJK stroke
        return True
    elif u'\u2f00' <= uchar <= u'\u2fdf':  # Kangxi Radicals
        return True
    elif u'\u2ff0' <= uchar <= u'\u2fff':  # Chinese character structure
        return True
    elif u'\u3100' <= uchar <= u'\u312f':  # Phonetic symbols
        return True
    elif u'\u31a0' <= uchar <= u'\u31bf':  # Phonetic symbols (Taiwanese and Hakka expansion)
        return True
    elif u'\ufe10' <= uchar <= u'\ufe1f':
        return True
    elif u'\ufe30' <= uchar <= u'\ufe4f':
        return True
    elif u'\u2600' <= uchar <= u'\u26ff':
        return True
    elif u'\u2700' <= uchar <= u'\u27bf':
        return True
    elif u'\u3200' <= uchar <= u'\u32ff':
        return True
    elif u'\u3300' <= uchar <= u'\u33ff':
        return True

    return False


def is_char_number(uchar):
    return u'\u0030' <= uchar <= u'\u0039'


def is_char_alphabet(uchar):
    return (u'\u0041' <= uchar <= u'\u005a') or (u'\u0061' <= uchar <= u'\u007a')


def is_char_zh_punctuation(char):
    return char in PUNCTUATION_ZH


def is_char_en_punctuation(char):
    return char in string.punctuation


def is_char_punctuation(char):
    return is_char_zh_punctuation(char) or is_char_en_punctuation(char)


def is_char_valid(char):
    return (
            is_char_alphabet(char)
            or is_char_number(char)
            or is_char_punctuation(char)
            or is_char_chinese(char)
    )


def is_char_control(char, trn: bool = False):
    if char in ('\t', '\r', '\n'):
        return trn
    cat = unicodedata.category(char)
    return cat in ("Cc", "Cf")


def count_char_invalid(text):
    return sum([0 if is_char_valid(c) else 1 for c in text])


def count_alpha(text: str):
    if text is None:
        return 0
    return sum([c.isalpha() for c in text])


def count_decimal(text: str):
    if text is None:
        return 0
    return sum([c.isdecimal() for c in text])


def count_digit(text: str):
    if text is None:
        return 0
    return sum([c.isdigit() for c in text])


def count_numeric(text: str):
    if text is None:
        return 0
    return sum([c.isnumeric() for c in text])


def count_space(text: str):
    if text is None:
        return 0
    return sum([c.isspace() for c in text])


def count_lower(text: str):
    if text is None:
        return 0
    return sum([c.islower() for c in text])


def count_upper(text: str):
    if text is None:
        return 0
    return sum([c.isupper() for c in text])


def count_chinese(text: str):
    if text is None:
        return 0
    return sum([is_char_chinese(c) for c in text])


def is_invalid_chinese(text: str):
    """
    判断是不是中文乱码
    :param text:
    :return: False if not Chinese
    :return: False if is Chinese and is valid
    :return: True if is Chinese is invalid
    """
    try:
        text.encode('gbk')
        return False
    except UnicodeEncodeError:
        return True


def strip_accents(text):
    return ''.join([c for c in unicodedata.normalize("NFD", text) if not unicodedata.combining(c)])


def pad(text: str, width: int, align: str = '<', fill: str = ' '):
    """
    pad the string with `fill` to length of `width`
    :param text: text to pad
    :param width: expected length
    :param align: left: <, center: ^, right: >
    :param fill: char to fill the padding
    :return:
    """
    assert align in ('<', '^', '>')
    return f"{text:{fill}{align}{width}}"


def random(n: int) -> str:
    return ''.join(rand.choices(string.ascii_letters + string.digits, k=n))
