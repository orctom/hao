# -*- coding: utf-8 -*-
from typing import Optional

import regex
from charset_normalizer import from_bytes

from . import lists

RE_CHARSET = regex.compile(r'<meta.*?charset=["\']*(.+?)["\'>]', flags=regex.I)
RE_CONTENT_LANGUAGE = regex.compile(r'<meta.*Content-Language:(.+?)[;"\'>]')
RE_PRAGMA = regex.compile(r'<meta.*?content=["\']*;?charset=(.+?)["\'>]', flags=regex.I)

STANDARD_ENCODINGS = [
    "ASMO-708",
    "big5",
    "cp1025",
    "cp866",
    "cp875",
    "csISO2022JP",
    "DOS-720",
    "DOS-862",
    "EUC-CN",
    "EUC-JP",
    "euc-jp",
    "euc-kr",
    "GB18030",
    "gb2312",
    "hz-gb-2312",
    "IBM00858",
    "IBM00924",
    "IBM01047",
    "IBM01140",
    "IBM01141",
    "IBM01142",
    "IBM01143",
    "IBM01144",
    "IBM01145",
    "IBM01146",
    "IBM01147",
    "IBM01148",
    "IBM01149",
    "IBM037",
    "IBM1026",
    "IBM273",
    "IBM277",
    "IBM278",
    "IBM280",
    "IBM284",
    "IBM285",
    "IBM290",
    "IBM297",
    "IBM420",
    "IBM423",
    "IBM424",
    "IBM437",
    "IBM500",
    "ibm737",
    "ibm775",
    "ibm850",
    "ibm852",
    "IBM855",
    "ibm857",
    "IBM860",
    "ibm861",
    "IBM863",
    "IBM864",
    "IBM865",
    "ibm869",
    "IBM870",
    "IBM871",
    "IBM880",
    "IBM905",
    "IBM-Thai",
    "iso-2022-jp",
    "iso-2022-jp",
    "iso-2022-kr",
    "iso-8859-1",
    "iso-8859-13",
    "iso-8859-15",
    "iso-8859-2",
    "iso-8859-3",
    "iso-8859-4",
    "iso-8859-5",
    "iso-8859-6",
    "iso-8859-7",
    "iso-8859-8",
    "iso-8859-8-i",
    "iso-8859-9",
    "Johab",
    "koi8-r",
    "koi8-u",
    "ks_c_5601-1987",
    "macintosh",
    "shift_jis",
    "unicodeFFFE",
    "us-ascii",
    "utf-16",
    "utf-32",
    "utf-32BE",
    "utf-7",
    "utf-8",
    "windows-1250",
    "windows-1251",
    "Windows-1252",
    "windows-1253",
    "windows-1254",
    "windows-1255",
    "windows-1256",
    "windows-1257",
    "windows-1258",
    "windows-874",
    "x-Chinese-CNS",
    "x-Chinese-Eten",
    "x-cp20001",
    "x-cp20003",
    "x-cp20004",
    "x-cp20005",
    "x-cp20261",
    "x-cp20269",
    "x-cp20936",
    "x-cp20949",
    "x-cp50227",
    "x-EBCDIC-KoreanExtended",
    "x-Europa",
    "x-IA5",
    "x-IA5-German",
    "x-IA5-Norwegian",
    "x-IA5-Swedish",
    "x-iscii-as",
    "x-iscii-be",
    "x-iscii-de",
    "x-iscii-gu",
    "x-iscii-ka",
    "x-iscii-ma",
    "x-iscii-or",
    "x-iscii-pa",
    "x-iscii-ta",
    "x-iscii-te",
    "x-mac-arabic",
    "x-mac-ce",
    "x-mac-chinesesimp",
    "x-mac-chinesetrad",
    "x-mac-croatian",
    "x-mac-cyrillic",
    "x-mac-greek",
    "x-mac-hebrew",
    "x-mac-icelandic",
    "x-mac-japanese",
    "x-mac-korean",
    "x-mac-romanian",
    "x-mac-thai",
    "x-mac-turkish",
    "x-mac-ukrainian",
]

CHARSETS = {
    'big5': 'big5hkscs',
    'gbk': 'gbk',
    'gb18030': 'gb18030',
    'gb2312': 'gb18030',
    'ascii': 'utf-8',
    'maccyrillic': 'cp1251',
    'win1251': 'cp1251',
    'win-1251': 'cp1251',
    'windows-1251': 'cp1251',
}

FIXING_ENCODINGS = (
    'iso-8859-1',
    'cp1025',
    'GB18030',
)

def fix_encoding_name(encoding):
    encoding = encoding.lower()
    return CHARSETS.get(encoding, 'utf-8')


def get_encoding(html_text):
    declared_encodings = (RE_CONTENT_LANGUAGE.findall(html_text) + RE_CHARSET.findall(html_text) + RE_PRAGMA.findall(html_text))
    if len(declared_encodings) > 0:
        return fix_encoding_name(declared_encodings[-1])

    return guess_encoding(html_text)


def get_declared_encodings(html_text, uniquify=False):
    encodings = (RE_CONTENT_LANGUAGE.findall(html_text) + RE_CHARSET.findall(html_text) + RE_PRAGMA.findall(html_text))
    if uniquify:
        encodings = lists.uniquify(encodings)
    encodings = [fix_encoding_name(encoding) for encoding in encodings]
    return encodings


def guess_encoding(html_text):
    text = regex.sub("<.*?>", " ", html_text)
    encodding = from_bytes(text.encode()).best()
    return fix_encoding_name(encodding.encoding)


def fix_encoding(text: str, encoding: Optional[str] = None):
    encodings = lists.uniquify([encoding] + FIXING_ENCODINGS)
    for encoding in encodings:
        try:
            return text.encode(encoding).decode()
        except UnicodeDecodeError:
            pass
    return text
