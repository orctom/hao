# -*- coding: utf-8 -*-
import calendar
import datetime
import os
import time

import dateparser
import regex

os.environ['TZ'] = 'Asia/Shanghai'

SECONDS_2_HOUR = 7200
SECONDS_3_HOUR = 10800
SECONDS_4_HOUR = 14400
SECONDS_6_HOUR = 21600
SECONDS_OF_ONE_DAY = 86400
SECONDS_OF_SEVEN_DAY = 86400 * 7

MILLI_SEC_2_WEEK = 1209600000
MILLI_SEC_3_WEEK = 1814400000
MILLI_SEC_4_WEEK = 2419200000

FORMAT_DATE = '%Y-%m-%d'
FORMAT_DATE_CN = '%Y年%m月%d日'
FORMAT_DATE_TIME = '%Y-%m-%d %H:%M:%S'

PARSE_SETTINGS = {'PREFER_DAY_OF_MONTH': 'first'}

TIMEZONE_CST = datetime.timezone(datetime.timedelta(hours=8), 'CST')

CHAR_MAPPING_YEAR = {
    '〇': '0', '一': '1', '二': '2', '三': '3', '四': '4', '五': '5', '六': '6', '七': '7', '八': '8', '九': '9', '零': '0',
    '壹': '1', '贰': '2', '叁': '3', '肆': '4', '伍': '5', '陆': '6', '柒': '7', '捌': '8', '玖': '9', '貮': '2', '两': '2',
    '：': ':', '○': '0', 'O': '0',
    '0': '0', '1': '1', '2': '2', '3': '3', '4': '4', '5': '5', '6': '6', '7': '7', '8': '8', '9': '9',
    '_': '', '◯': '0',
}
CHAR_MAPPING_MONTH = {
    '初': '01',
    '底': '12',
    '春': '03',
    '夏': '06',
    '秋': '10',
    '冬': '12',
    '十': '10',
    '〇': '0', '一': '1', '二': '2', '三': '3', '四': '4', '五': '5', '六': '6', '七': '7', '八': '8', '九': '9', '零': '0',
    '壹': '1', '贰': '2', '叁': '3', '肆': '4', '伍': '5', '陆': '6', '柒': '7', '捌': '8', '玖': '9', '貮': '2', '两': '2',
    '：': ':', '○': '0', 'O': '0',
    '0': '0', '1': '1', '2': '2', '3': '3', '4': '4', '5': '5', '6': '6', '7': '7', '8': '8', '9': '9',
    '_': '', '◯': '0',
}
CHAR_MAPPING_DAY = {
    '初': '01',
    '底': '-1',
    '冬': '12',
    '十': '10', '廿': '20', '卅': '30',
    '〇': '0', '一': '1', '二': '2', '三': '3', '四': '4', '五': '5', '六': '6', '七': '7', '八': '8', '九': '9', '零': '0',
    '壹': '1', '贰': '2', '叁': '3', '肆': '4', '伍': '5', '陆': '6', '柒': '7', '捌': '8', '玖': '9', '貮': '2', '两': '2',
    '：': ':', '○': '0', 'O': '0',
    '0': '0', '1': '1', '2': '2', '3': '3', '4': '4', '5': '5', '6': '6', '7': '7', '8': '8', '9': '9',
    '_': '', '◯': '0',
}
RULES_NORMALIZE = [
    (regex.compile(r'\s*_+\s*(?=[年月日时分秒0-9-])'), ''),
    (regex.compile(r'年+'), '年'),
    (regex.compile(r'月+'), '月'),
    (regex.compile(r'日+'), '日'),
    (regex.compile(r'时+'), '时'),
    (regex.compile(r'分+'), '分'),
    (regex.compile(r'秒+'), '秒'),
    (regex.compile(r'(?<=\d)\s*点'), '时'),
    (regex.compile(r'(?<=[\d])\s+(?=[年月日时分秒])'), ''),
    (regex.compile(r'(?<=[年月日时分秒])\s+(?=[\d])'), ''),
    (regex.compile(r'(?<=[年月日时分秒]\d)\s*(?=\d)'), ''),
    (regex.compile(r'(?<=[年月日时分秒]\s\d)\s*(?=\d)'), ''),
    (regex.compile(r'(?<=\d)\s*(?=\d{1,3}\s*[年月日时分秒])'), ''),
    (regex.compile(r'[^年月日时分秒T:.0-9-]'), ' '),
    (regex.compile(r'(\d)\s*(\d)\s*(\d)\s*(\d)\s*(?=[年.-])'), r'\1\2\3\4'),
    (regex.compile(r'(?<=[月.-])\s*(\d)\s*(\d)'), r'\1\2'),
]

P_ZH_DATE = regex.compile('[年月日号]')
P_ZH_TIME = regex.compile('[时分秒]')


def parse(value, timezone='Asia/Shanghai', keep_timezone: bool = False):
    if value is None:
        return None

    value_type = type(value)

    if value_type in [datetime.date, datetime.datetime]:
        return value

    if value_type == float:
        return epoch_to_datetime(int(value))

    if value_type == int:
        return epoch_to_datetime(value)

    text = value
    text = normalize_chinese(text)
    for p, sub in RULES_NORMALIZE:
        text = p.sub(sub, text)
    if regex.search(r'^\d{8}$', text):
        text = f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
    elif regex.search(r'^\d{4}[-/_]\d{4}$', text):
        text = f"{text[0:4]}-{text[5:7]}-{text[7:9]}"
    elif regex.search(r'^\d{6}[-/_]\d{1,2}$', text):
        text = f"{text[0:4]}-{text[4:6]}-{text[7:9]}"
    elif regex.search(r'^\d{2}[-/_]\d{2}[-/_]\d{2}$', text):
        text = f"20{text[0:2]}-{text[3:5]}-{text[6:8]}"
    elif regex.search(r'^\d{2}[-/_]\d{2}[-/_]\d{2}\b', text):
        text = f"20{text}"
    try:
        settings = {**PARSE_SETTINGS, 'TIMEZONE': timezone}
        date = dateparser.parse(text, languages=['en', 'zh'], settings=settings)
        if date is None:
            return None
        return date if keep_timezone else date.replace(tzinfo=None)
    except (ValueError, OverflowError):
        return None


def normalize_chinese(text: str):
    if P_ZH_TIME.search(text) is not None:
        return text
    splits = list(filter(None, P_ZH_DATE.split(text)))
    if len(splits) not in (3, 4):
        return text
    year, month, day = splits[:3]
    year = ''.join([CHAR_MAPPING_YEAR.get(c, c) for c in year if not c.isspace()])
    month = ''.join([CHAR_MAPPING_MONTH.get(c, c) for c in month if not c.isspace()])
    day = ''.join([CHAR_MAPPING_DAY.get(c, c) for c in day if not c.isspace()])
    if len(month) > 2:
        month = f"{month[0]}{month[-1]}"
    if len(day) > 2:
        day = f"{day[0]}{day[-1]}"
    if day == '-1':
        try:
            day = str(calendar.monthrange(int(year), int(month))[-1])
        except ValueError:
            day = '29'
    return f"{year}年{month}月{day}日"


def formatted(date, fmt=FORMAT_DATE_TIME):
    if date is None:
        return None
    if hasattr(date, 'tzinfo'):
        tz_info = date.tzinfo
        if tz_info is not None and tz_info != TIMEZONE_CST:
            date = date.astimezone(TIMEZONE_CST)
    return date.strftime(fmt)


def pretty_time_delta(seconds):
    if seconds is None:
        return '-'
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    seconds, milliseconds = divmod(seconds, 1)
    if days > 0:
        return f'{int(days)}d, {int(hours)}h, {int(minutes)}m, {int(seconds)}s'
    elif hours > 0:
        return f'{int(hours)}h, {int(minutes)}m, {int(seconds)}s'
    elif minutes > 0:
        return f'{int(minutes)}m, {int(seconds)}s'
    elif seconds > 0:
        if seconds >= 10:
            return f'{int(seconds)}s'
        else:
            return f'{int(seconds)}s, {int(milliseconds * 1000)}ms'
    else:
        return f'{int(milliseconds * 1000)}ms'


def current_milliseconds():
    return int(round(time.time() * 1000))


def current_seconds():
    return round(time.time())


def epoch_to_datetime(epoch: int):
    return datetime.datetime.fromtimestamp(epoch / 1000)


def date_to_datetime(_date: datetime.date) -> datetime.datetime:
    return datetime.datetime.combine(_date, datetime.datetime.min.time())


def is_future(date, margin_seconds=0) -> bool:
    if date is None:
        return False
    now = datetime.datetime.now()
    date_type = type(date)

    if str == date_type:
        time_point = dateparser.parse(date, settings=PARSE_SETTINGS)
    elif tuple == date_type or list == date_type:
        return is_future(date[0], margin_seconds)
    else:
        time_point = date
    time_point = time_point.replace(tzinfo=None)
    return (time_point - now).total_seconds() >= margin_seconds


def is_past(date, margin_seconds=0) -> bool:
    if date is None:
        return False
    now = datetime.datetime.now()
    date_type = type(date)
    if str == date_type:
        time_point = dateparser.parse(date, settings=PARSE_SETTINGS)
    elif tuple == date_type or list == date_type:
        return is_past(date[0], margin_seconds)
    else:
        time_point = date
    time_point = time_point.replace(tzinfo=None)
    return (now - time_point).total_seconds() >= margin_seconds


def to_epoch_milliseconds(value):
    if value is None:
        return 0
    value_type = type(value)
    if value_type == datetime.datetime:
        return int(value.timestamp() * 1000)
    if value_type == datetime.date:
        return int(date_to_datetime(value).timestamp() * 1000)
    if value_type == str:
        value = parse(value)
        return int(value.timestamp() * 1000) if value is not None else None


class Date:

    def __init__(self, val, timezone='Asia/Shanghai', keep_timezone: bool = False) -> None:
        super().__init__()
        self._val = val
        self._date = parse(val, timezone, keep_timezone)

    def formatted(self, fmt=FORMAT_DATE_TIME):
        return formatted(self._date, fmt)

    def is_future(self, margin_seconds=0) -> bool:
        return is_future(self._date, margin_seconds)

    def is_past(self, margin_seconds=0) -> bool:
        return is_past(self._date, margin_seconds)

    def to_epoch_milliseconds(self):
        return to_epoch_milliseconds(self._date)

    def __str__(self):
        return formatted(self._date)

    def __repr__(self):
        return self.__str__()
