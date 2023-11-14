# -*- coding: utf-8 -*-

import calendar
import collections
import re
import pytz
import time
from datetime import datetime, timedelta

# import maya

TIMEZONE_SHANGHAI = 'Asia/Shanghai'
TIMEZONE_UTC = 'utc'
TIMEZONE = TIMEZONE_SHANGHAI

# 时间戳常量(秒级)
ONE_SECOND = 1
ONE_MINUTE = 60 * ONE_SECOND
ONE_HOUR = 60 * ONE_MINUTE
ONE_DAY = 24 * ONE_HOUR
ONE_WEEK = 7 * ONE_DAY


def get_datetime_in_timezone(epoch_second, timezone_code):
    return datetime.fromtimestamp(epoch_second, tz=get_timezone(timezone_code))


def timestamp_to_string(timestamp, fmt="%Y-%m-%d %H:%M:%S", fromtz=None, totz=None):
    if fromtz is None:
        fromtz = pytz.timezone(TIMEZONE_UTC)
    if totz is None:
        totz = pytz.timezone(TIMEZONE_SHANGHAI)
    date = datetime.fromtimestamp(timestamp).replace(tzinfo=fromtz).astimezone(totz)
    return date.strftime(fmt)


def string_to_timestamp(date, fmt="%Y-%m-%d %H:%M:%S", fromtz=None, totz=None):
    if fromtz is None:
        fromtz = pytz.timezone(TIMEZONE_UTC)
    if totz is None:
        totz = pytz.timezone(TIMEZONE_SHANGHAI)
    date = datetime.strptime(date, fmt).replace(tzinfo=fromtz).astimezone(totz)
    return date.timestamp()


def strtotime(date, fromtz=None, totz=None):
    if fromtz is None:
        fromtz = pytz.timezone(TIMEZONE_UTC)
    if totz is None:
        totz = pytz.timezone(TIMEZONE_SHANGHAI)
    fmt = date
    for i in ['%Y', '%m', '%H', '%M', '%S']:
        fmt = re.sub(r'\d+', i, fmt, 1)
    return int(datetime.strptime(date, fmt).replace(tzinfo=fromtz).astimezone(totz).timestamp())


def datetime_now_in_timezone(timezone_code=TIMEZONE_SHANGHAI):
    return datetime.now(pytz.timezone(timezone_code))


def get_timezone(timezone_code=TIMEZONE_SHANGHAI):
    return pytz.timezone(timezone_code)


def get_date_range(start, end):
    """ 计算两个时间之间的日期
    start: timestamp
    end: timestamp
    """
    TimeRange = collections.namedtuple("TimeRange", ["date", "sort_date"])
    start = int(start)
    end = int(end)

    time_ranges = []
    _start_time = start
    while _start_time < end:
        date = datetime.fromtimestamp(_start_time).strftime("%m-%d")
        sort_date = datetime.fromtimestamp(_start_time).strftime("%Y-%m-%d %H:%M:%S")
        time_ranges.append(TimeRange(date, sort_date))
        _start_time += ONE_DAY
    return time_ranges


def convert_timestamp_to_date(timestamp):
    shanghai = pytz.timezone(TIMEZONE)
    fmt = "%Y-%m-%d"

    date = datetime.fromtimestamp(timestamp)
    date = date.astimezone(shanghai)
    date_str = date.strftime(fmt)

    today = datetime.now(tz=shanghai)
    today_str = today.strftime(fmt)
    yesterday = today - timedelta(1)
    yesterday_str = yesterday.strftime(fmt)
    if date_str == today_str:
        return "今天"
    if date_str == yesterday_str:
        return "昨天"
    return date_str


def get_time_ranges(start_time, end_time, fmt="%Y-%m-%d"):
    TimeRange = collections.namedtuple("TimeRange", ["start_time", "end_time", "start_date", "end_date"])
    shanghai = pytz.timezone(TIMEZONE)
    delta = timedelta(1)

    start_datetime = datetime.fromtimestamp(start_time)
    end_datetime = datetime.fromtimestamp(end_time)

    start_datetime_shanghai = start_datetime.astimezone(shanghai)
    end_datetime_shanghai = end_datetime.astimezone(shanghai)

    start_date = start_datetime_shanghai.replace(hour=0, minute=0, second=0)
    end_date = end_datetime_shanghai.replace(hour=0, minute=0, second=0)
    interval_days = (end_date - start_date).days
    if interval_days == 0:
        end_date = end_date + delta
    if interval_days > 30:
        interval_days = 30

    ret = []
    while interval_days > 0:
        next_date = start_date + delta
        start_time = start_date.timestamp()
        next_time = next_date.timestamp()
        ret.append(TimeRange(start_time, next_time, start_date.strftime(fmt), next_date.strftime(fmt)))
        start_date = start_date + delta
        next_date = next_date + delta
        interval_days -= 1
    return ret


def get_time_range_from_timestamp_and_timedelta(timestamp, days):
    Range = collections.namedtuple("Range", ["start_time", "end_time"])
    timezone = pytz.timezone(TIMEZONE)
    delta = timedelta(days)
    date = datetime.fromtimestamp(timestamp)
    next_date = date - delta

    date = date.astimezone(timezone)
    next_date = next_date.astimezone(timezone)
    date = date.replace(hour=0, minute=0, second=0)
    next_date = next_date.replace(hour=0, minute=0, second=0)
    return Range(start_time=int(next_date.timestamp()), end_time=int(date.timestamp()))


def get_date_zero_timestamp(date=None, days=0):
    now = maya.when('now')
    if date is None:
        date = now
    if not isinstance(date, type(now)):
        raise Exception("传参类型为: {}, 需要参数和为: {}".format(type(date), type(now)))
    timestamp = date.datetime(to_timezone=TIMEZONE_SHANGHAI)
    timestamp = int(timestamp.timestamp()) - date.hour * ONE_HOUR - date.minute * ONE_MINUTE - date.second - days * ONE_DAY
    return timestamp


def get_date_interval_day(date=None, time_delta: int = 0, str_format: str = '%Y-%m-%d %H:%M:%S'):
    """
    获取日期字符串
    :param time_delta: 时间间隔(天)
    :param str_format: 格式化字符串
    :return:
    """
    dt = datetime.today()
    if date:
        dt = datetime.strptime(date, str_format)
    return (dt + timedelta(time_delta)).strftime(str_format)


def get_date_interval_hour(date=None, time_delta: int = 0, str_format: str = '%Y-%m-%d %H:%M:%S'):
    """
    获取日期字符串
    :param time_delta: 时间间隔(小时)
    :param str_format: 格式化字符串
    :return:
    """
    if date:
        timestamp = date_to_timestamp_second(date, str_format=str_format)
    else:
        timestamp = int(time.time())
    return time.strftime(str_format, time.localtime(timestamp + time_delta * 60 * 60))


def timestamp_second():
    """
    秒级时间戳(十位)
    """
    return int(time.time())


def timestamp_millisecond():
    """
    毫秒级时间戳(十三位)
    """
    return int(time.time() * 1000)


def date_to_timestamp_second(date, str_format: str = '%Y-%m-%d %H:%M:%S'):
    """
    秒级时间戳(十位)
    """
    if date is None or len(date) == 0:
        return None

    try:
        timeArray = time.strptime(date, str_format)
        timeStamp = int(time.mktime(timeArray))
        return timeStamp
    except Exception as e:
        print(e)
    return None


def date_to_timestamp_second_tz(date, tz=TIMEZONE_UTC, str_format: str = '%Y-%m-%d %H:%M:%S'):
    """ 把时间字符串转成时间戳
    """
    return int(maya.parse(date, timezone=tz).datetime(to_timezone=tz).timestamp())


def date_to_timestamp_millisecond(date, str_format: str = '%Y-%m-%d %H:%M:%S'):
    """
    毫秒级时间戳(十三位)
    """
    timestamp = date_to_timestamp_second(date, str_format)

    if timestamp:
        timestamp = timestamp * 1000
    return timestamp


def get_date(timestamp=None, str_format: str = '%Y-%m-%d %H:%M:%S'):
    """
    时间戳转日期(十位)
    """
    if timestamp_second:
        return timestamp_second_to_date(timestamp, str_format)

    return time.strftime(str_format, time.localtime(time.time()))


def timestamp_second_to_date(timestamp_second=None, str_format: str = '%Y-%m-%d %H:%M:%S'):
    """
    时间戳转日期(十位)
    """
    time_array = time.localtime(timestamp_second)
    other_style_time = time.strftime(str_format, time_array)
    return other_style_time


def get_weekday(date=None, timestamp=None, str_format: str = '%Y-%m-%d %H:%M:%S'):
    """
    时间戳获取 星期几
    """
    if date:
        timestamp = date_to_timestamp_second(date, str_format)
    if not timestamp:
        timestamp = timestamp_second()

    return int(time.strftime("%w", time.localtime(timestamp)))


def get_month_day(date=None, timestamp=None, str_format: str = '%Y-%m-%d %H:%M:%S'):
    """
  时间戳获取 这个月几号
  """
    if date:
        timestamp = date_to_timestamp_second(date, str_format)
    if not timestamp:
        timestamp = timestamp_second()

    return int(time.strftime("%d", time.localtime(timestamp)))


def timestamp_to_hour(timestamp=0):
    """
  时间戳获取 时间段
  """
    date = time.localtime(timestamp)
    return time.strftime("%H", date)


def get_date_min_interval_day(date=None, time_delta: int = 0, str_format: str = '%Y-%m-%d %H:%M:%S'):
    """
    获取指定间隔日期的最小时间的字符串， 就是如 2019-01-10 00:00:00
    :param time_delta: 时间间隔(天)
    :param str_format: 格式化字符串
    :return:
    """
    time_delta = 0 - time_delta
    dt = datetime.today()
    if date:
        dt = datetime.strptime(date, str_format)
    date = datetime.combine(dt - timedelta(time_delta), datetime.min.time()).strftime(str_format)
    return date


def get_date_max_interval_day(date=None, time_delta: int = 0, str_format: str = '%Y-%m-%d %H:%M:%S'):
    """
    获取指定间隔日期的最大时间的字符串， 就是如 2019-01-10 23:59:59
    :param time_delta: 时间间隔(天)
    :param str_format: 格式化字符串
    :return:
  """

    time_delta = 0 - time_delta
    dt = datetime.today()
    if date:
        dt = datetime.strptime(date, str_format)

    date = datetime.combine(dt - timedelta(time_delta), datetime.max.time()).strftime(str_format)
    return date


def get_hour_delta(from_timestamp=None, to_timestamp=None, from_date=None, to_date=None, str_format: str = '%Y-%m-%d %H:%M:%S'):
    """
    计算指定时间区间(支持时间戳或者日期)的间隔小时
    :param from_timestamp: 开始时间戳
    :param to_timestamp: 结束时间戳
    :param from_date: 开始日期
    :param to_date: 结束日期
    :param str_format: 格式化字符串
    :return:
    """
    if from_date and to_date:
        from_timestamp = date_to_timestamp_second(from_date, str_format)
        to_timestamp = date_to_timestamp_second(to_date, str_format)

    from_date = timestamp_second_to_date(from_timestamp, '%Y-%m-%d:%H')
    to_date = timestamp_second_to_date(to_timestamp, '%Y-%m-%d:%H')
    start_sec = time.mktime(time.strptime(from_date, '%Y-%m-%d:%H'))
    end_sec = time.mktime(time.strptime(to_date, '%Y-%m-%d:%H'))
    day_delta = int((end_sec - start_sec) / (60 * 60))

    return day_delta


def get_day_delta(from_timestamp=None, to_timestamp=None, from_date=None, to_date=None, str_format: str = '%Y-%m-%d %H:%M:%S'):
    """
      计算指定时间区间(支持时间戳或者日期)的间隔天数
      :param from_timestamp: 开始时间戳
      :param to_timestamp: 结束时间戳
      :param from_date: 开始日期
      :param to_date: 结束日期
      :param str_format: 格式化字符串
      :return:
    """
    if from_date and to_date:
        from_timestamp = date_to_timestamp_second(from_date, str_format)
        to_timestamp = date_to_timestamp_second(to_date, str_format)

    from_date = timestamp_second_to_date(from_timestamp, '%Y-%m-%d')
    to_date = timestamp_second_to_date(to_timestamp, '%Y-%m-%d')
    start_sec = time.mktime(time.strptime(from_date, '%Y-%m-%d'))
    end_sec = time.mktime(time.strptime(to_date, '%Y-%m-%d'))
    day_delta = int((end_sec - start_sec) / (24 * 60 * 60))

    return day_delta


def get_week_delta(from_timestamp=None, to_timestamp=None, from_date=None, to_date=None, str_format: str = '%Y-%m-%d %H:%M:%S'):
    """
      计算指定时间区间(支持时间戳或者日期)的间隔周数
      :param from_timestamp: 开始时间戳
      :param to_timestamp: 结束时间戳
      :param from_date: 开始日期
      :param to_date: 结束日期
      :param str_format: 格式化字符串
      :return:
    """
    if from_date and to_date:
        from_timestamp = date_to_timestamp_second(from_date, str_format)
        to_timestamp = date_to_timestamp_second(to_date, str_format)

    from_date = timestamp_second_to_date(from_timestamp, '%Y-%m-%d')
    to_date = timestamp_second_to_date(to_timestamp, '%Y-%m-%d')
    start_sec = time.mktime(time.strptime(from_date, '%Y-%m-%d'))
    end_sec = time.mktime(time.strptime(to_date, '%Y-%m-%d'))
    day_delta = int((end_sec - start_sec) / (24 * 60 * 60))
    week_delta = int(day_delta / 7)
    from_weekday = get_weekday(timestamp=start_sec)
    to_weekday = get_weekday(timestamp=end_sec)
    if from_weekday > to_weekday:
        week_delta += 1

    return week_delta


def get_month_delta(from_timestamp=None, to_timestamp=None, from_date=None, to_date=None, str_format: str = '%Y-%m-%d %H:%M:%S'):
    """
        计算指定时间区间(支持时间戳或者日期)的间隔月数
        :param from_timestamp: 开始时间戳
        :param to_timestamp: 结束时间戳
        :param from_date: 开始日期
        :param to_date: 结束日期
        :param str_format: 格式化字符串
        :return:
    """
    if from_date and to_date:
        from_timestamp = date_to_timestamp_second(from_date, str_format)
        to_timestamp = date_to_timestamp_second(to_date, str_format)

    from_month = int(time.strftime("%m", time.localtime(from_timestamp)))
    to_month = int(time.strftime("%m", time.localtime(to_timestamp)))

    from_year = int(time.strftime("%Y", time.localtime(from_timestamp)))
    to_year = int(time.strftime("%Y", time.localtime(to_timestamp)))

    month_delta = (to_year - from_year) * 12 + (to_month - from_month)

    return month_delta


def get_from_and_to_date(date_year=None, date_month=None, date_day=None, date_hour=None, date_year_week=None):
    if date_year is not None and date_month is not None \
            and date_day is None and date_hour is None and date_year_week is None:
        month_range = calendar.monthrange(int(date_year), int(date_month))
        from_date = datetime(int(date_year), int(date_month), 1)
        to_date = datetime(int(date_year), int(date_month), int(month_range[1]), 23, 59, 59)

    elif date_year is not None and date_year_week is not None \
            and date_day is None and date_hour is None and date_month is None :
        from_date = datetime.strptime("{}-{}-{}".format(date_year, date_year_week, 1), "%Y-%U-%w")
        to_date = datetime.strptime("{}-{}-{} 23:59:59".format(date_year, date_year_week + 1, 0), "%Y-%U-%w %H:%M:%S")
    elif date_year is not None and date_month is not None and date_day is not None \
            and date_hour is None and date_year_week is None :
        from_date = datetime(date_year, date_month, date_day)
        to_date = datetime(date_year, date_month, date_day, 23, 59, 59)

    elif date_year is not None and date_month is not None and date_day is not None and date_hour is not None \
            and date_year_week is None:
        from_date = datetime(date_year, date_month, date_day, date_hour)
        to_date = datetime(date_year, date_month, date_day, date_hour, 59, 59)

    # print("from_date {}, to_date: {}".format(from_date, to_date))
    return from_date.strftime("%Y-%m-%d %H:%M:%S"), to_date.strftime("%Y-%m-%d %H:%M:%S")


def hours_text(timestamp_in_second):
    """ 把时间转成 时:分 的格式. e.g. 10:21
    Args:
        timestamp_in_second: 时间戳,可以是完整的时间戳,也可以是对86400的余数
    Return:
        string
    """
    hours = int(timestamp_in_second / ONE_HOUR)
    minutes = int(timestamp_in_second % ONE_HOUR / ONE_MINUTE)
    return '{:02d}:{:02d}'.format(hours, minutes)


def time_to_timestamp(time_str, separator=":"):
    time_list = time_str.split(separator)
    hour, minute, second = 0, 0, 0
    if len(time_list) == 3:
        hour = int(time_list[0])
        minute = int(time_list[1])
        second = int(time_list[2])
    if len(time_list) == 2:
        hour = int(time_list[0])
        minute = int(time_list[1])
    return hour * ONE_HOUR + minute * ONE_MINUTE + second
