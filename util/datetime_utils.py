# -*- coding: utf-8 -*-

import pytz
import time
from datetime import datetime

# import maya


class DateTime(object):

    # 时间戳常量(秒级)
    ONE_SECOND = 1
    ONE_MINUTE = 60 * ONE_SECOND
    ONE_HOUR = 60 * ONE_MINUTE
    ONE_DAY = 24 * ONE_HOUR
    ONE_WEEK = 7 * ONE_DAY
    MILLISECONDS_IN_ONE_SECOND = 1000
    NANOSECONDS_IN_ONE_SECOND = 1000000000

    # 时区
    TIMEZONE_UTC = 'utc'
    TIMEZONE_BEIJING = 'Etc/GMT-8'

    @classmethod
    def timestamp_second(cls):
        """秒级时间戳(十位)"""
        return int(time.time())

    @classmethod
    def timestamp_millisecond(cls):
        """毫秒级时间戳(十三位)"""
        return int(time.time() * cls.MILLISECONDS_IN_ONE_SECOND)

    @classmethod
    def timestamp_nanoseconds(cls):
        """纳秒级时间戳(十九位)"""
        return int(time.time() * cls.NANOSECONDS_IN_ONE_SECOND)

    @property
    def seconds(self):
        return int(self.timestamp / self.NANOSECONDS_IN_ONE_SECOND)

    @property
    def milliseconds(self):
        return int(self.timestamp / 1000000)

    @property
    def nanoseconds(self):
        return self.timestamp

    @property
    def datetime(self):
        utc = pytz.timezone(self.TIMEZONE_UTC)
        return datetime.fromtimestamp(self.seconds, tz=utc).replace(tzinfo=utc).astimezone(self.timezone)

    @property
    def year(self):
        return self.datetime.year

    @property
    def month(self):
        return self.datetime.month

    @property
    def day(self):
        return self.datetime.day

    @property
    def hour(self):
        return self.datetime.hour

    @property
    def minute(self):
        return self.datetime.minute

    @property
    def second(self):
        return self.datetime.second

    @property
    def weekday(self):
        return self.datetime.weekday()

    def __init__(self,
                 seconds=None,
                 milliseconds=None,
                 nanoseconds=None,
                 date_string=None,
                 str_format='%Y-%m-%d %H:%M:%S',
                 timezone=TIMEZONE_BEIJING):
        self.timezone = pytz.timezone(timezone)
        self.reset_time(seconds=seconds,
                        milliseconds=milliseconds,
                        nanoseconds=nanoseconds,
                        date_string=date_string,
                        str_format=str_format)

    def reset_time(self, seconds=None, milliseconds=None, nanoseconds=None, date_string=None, str_format='%Y-%m-%d %H:%M:%S'):
        # 初始化时间
        if seconds is not None:
            self.timestamp = seconds * self.NANOSECONDS_IN_ONE_SECOND
        elif milliseconds:
            self.timestamp = milliseconds * 1000000
        elif nanoseconds:
            self.timestamp = nanoseconds
        elif date_string:
            date = datetime.strptime(date_string, str_format).replace(tzinfo=self.timezone).astimezone(self.timezone)
            self.timestamp = date.timestamp() * self.NANOSECONDS_IN_ONE_SECOND
        else:
            self.timestamp = self.timestamp_nanoseconds()

    def set_timezone(self, timezone):
        self.timezone = pytz.timezone(timezone)
        return self

    def get_date_string(self, format='%Y-%m-%d %H:%M:%S'):
        return self.datetime.strftime(format)

    def add_years(self, years):
        new_year = self.datetime.year + years
        self.timestamp = int(self.datetime.replace(year=new_year).timestamp() * self.NANOSECONDS_IN_ONE_SECOND)
        return self

    def add_months(self, months):
        new_month = self.datetime.month + months
        new_year = self.datetime.year
        if new_month <= 0:
            new_year -= int(-new_month / 12)
            new_year -= 1
            new_month = 12 - (-new_month % 12)
        else:
            new_year += int(new_month / 12)
            new_month = new_month % 12
            if new_month == 0:
                new_month = 12
        self.timestamp = int(self.datetime.replace(year=new_year, month=new_month).timestamp() * self.NANOSECONDS_IN_ONE_SECOND)
        return self

    def add_weeks(self, weeks):
        self.timestamp += weeks * 7 * self.ONE_DAY * self.NANOSECONDS_IN_ONE_SECOND
        return self

    def add_days(self, days):
        self.timestamp += days * self.ONE_DAY * self.NANOSECONDS_IN_ONE_SECOND
        return self

    def add_hours(self, hours):
        self.timestamp += hours * self.ONE_HOUR * self.NANOSECONDS_IN_ONE_SECOND
        return self

    def add_minutes(self, minutes):
        self.timestamp += minutes * self.ONE_MINUTE * self.NANOSECONDS_IN_ONE_SECOND
        return self

    def add_seconds(self, seconds):
        self.timestamp += seconds * self.ONE_SECOND * self.NANOSECONDS_IN_ONE_SECOND
        return self

    def is_weekday(self):
        return self.datetime.weekday() < 5

    def is_weekend(self):
        return not self.is_weekday()

    def reset_minute(self):
        date_format = '%Y-%m-%d %H:%M'
        date_str = self.get_date_string(format=date_format)
        self.reset_time(date_string=date_str, str_format=date_format)
        return self

    def reset_hour(self):
        date_format = '%Y-%m-%d %H'
        date_str = self.get_date_string(format=date_format)
        self.reset_time(date_string=date_str, str_format=date_format)
        return self

    def reset_day(self):
        date_format = '%Y-%m-%d'
        date_str = self.get_date_string(format=date_format)
        self.reset_time(date_string=date_str, str_format=date_format)
        return self

    def reset_week(self):
        self.reset_day()
        self.add_days(-self.datetime.weekday())
        return self

    def reset_month(self):
        date_format = '%Y-%m'
        date_str = self.get_date_string(format=date_format)
        self.reset_time(date_string=date_str, str_format=date_format)
        return self

    def reset_year(self):
        date_format = '%Y'
        date_str = self.get_date_string(format=date_format)
        self.reset_time(date_string=date_str, str_format=date_format)
        return self
