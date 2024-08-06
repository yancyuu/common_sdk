# -*- coding: utf-8 -*-

from ..util.datetime_utils import DateTime


class TimePeriod(object):
    # 当下
    CURRENT_MINUTE = 0
    CURRENT_HOUR = 1
    CURRENT_DAY = 2
    CURRENT_WEEK = 3
    CURRENT_MONTH = 4
    CURRENT_YEAR = 5
    # 最近的一个完整时间周期
    LAST_MINUTE = 101
    LAST_HOUR = 102
    LAST_DAY = 103
    LAST_WEEK = 104
    LAST_MONTH = 105
    LAST_YEAR = 106
    #  固定长度移动时间窗口
    LATEST_24_HOURS = 201
    LATEST_7_DAYS = 202
    LATEST_30_DAYS = 203
    LATEST_90_DAYS = 204
    LATEST_180_DAYS = 205
    LATEST_360_DAYS = 206
    # 随机选择固定数量的数据点样本
    RANDOM_SAMPLE = 501
    # 自定义
    CUSTOM = 1001


    @property
    def period_type(self):
        return self._period_type

    @property
    def start_time(self):
        return self._start_time

    @property
    def end_time(self):
        return self._end_time


    def __init__(self, period_type):
        self._period_type = period_type
        self._start_time = None
        self._end_time = None
        self.set_period_type(period_type)
    
    def set_period_type(self, period_type):
        datetime = DateTime(timezone=DateTime.TIMEZONE_BEIJING)
        if period_type == self.CURRENT_MINUTE:
            self._start_time = datetime.reset_minute().seconds
            self._end_time = datetime.add_minutes(1).seconds
        elif period_type == self.CURRENT_HOUR:
            self._start_time = datetime.reset_hour().seconds
            self._end_time = datetime.add_hours(1).seconds
        elif period_type == self.CURRENT_DAY:
            self._start_time = datetime.reset_day().seconds
            self._end_time = datetime.add_days(1).seconds
        elif period_type == self.CURRENT_WEEK:
            self._start_time = datetime.reset_week().seconds
            self._end_time = datetime.add_weeks(1).seconds
        elif period_type == self.CURRENT_MONTH:
            self._start_time = datetime.reset_month().seconds
            self._end_time = datetime.add_months(1).seconds
        elif period_type == self.CURRENT_YEAR:
            self._start_time = datetime.reset_year().seconds
            self._end_time = datetime.add_years(1).seconds
        elif period_type == self.LAST_MINUTE:
            self._end_time = datetime.reset_minute().seconds
            self._start_time = datetime.add_minutes(-1).seconds
        elif period_type == self.LAST_HOUR:
            self._end_time = datetime.reset_hour().seconds
            self._start_time = datetime.add_hours(-1).seconds
        elif period_type == self.LAST_DAY:
            self._end_time = datetime.reset_day().seconds
            self._start_time = datetime.add_days(-1).seconds
        elif period_type == self.LAST_WEEK:
            self._end_time = datetime.reset_week().seconds
            self._start_time = datetime.add_days(-7).seconds
        elif period_type == self.LAST_MONTH:
            self._end_time = datetime.reset_month().seconds
            self._start_time = datetime.add_months(-1).seconds
        elif period_type == self.LAST_YEAR:
            self._end_time = datetime.reset_year().seconds
            self._start_time = datetime.add_years(-1).seconds
        elif period_type == self.LATEST_24_HOURS:
            self._end_time = datetime.seconds
            self._start_time = self._end_time - DateTime.ONE_DAY
        elif period_type == self.LATEST_7_DAYS:
            self._end_time = datetime.seconds
            self._start_time = self._end_time - DateTime.ONE_DAY * 7
        elif period_type == self.LATEST_30_DAYS:
            self._end_time = datetime.seconds
            self._start_time = self._end_time - DateTime.ONE_DAY * 30
        elif period_type == self.LATEST_90_DAYS:
            self._end_time = datetime.seconds
            self._start_time = self._end_time - DateTime.ONE_DAY * 90
        elif period_type == self.LATEST_180_DAYS:
            self._end_time = datetime.seconds
            self._start_time = self._end_time - DateTime.ONE_DAY * 180
        elif period_type == self.LATEST_360_DAYS:
            self._end_time = datetime.seconds
            self._start_time = self._end_time - DateTime.ONE_DAY * 360
            
    
    def set_custom_period(self, start_time, end_time):
        self.period_type = self.CUSTOM
        self._start_time = start_time
        self._end_time = end_time

        