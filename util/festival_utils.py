from datetime import date, timedelta
from borax.calendars import LunarDate
from borax.calendars.festivals2 import FestivalLibrary


class FestivalResult:
    """表示节日的结果类，包含阳历和阴历节日"""

    def __init__(self, solar_festivals=None, lunar_festivals=None, today=None, lunar_today=None):
        self.solar_festivals = solar_festivals or []
        self.lunar_festivals = lunar_festivals or []
        self.today = today
        self.lunar_today = lunar_today

    def __str__(self):
        """格式化输出阳历和阴历节日信息"""
        solar = ', '.join(self.solar_festivals) if self.solar_festivals else '无阳历节日'
        lunar = ', '.join(self.lunar_festivals) if self.lunar_festivals else '无阴历节日'
        return f"阳历日期:{self.today} 阳历节日: {solar}\n阴历日期:{self.lunar_today} 阴历节日: {lunar}"

    def __repr__(self):
        """调试模式下展示"""
        return f"阳历日期:{self.today} 阳历节日:{self.solar_festivals}\n阴历日期:{self.lunar_today} 阴历节日:{self.lunar_festivals})>"


class FestivalInfo:
    """包含节日信息，如距离今天多少天和节日名称"""

    def __init__(self, days_from_today, date, festival_name):
        self.days_from_today = days_from_today
        self.date = date
        self.festival_name = festival_name

    def __str__(self):
        """当打印对象时，返回易于理解的节日信息"""
        return f"{self.festival_name} 距离{self.date}还有 {self.days_from_today} 天"

    def __repr__(self):
        """调试模式下的展示"""
        return f"{self.festival_name} 距离{self.date}还有 {self.days_from_today} 天"


class FestivalUtils:
    def __init__(self):
        # 加载内置节日库
        self.festival_lib = FestivalLibrary.load_builtin()

    def get_today_festivals(self):
        """
        获取今天的阳历节日，阴历节日
        Returns:

        """
        today = date.today()
        lunar_today = LunarDate.today()

        solar_festivals = self.festival_lib.get_festival_names(today)  # 阳历节日
        lunar_festivals = self.festival_lib.get_festival_names(lunar_today)  # 阴历节日

        return FestivalResult(solar_festivals=solar_festivals, lunar_festivals=lunar_festivals, today=today, lunar_today=lunar_today.cn_str())

    def get_all_festivals(self, days=365):
        """

        Args:
            days: 天数

        Returns:

        """
        festivals = []

        for n_days, wd, festival in self.festival_lib.list_days_in_countdown(countdown=days):
            festivals.append(FestivalInfo(n_days, wd, festival.name))
        return festivals

    def get_days_until_festival(self, festival_name):
        """

        Args:
            festival_name: 节日名称

        Returns:

        """
        # 遍历节日库，寻找匹配的节日
        for ndays, wd, festival in self.festival_lib.list_days_in_countdown(countdown=365):
            if festival.name == festival_name:
                return FestivalInfo(ndays, wd, festival.name)

        return None


festival_utils = FestivalUtils()
# 示例调用
if __name__ == "__main__":
    service = FestivalUtils()

    # 获取今天的节日
    today_festivals = service.get_today_festivals()
    print(f"Today's Solar Festivals: {today_festivals.solar_festivals}")
    print(f"Today's Lunar Festivals: {today_festivals.lunar_festivals}")

    # 获取未来365天的所有节日
    all_festivals = service.get_all_festivals(days=365)
    print(f"All Festivals in the next year: {all_festivals}")

    # 获取距离下一个中秋节的天数
    days_until_mid_autumn = service.get_days_until_festival('儿童节')
    print(f"Days until next Mid-Autumn Festival: {days_until_mid_autumn}")
