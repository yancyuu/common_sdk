"""用户进程内部通讯的的公共变量"""
from common_sdk.base_class.singleton import SingletonMetaThreadSafe as SingletonMetaclass
from common_sdk.logging.logger import logger

"""
    e.g.  [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "Who won the world series in 2020?"},
        {"role": "assistant", "content": "The Los Angeles Dodgers won the World Series in 2020."},
        {"role": "user", "content": "Where was it played?"}
    ]
"""


class GlobalSession(metaclass=SingletonMetaclass):

    def __init__(self):
        self.global_map = {}


global_param = GlobalSession()