# -*- coding: utf-8 -*-

import logging
from logging.handlers import SysLogHandler
from uuid import uuid1

from common_sdk.base_class.singleton import SingletonMetaThreadSafe as SingletonMetaclass
from ..system import sys_env
from ..util import file_utils, context

APPNAME_ENV_NAME = 'APP_NAME'
# logger
LOGGER_CATEGORY_ENV_NAME = "LOGGER_CATEGORY"
# console
LOGGER_ENABLE_CONSOLE_ENV_NAME = "LOGGER_ENABLE_CONSOLE"
# syslog
LOGGER_ENABLE_SYSLOG_ENV_NAME = "LOGGER_ENABLE_SYSLOG"
LOGGER_SYSLOG_HOST_ENV_NAME = "LOGGER_SYSLOG_HOST"
LOGGER_SYSLOG_PORT_ENV_NAME = "LOGGER_SYSLOG_PORT"
LOGGER_SYSLOG_FACILITY_ENV_NAME = "LOGGER_SYSLOG_FACILITY"
# FILE
LOGGER_ENABLE_FILE_ENV_NAME = "LOGGER_ENABLE_FILE"
LOGGER_FILE_DIRECTORY_ENV_NAME = "LOGGER_FILE_DIRECTORY"


class Logger(metaclass=SingletonMetaclass):

    def __init__(self):
        self._formatter = None
        self._logger = logging.getLogger()
        self._logger.setLevel(logging.INFO)
        self.__init_syslog_handler()
        self.__init_console_handler()
        self.__init_file_handler()
        self.logger.info(self.__wrap_message_with_uuid(f"########日志类初始化#######"))

    @property
    def name(self):
        name = sys_env.get_env(APPNAME_ENV_NAME, default="未知应用名称")
        return name

    @property
    def formatter(self):
        if self._formatter is not None:
            return self._formatter
        formatter = f'{self.name} | %(asctime)s | %(levelname)s | %(name)s | pid:%(process)d@%(pathname)s:%(lineno)s | %(message)s'
        self._formatter = logging.Formatter(formatter)
        return self._formatter

    @property
    def message_uuid(self):
        try:
            message_uuid = context.get_message_uuid()
        except Exception:
            pass
        if not message_uuid:
            message_uuid = f"{uuid1()}".replace('-', '')
            context.set_message_uuid(message_uuid)
        return message_uuid

    @property
    def logger(self):
        return self._logger

    def exc_info(self):
        return sys_env.get_env('LOGGER_EXC_INFO', False)

    def debug(self, message, *args, **kwargs):
        message = self.__wrap_message_with_uuid(message)
        kwargs = dict({'stacklevel': 2}, **kwargs)
        self.logger.debug(message, *args, **kwargs)

    def info(self, message, *args, **kwargs):
        message = self.__wrap_message_with_uuid(message)
        kwargs = dict({'stacklevel': 2}, **kwargs)
        self.logger.info(message, *args, **kwargs)

    def exception(self, message, *args, **kwargs):
        message = self.__wrap_message_with_uuid(message)
        kwargs = dict({'exc_info': self.exc_info, 'stacklevel': 2}, **kwargs)
        self.logger.exception(message, *args, **kwargs)

    def error(self, message, *args, **kwargs):
        message = self.__wrap_message_with_uuid(message)
        kwargs = dict({'stacklevel': 2}, **kwargs)
        self.logger.error(message, *args, **kwargs)

    def warning(self, message, *args, **kwargs):
        message = self.__wrap_message_with_uuid(message)
        kwargs = dict({'stacklevel': 2}, **kwargs)
        self.logger.warning(message, *args, **kwargs)

    def fatal(self, message, *args, **kwargs):
        message = self.__wrap_message_with_uuid(message)
        kwargs = dict({'stacklevel': 2}, *kwargs)
        self.logger.critical(message, *args, **kwargs)

    def critical(self, message, *args, **kwargs):
        message = self.__wrap_message_with_uuid(message)
        kwargs = dict({'stacklevel': 2}, *kwargs)
        self.logger.critical(message, *args, **kwargs)

    def __wrap_message_with_uuid(self, message):
        message = str(message).replace('|', '').replace('\r', ' ').replace('\n', ' ')
        if self.message_uuid:
            message = f"{self.message_uuid} | {message}".strip()
        return message

    def __init_syslog_handler(self):
        """ 设置syslog日志
        """
        enable = sys_env.get_env(LOGGER_ENABLE_SYSLOG_ENV_NAME)
        if enable == "false":
            return
        host = sys_env.get_env(LOGGER_SYSLOG_HOST_ENV_NAME)
        port = int(sys_env.get_env(LOGGER_SYSLOG_PORT_ENV_NAME))
        facility = sys_env.get_env(LOGGER_SYSLOG_FACILITY_ENV_NAME)
        category = sys_env.get_env(LOGGER_CATEGORY_ENV_NAME)
        if not category:
            self.__create_syslog_handler(host, port, facility, logging.INFO)
        else:
            cs = category.split(",")
            for c in cs:
                level = self.__name_to_level(c)
                self.__create_syslog_handler(host, port, facility, level)

    def __name_to_level(self, name):
        return logging._nameToLevel[name]

    def __create_syslog_handler(self, host, port, facility, level):
        handler = SysLogHandler(address=(host, port), facility=SysLogHandler.facility_names[facility])
        handler.setFormatter(self.formatter)
        handler.setLevel(level)
        self._logger.addHandler(handler)

    def __init_console_handler(self):
        """ 设置终端日志
        """
        enable = sys_env.get_env(LOGGER_ENABLE_CONSOLE_ENV_NAME)
        if enable == "false":
            return
        handler = logging.StreamHandler()
        handler.setFormatter(self.formatter)
        self._logger.addHandler(handler)

    def __init_file_handler(self):
        """ 设置文件日志
        """
        enable = sys_env.get_env(LOGGER_ENABLE_FILE_ENV_NAME)
        if enable == "false":
            return
        directory = sys_env.get_env(LOGGER_FILE_DIRECTORY_ENV_NAME)
        file_utils.create_dir_if_not_exists(directory)
        category = sys_env.get_env(LOGGER_CATEGORY_ENV_NAME)
        cs = category.split(",")
        for c in cs:
            level = self.__name_to_level(c)
            filepath = file_utils.join_path_filename(directory, c.lower())
            self.__create_file_handler(filepath, level)

    def __create_file_handler(self, filepath, level):
        handler = logging.FileHandler(filepath)
        handler.setLevel(level)
        handler.setFormatter(self.formatter)
        self._logger.addHandler(handler)


# 初始化单例日志实例
logger = Logger()