# -*- coding: utf-8 -*-

import logging, sys
from logging.handlers import SysLogHandler
from uuid import uuid1
from typing import Optional

from common_sdk.base_class.singleton import SingletonMetaThreadSafe as SingletonMetaclass
from ..system import sys_env
from ..util import context
from ..config import settings

class LoggerConfig:
    def __init__(
        self,
        app_name: str = "default-app",
        logger_category: str = "INFO",
        enable_console: bool = True,
        enable_syslog: bool = False,
        syslog_host: Optional[str] = None,
        syslog_port: Optional[int] = None,
        syslog_facility: Optional[str] = None,
        enable_file: bool = False,
        file_directory: Optional[str] = None,
        file_categories: Optional[str] = "ERROR,WARNING",
        log_level: int = logging.INFO,
    ):
        self.app_name = app_name
        self.logger_category = logger_category
        self.enable_console = enable_console
        self.enable_syslog = enable_syslog
        self.syslog_host = syslog_host
        self.syslog_port = syslog_port
        self.syslog_facility = syslog_facility
        self.enable_file = enable_file
        self.file_directory = file_directory
        self.file_categories = file_categories
        self.log_level = log_level


class Logger(metaclass=SingletonMetaclass):
    def __init__(self, config: LoggerConfig):
        self.config = config
        self._formatter = None
        self._logger = logging.getLogger()
        self._logger.setLevel(config.log_level)
        self.__init_syslog_handler()
        self.__init_console_handler()
        self.__init_file_handler()
        self.logger.info(self.__wrap_message_with_uuid(f"########日志类初始化#######"))

    @property
    def name(self):
        return self.config.app_name

    @property
    def formatter(self):
        if self._formatter is not None:
            return self._formatter
        formatter = f'{self.name} | %(asctime)s | %(levelname)s | %(name)s | pid:%(process)d@%(pathname)s:%(lineno)s | %(message)s'
        self._formatter = logging.Formatter(formatter)
        return self._formatter

    @property
    def message_uuid(self):
        message_uuid = None
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
        kwargs.setdefault("stacklevel", 2)
        self.logger.debug(message, *args, **kwargs)

    def info(self, message, *args, **kwargs):
        message = self.__wrap_message_with_uuid(message)
        kwargs.setdefault("stacklevel", 2)
        self.logger.info(message, *args, **kwargs)

    def exception(self, message, *args, **kwargs):
        message = self.__wrap_message_with_uuid(message)
        kwargs.setdefault("exc_info", self.exc_info())
        kwargs.setdefault("stacklevel", 2)
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
        kwargs.setdefault("stacklevel", 2)
        self.logger.critical(message, *args, **kwargs)

    def critical(self, message, *args, **kwargs):
        message = self.__wrap_message_with_uuid(message)
        kwargs.setdefault("stacklevel", 2)
        self.logger.critical(message, *args, **kwargs)

    def bind_logger(self, target_logger_name: str, level=logging.DEBUG):
        target_logger = logging.getLogger(target_logger_name)
        target_logger.setLevel(level)
        for handler in self._logger.handlers:
            target_logger.addHandler(handler)
        target_logger.propagate = False  # 防止重复输出

    def __wrap_message_with_uuid(self, message):
        message = str(message).replace('|', '').replace('\r', ' ').replace('\n', ' ')
        if self.message_uuid:
            message = f"{self.message_uuid} | {message}".strip()
        return message

    def __init_syslog_handler(self):
        if self.config.enable_syslog != 'true':
            return
        handler = SysLogHandler(
            address=(self.config.syslog_host, self.config.syslog_port),
            facility=SysLogHandler.facility_names.get(self.config.syslog_facility, SysLogHandler.LOG_USER)
        )
        handler.setFormatter(self.formatter)
        handler.setLevel(self.config.log_level)
        self._logger.addHandler(handler)

    def __init_console_handler(self):
        if self.config.enable_console != 'true':
            return
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(self.formatter)
        self._logger.addHandler(handler)

    def __init_file_handler(self):
        if self.config.enable_file != 'true' or self.config.file_directory != 'true':
            return
        import os
        os.makedirs(self.config.file_directory, exist_ok=True)
        categories = (self.config.file_categories or self.config.logger_category).split(",")
        for c in categories:
            filepath = f"{self.config.file_directory}/{c.lower()}.log"
            handler = logging.FileHandler(filepath)
            handler.setLevel(self.config.log_level)
            handler.setFormatter(self.formatter)
            self._logger.addHandler(handler)


logger = Logger(LoggerConfig(**settings.LOGGING_CONFIG))