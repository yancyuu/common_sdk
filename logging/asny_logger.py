import asyncio
from aiologger import Logger as AsyncLogger
from aiologger.handlers.streams import AsyncStreamHandler
from aiologger.handlers.files import AsyncFileHandler
from aiologger.handlers.base import Handler
from aiologger.formatters.base import Formatter
from logging.handlers import SysLogHandler
from uuid import uuid1
from typing import Union

from common_sdk.base_class.singleton import SingletonMetaThreadSafe as SingletonMetaclass
from ..system import sys_env
from ..util import file_utils, context

APPNAME_ENV_NAME = 'APP_NAME'
LOGGER_CATEGORY_ENV_NAME = "LOGGER_CATEGORY"
LOGGER_ENABLE_CONSOLE_ENV_NAME = "LOGGER_ENABLE_CONSOLE"
LOGGER_ENABLE_SYSLOG_ENV_NAME = "LOGGER_ENABLE_SYSLOG"
LOGGER_SYSLOG_HOST_ENV_NAME = "LOGGER_SYSLOG_HOST"
LOGGER_SYSLOG_PORT_ENV_NAME = "LOGGER_SYSLOG_PORT"
LOGGER_SYSLOG_FACILITY_ENV_NAME = "LOGGER_SYSLOG_FACILITY"
LOGGER_ENABLE_FILE_ENV_NAME = "LOGGER_ENABLE_FILE"
LOGGER_FILE_DIRECTORY_ENV_NAME = "LOGGER_FILE_DIRECTORY"


class AsyncSyslogHandler(Handler):
    """自定义异步 Syslog 日志处理器，适配 aiologger。

    Args:
        address (tuple): Syslog 服务地址，默认 ('localhost', 514)。
        facility (str): Syslog 设施，默认 'LOG_USER'。
    """

    def __init__(self, address: Union[str, tuple[str, int]] = ('localhost', 514), facility=SysLogHandler.LOG_USER):
        super().__init__()
        self._handler = SysLogHandler(address=address, facility=facility)

    async def emit(self, record):
        """异步写入日志记录。

        Args:
            record (logging.LogRecord): 日志记录。
        """
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._handler.emit, record)

    async def close(self):
        """关闭处理器。"""
        await super().close()
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._handler.close)


class AsyncLoggerWrapper(metaclass=SingletonMetaclass):
    """异步日志类，支持控制台、文件和Syslog输出。

    Attributes:
        _formatter (str): 日志格式化模板。
        _logger (AsyncLogger): 核心异步日志器实例。
    """

    def __init__(self):
        """初始化异步日志器"""
        self._formatter = None
        self._logger = None

    async def init_async(self):
        """异步初始化日志器"""
        self._formatter = Formatter(
            fmt=(
                f'{self.name} | %(asctime)s | %(levelname)s | %(name)s | '
                f'pid:%(process)d@%(pathname)s:%(lineno)s | %(message)s'
            )
        )
        self._logger = AsyncLogger(name="async_logger", level="INFO")
        self._init_console_handler()
        self._init_file_handler()
        self._init_syslog_handler()
        await self.info("########日志类初始化#######")

    @property
    def name(self):
        """获取应用名称

        Returns:
            str: 应用名称（从环境变量中获取）。
        """
        return sys_env.get_env(APPNAME_ENV_NAME, default="未知应用名称")

    @property
    def logger(self):
        """返回异步日志器实例

        Returns:
            AsyncLogger: 异步日志器实例。
        """
        return self._logger

    async def debug(self, message, *args, **kwargs):
        """记录调试级别的日志信息"""
        await self._log("debug", message, *args, **kwargs)

    async def info(self, message, *args, **kwargs):
        """记录信息级别的日志信息"""
        await self._log("info", message, *args, **kwargs)

    async def warning(self, message, *args, **kwargs):
        """记录警告级别的日志信息"""
        await self._log("warning", message, *args, **kwargs)

    async def error(self, message, *args, **kwargs):
        """记录错误级别的日志信息"""
        await self._log("error", message, *args, **kwargs)

    async def critical(self, message, *args, **kwargs):
        """记录严重级别的日志信息"""
        await self._log("critical", message, *args, **kwargs)

    async def _log(self, level, message, *args, **kwargs):
        """记录日志信息

        Args:
            level (str): 日志级别名称，如 'info'。
            message (str): 日志信息。
            *args: 其他日志参数。
            **kwargs: 日志关键字参数。
        """
        message = self._wrap_message_with_uuid(message)
        log_method = getattr(self._logger, level)
        await log_method(message, *args, **kwargs)

    def _wrap_message_with_uuid(self, message):
        """为日志信息添加 UUID

        Args:
            message (str): 原始日志信息。

        Returns:
            str: 带 UUID 的日志信息。
        """
        message_uuid = context.get_message_uuid() or str(uuid1()).replace("-", "")
        return f"{message_uuid} | {message}"

    def _init_console_handler(self):
        """初始化控制台日志处理器"""
        enable_console = sys_env.get_env(LOGGER_ENABLE_CONSOLE_ENV_NAME, default="true").lower() == "true"
        if enable_console:
            handler = AsyncStreamHandler(formatter=self._formatter)
            self._logger.add_handler(handler)

    def _init_file_handler(self):
        """初始化文件日志处理器"""
        enable_file = sys_env.get_env(LOGGER_ENABLE_FILE_ENV_NAME, default="false").lower() == "true"
        if enable_file:
            log_dir = sys_env.get_env(LOGGER_FILE_DIRECTORY_ENV_NAME, default="./logs")
            file_utils.create_dir_if_not_exists(log_dir)
            log_file = file_utils.join_path_filename(log_dir, "app.log")
            handler = AsyncFileHandler(filename=log_file)
            self._logger.add_handler(handler)

    def _init_syslog_handler(self):
        """初始化 Syslog 日志处理器"""
        enable_syslog = sys_env.get_env(LOGGER_ENABLE_SYSLOG_ENV_NAME, default="false").lower() == "true"
        if enable_syslog:
            host = sys_env.get_env(LOGGER_SYSLOG_HOST_ENV_NAME, default="localhost")
            port = int(sys_env.get_env(LOGGER_SYSLOG_PORT_ENV_NAME, default="514"))
            facility = sys_env.get_env(LOGGER_SYSLOG_FACILITY_ENV_NAME, default="LOG_USER")

            # 使用自定义的 AsyncSyslogHandler
            handler = AsyncSyslogHandler(address=(host, port), facility=facility)
            self._logger.add_handler(handler)

    def _create_syslog_handler(self, host, port, facility):
        """创建 Syslog 日志处理器

        Args:
            host (str): Syslog 主机地址。
            port (int): Syslog 端口号。
            facility (str): Syslog 设施名称。

        Returns:
            SysLogHandler: 配置好的 Syslog 处理器。
        """
        syslog_handler = SysLogHandler(address=(host, port), facility=facility)
        syslog_handler.setFormatter(self._formatter)
        return syslog_handler


logger = AsyncLoggerWrapper()
