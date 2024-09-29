
# -*- coding: utf-8 -*-

"""用于函数执行时间自动计时并通过日志输出的decorator。示例:
  from common_sdk.system.function_timer import function_timer

  @function_timer(name='YourFunction')
  def func():
      xxx
"""
import inspect
from ..logging.logger import logger
from ..util.datetime_utils import DateTime


def function_timer(name=None):
    def decorator(func):
        # 同步函数的包装器
        def wrapper_function_sync(*args, **kwargs):
            start = DateTime()
            result = func(*args, **kwargs)
            milliseconds = DateTime().milliseconds - start.milliseconds
            func_name = name or f'{func.__name__!r}'
            logger.info('[{}] 函数执行时间: {}毫秒'.format(func_name, milliseconds))
            return result

        # 异步函数的包装器
        async def wrapper_function_async(*args, **kwargs):
            start = DateTime()
            result = await func(*args, **kwargs)
            milliseconds = DateTime().milliseconds - start.milliseconds
            func_name = name or f'{func.__name__!r}'
            logger.info('[{}] 函数执行时间: {}毫秒'.format(func_name, milliseconds))
            return result

        # 判断函数是同步还是异步
        if inspect.iscoroutinefunction(func):
            return wrapper_function_async
        else:
            return wrapper_function_sync

    return decorator