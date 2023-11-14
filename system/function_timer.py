
# -*- coding: utf-8 -*-

"""用于函数执行时间自动计时并通过日志输出的decorator。示例:
  from common_sdk.system.function_timer import function_timer

  @function_timer(name='YourFunction')
  def func():
      xxx
"""

from ..logging.logger import logger
from ..util.datetime_utils import DateTime


def function_timer(name=None):
    def decorator(func):
        def wrapper_function(*args, **kwargs):
            start = DateTime()
            result = func(*args,  **kwargs)
            milliseconds = DateTime().milliseconds - start.milliseconds
            func_name = name
            if func_name is None:
                func_name = f'{func.__name__!r}'
            logger.info('[{}]  函数执行时间: {}毫秒'.format(func_name, milliseconds))
            return result
        return wrapper_function

    return decorator