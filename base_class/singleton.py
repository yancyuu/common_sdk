# -*- coding: utf-8 -*-

from threading import Lock


class SingletonMetaThreadSafe(type):
    _instances = {}
    _lock = Lock()

    def __call__(cls, *args, **kwargs):
        # 创建一个基于参数的键，这里选择将参数转化为字符串
        # 注意：这里可能需要对参数做一些预处理，以确保它们是可哈希的（例如，列表转元组）
        # 例如，可以决定只使用特定的关键字参数
        args_repr = tuple(args)   # 将位置参数转为元组
        kwargs_repr = tuple(sorted(kwargs.items()))  # 对关键字参数排序并转为元组
        key = (args_repr, kwargs_repr)

        with cls._lock:
            if (cls, key) not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[(cls, key)] = instance
            return cls._instances[(cls, key)]


class SingletonMetaNoThreadSafe(type):
    """ 非线程安全的单例metaclass
    >>> class BusinessClass(metaclass=SingletonMetaNoThreadSafe):
    >>>     pass
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        args_repr = tuple(args)  # 将位置参数转为元组
        kwargs_repr = tuple(sorted(kwargs.items()))  # 对关键字参数排序并转为元组
        key = (args_repr, kwargs_repr)

        if (cls, key) not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[(cls, key)] = instance
        return cls._instances[(cls, key)]
