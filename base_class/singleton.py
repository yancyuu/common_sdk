# -*- coding: utf-8 -*-

from threading import Lock


class SingletonMetaThreadSafe(type):
    """
    线程安全的单例类的metaclass
    >>> class BusinessClass(metaclass=SingletonMetaThreadSafe):
    >>>     pass
    """
    _instances = {}
    _lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]


class SingletonMetaNoThreadSafe(type):
    """ 非线程安全的单例metaclass
    >>> class BusinessClass(metaclass=SingletonMetaNoThreadSafe):
    >>>     pass
    """
    _instances = {}

    def __call__(cls, *args, **kargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kargs)
            cls._instances[cls] = instance
        return cls._instances[cls]
