# -*- coding: utf-8 -*-

import os

def get_env(key, default=None):
    value = os.environ.get(key)
    if value is None:
        return default
    return value
