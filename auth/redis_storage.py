# -*- coding: utf-8 -*-

import json
from common_sdk.base_class.singleton import SingletonMetaThreadSafe
import redis
from ..system import sys_env


class RedisStorage(metaclass=SingletonMetaThreadSafe):

    def __init__(self) -> None:
        self._redis = redis.Redis(host=sys_env.get_env('REDIS_ADDRESS'),
                                  port=sys_env.get_env('REDIS_PORT'),
                                  password=sys_env.get_env('REDIS_PASSWORD'))

    def save_token(self, token, key='id', expired=7200) -> bool:
        key = token.get('id')
        if not key:
            raise Exception("找不到key")
        return self._redis.setex(key, expired, json.dumps(token))

    def check_token(self, token, expired=24 * 60 * 60, key='id') -> bool:
        key = token.get('id')
        if not key:
            raise Exception("找不到key")
        return self._redis.expire(key, expired)

    def del_token(self, token, key='id') -> None:
        key = token.get('id')
        if not key:
            raise Exception("找不到key")
        self._redis.delete(key)
    
redis_storage = RedisStorage()
