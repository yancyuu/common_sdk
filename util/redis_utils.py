# -*- coding: utf-8 -*-

import json
from common_sdk.base_class.singleton import SingletonMetaThreadSafe
import redis
import aioredis
from ..system import sys_env


class RedisStorage(metaclass=SingletonMetaThreadSafe):


    def __init__(self) -> None:
        self._redis = redis.Redis.from_url(url=sys_env.get_env('REDIS_ADDRESS'))
        self._redis_async = None

    def set(self, key, data, expired=7200) -> bool:
        if not key:
            raise Exception("找不到key")
        return self._redis.setex(key, expired, json.dumps(data))

    def get(self, key) -> dict:
        if not key:
            raise Exception("找不到key")
        data = self._redis.get(key)
        if not data:
            return {}
        return json.loads(data)

    def check(self, key, expired=24 * 60 * 60) -> bool:
        if not key:
            raise Exception("找不到key")
        return self._redis.expire(key, expired)

    def delete(self, key) -> None:
        if not key:
            raise Exception("找不到key")
        self._redis.delete(key)
    
    def enqueue_message(self, key, message) -> None:
        """将消息添加到队列中"""
        self._redis.lpush(key, json.dumps(message))
    
    def dequeue_message(self, key, timeout=0) -> dict:
        """从队列中移除并返回一条消息"""
        message = self._redis.brpop(key, timeout=timeout)
        if message:
            return json.loads(message[1])
        return None

    def acquire_lock(self, lock_name, lock_timeout=60):
        """尝试获取锁，成功则返回 True，否则返回 False。"""
        result = self._redis.set(lock_name, 1, ex=lock_timeout, nx=True)
        return result is not None

    def release_lock(self, lock_name):
        """释放锁。"""
        self._redis.delete(lock_name)
    
    async def acquire_lock_async(self, lock_name, lock_timeout=60):
        await self._ensure_async_client()
        if lock_timeout == -1:
            # 当不希望键过期时，省略 EX 参数
            result = await self._redis_async.set(lock_name, 1, nx=True)
        else:
            # 确保过期时间是一个正整数
            result = await self._redis_async.set(lock_name, 1, ex=max(1, int(lock_timeout)), nx=True)
        return result is not None


    async def release_lock_async(self, lock_name):
        """释放锁。"""
        await self._ensure_async_client()
        await self._redis_async.delete(lock_name)
        
    async def _ensure_async_client(self):
        if not self._redis_async:
            self._redis_async = aioredis.from_url(
                url=sys_env.get_env('REDIS_ADDRESS'),
                encoding="utf-8",
                decode_responses=True
            )
    
    async def set_async(self, key, data, expired=None) -> bool:
        await self._ensure_async_client()
        if not key:
            raise Exception("找不到key")
        if expired is None:
            # 没有过期时间，使用set命令
            return await self._redis_async.set(key, json.dumps(data))
        else:
            # 有过期时间，使用setex命令
            return await self._redis_async.setex(key, expired, json.dumps(data))

    async def get_async(self, key):
        await self._ensure_async_client()
        if not key:
            raise Exception("找不到key")
        data = await self._redis_async.get(key)
        return json.loads(data) if data else None

    async def check_async(self, key, expired=24 * 60 * 60) -> bool:
        await self._ensure_async_client()
        if not key:
            raise Exception("找不到key")
        return await self._redis_async.expire(key, expired)

    async def delete_async(self, key) -> None:
        await self._ensure_async_client()
        if not key:
            raise Exception("找不到key")
        await self._redis_async.delete(key)

    async def close_async(self):
        if self._redis_async:
            self._redis_async.close()
            await self._redis_async.wait_closed()
    
    async def enqueue_message_async(self, queue_name, message) -> None:
        """异步将消息添加到队列中"""
        await self._ensure_async_client()
        await self._redis_async.lpush(queue_name, json.dumps(message))

    async def dequeue_message_async(self, queue_name, timeout=0) -> dict:
        """异步从队列中移除并返回一条消息"""
        await self._ensure_async_client()
        message = await self._redis_async.brpop(queue_name, timeout=timeout)
        if message:
            return json.loads(message[1])
        return None

    async def get_queue_length_async(self, queue_name):
        """异步获取队列长度"""
        await self._ensure_async_client()
        return await self._redis_async.llen(queue_name)
    
redis_storage = RedisStorage()
