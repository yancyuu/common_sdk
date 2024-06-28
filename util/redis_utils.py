# -*- coding: utf-8 -*-

import json
import time
from common_sdk.base_class.singleton import SingletonMetaThreadSafe
import redis
import aioredis
import aioredlock
from typing import TypeVar, Generic, Union
from ..system import sys_env

T = TypeVar('T')

class BaseRedisStorage(Generic[T], metaclass=SingletonMetaThreadSafe):
    
    def __init__(self) -> None:
        self.redis_addresses = sys_env.get_env('REDIS_ADDRESS').split(',')
        self.master_address = self.redis_addresses[0]
        self._redis = redis.Redis.from_url(self.master_address)

        self._redlock = aioredlock.Aioredlock([{"address": url} for url in self.redis_addresses])
        self._redis_async_client = None

    def set(self, key: str, data: T, expired: int = 7200) -> bool:
        if not key:
            raise Exception("找不到key")
        return self._redis.setex(key, expired, json.dumps(data))

    def get(self, key: str) -> Union[T, None]:
        if not key:
            raise Exception("找不到key")
        data = self._redis.get(key)
        if not data:
            return None
        return json.loads(data)

    def check(self, key: str, expired: int = 24 * 60 * 60) -> bool:
        if not key:
            raise Exception("找不到key")
        return self._redis.expire(key, expired)

    def delete(self, key: str) -> None:
        if not key:
            raise Exception("找不到key")
        self._redis.delete(key)
    
    def enqueue_message(self, key: str, message: T) -> None:
        self._redis.lpush(key, json.dumps(message))
    
    def dequeue_message(self, key: str, timeout: int = 0) -> Union[T, None]:
        message = self._redis.brpop(key, timeout=timeout)
        if message:
            return json.loads(message[1])
        return None
    
    def acquire_lock(self, lock_name: str, lock_timeout: int = 60):
        start_time = time.time() * 1000
        lock = self._redlock.lock(lock_name, lock_timeout * 1000)
        elapsed_time = time.time() * 1000 - start_time

        if not lock:
            return False

        adjusted_lock_timeout = lock_timeout * 1000 - elapsed_time
        if adjusted_lock_timeout <= 0:
            self._redlock.unlock(lock)
            return False

        return lock

    def release_lock(self, lock) -> None:
        self._redlock.unlock(lock)

class AsyncRedisStorage(BaseRedisStorage[T]):

    @property
    async def redis_async_client(self):
        if not self._redis_async_client:
            self._redis_async_client = await aioredis.create_redis_pool(
                self.master_address,
                encoding="utf-8",
                decode_responses=True
            )
        return self._redis_async_client
    
    async def set(self, key: str, data: T, expired: int = None) -> bool:
        redis_async_client = await self.redis_async_client
        if not key:
            raise Exception("找不到key")
        if expired is None:
            return await redis_async_client.set(key, json.dumps(data))
        else:
            return await redis_async_client.setex(key, expired, json.dumps(data))

    async def get(self, key: str) -> Union[T, None]:
        redis_async_client = await self.redis_async_client
        if not key:
            raise Exception("找不到key")
        data = await redis_async_client.get(key)
        return json.loads(data) if data else None

    async def check(self, key: str, expired: int = 24 * 60 * 60) -> bool:
        redis_async_client = await self.redis_async_client
        if not key:
            raise Exception("找不到key")
        return await redis_async_client.expire(key, expired)

    async def delete(self, key: str) -> None:
        redis_async_client = await self.redis_async_client
        if not key:
            raise Exception("找不到key")
        await redis_async_client.delete(key)

    async def close(self) -> None:
        if self._redis_async_client:
            self._redis_async_client.close()
            await self._redis_async_client.wait_closed()

    async def enqueue_message(self, queue_name: str, message: T) -> None:
        redis_async_client = await self.redis_async_client
        await redis_async_client.lpush(queue_name, json.dumps(message))

    async def dequeue_message(self, queue_name: str, timeout: int = 0) -> Union[T, None]:
        redis_async_client = await self.redis_async_client
        message = await redis_async_client.brpop(queue_name, timeout=timeout)
        if message:
            return json.loads(message[1])
        return None

    async def get_queue_length(self, queue_name: str) -> int:
        redis_async_client = await self.redis_async_client
        return await redis_async_client.llen(queue_name)

    async def acquire_lock(self, lock_name: str, lock_timeout: int = 60):
        start_time = time.time() * 1000
        lock = await self._redlock.lock(lock_name, lock_timeout)
        elapsed_time = time.time() * 1000 - start_time

        if not lock:
            return False

        adjusted_lock_timeout = lock_timeout * 1000 - elapsed_time
        if adjusted_lock_timeout <= 0:
            await self._redlock.unlock(lock)
            return False

        return lock

    async def release_lock(self, lock) -> None:
        await self._redlock.unlock(lock)

# 示例使用
redis_storage = BaseRedisStorage[dict]()
async_redis_storage = AsyncRedisStorage[dict]()