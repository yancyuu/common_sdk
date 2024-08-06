# -*- coding: utf-8 -*-
import json
import time
import traceback
from typing import TypeVar, Generic, Union

import aioredlock
import redis
import redis.asyncio as aioredis
from redlock import Redlock

from common_sdk.base_class.singleton import SingletonMetaThreadSafe
from common_sdk.logging.logger import logger
from ..system import sys_env

T = TypeVar('T')


class BaseRedisStorage(Generic[T], metaclass=SingletonMetaThreadSafe):

    def __init__(self) -> None:
        self.redis_addresses = sys_env.get_env('REDIS_ADDRESS').split(',')
        self.master_address = self.redis_addresses[0]
        self._redlock = Redlock([url for url in self.redis_addresses])
        self._redis = redis.from_url(self.master_address)

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

    def __init__(self) -> None:
        super().__init__()
        self._redlock = aioredlock.Aioredlock([url for url in self.redis_addresses])
        self._redis_async_client = aioredis.from_url(self.master_address)

    async def set(self, key: str, data: T, expired: int = None) -> bool:
        if not key:
            raise Exception("找不到key")
        if expired is None:
            return await self._redis_async_client.set(key, json.dumps(data))
        else:
            return await self._redis_async_client.setex(key, expired, json.dumps(data))

    async def get(self, key: str) -> Union[T, None]:
        if not key:
            raise Exception("找不到key")
        data = await self._redis_async_client.get(key)
        return json.loads(data) if data else None

    async def check(self, key: str, expired: int = 24 * 60 * 60) -> bool:
        if not key:
            raise Exception("找不到key")
        return await self._redis_async_client.expire(key, expired)

    async def delete(self, key: str) -> None:
        if not key:
            raise Exception("找不到key")
        await self._redis_async_client.delete(key)

    async def close(self) -> None:
        if self._redis_async_client:
            self._redis_async_client.close()

    async def enqueue_message(self, queue_name: str, message: T) -> None:
        await self._redis_async_client.lpush(queue_name, json.dumps(message))

    async def dequeue_message(self, queue_name: str, timeout: int = 0) -> Union[T, None]:
        """
        dequeue_message _summary_

        Args:
            queue_name: _description_
            timeout: _description_. Defaults to 0.

        Returns:
            _description_
        """
        message = await self._redis_async_client.brpop(queue_name, timeout=timeout)
        if message:
            return json.loads(message[1])
        return None

    async def dequeue_message_atomic(self, queue_name: str) -> Union[T, None]:
        """
        dequeue_message_atomic 原子化弹出队列数据（避免多线程竞争）

        Args:
            queue_name: 队列名称

        Returns:
            _description_
        """

        lua_script = """
        if redis.call('LLEN', KEYS[1]) > 0 then
            return redis.call('LPOP', KEYS[1])
        else
            return nil
        end
        """
        # 载入 Lua 脚本并执行
        script = await self._redis_async_client.script_load(lua_script)
        message = await self._redis_async_client.evalsha(script, 1, queue_name)
        if message:
            return json.loads(message.decode('utf-8'))
        return None

    async def get_queue_length(self, queue_name: str) -> int:
        return await self._redis_async_client.llen(queue_name)

    async def acquire_lock(self, lock_name: str, lock_timeout: int = 60):
        if lock_timeout <= 0:
            raise ValueError("Lock timeout must be greater than 0 seconds.")

        start_time = time.time() * 1000
        try:
            lock = await self._redlock.lock(lock_name, lock_timeout)
            elapsed_time = time.time() * 1000 - start_time

            if not lock.valid:
                return False

            adjusted_lock_timeout = lock_timeout * 1000 - elapsed_time
            if adjusted_lock_timeout <= 0:
                await self._redlock.unlock(lock)
                return False

            return lock
        except aioredlock.LockError as e:
            err_info = traceback.format_exc()
            logger.error(f"Failed to acquire lock due to error: {err_info}")
            return False

    async def release_lock(self, lock):
        try:
            await self._redlock.unlock(lock)
        except aioredlock.LockError as e:
            logger.error(f"Failed to release lock due to error: {e}")

    async def cleanup(self):
        # 清理资源
        await self._redlock.destroy()


# 示例使用
redis_storage = BaseRedisStorage[dict]()
async_redis_storage = AsyncRedisStorage[dict]()
