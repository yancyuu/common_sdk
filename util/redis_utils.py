# -*- coding: utf-8 -*-
import json
import time
from typing import TypeVar, Generic, Union

import redis.asyncio as aioredis
from ..system.sys_env import get_env
from typing import Optional, Dict


T = TypeVar("T")


class RedisInstanceManager:
    _instances: Dict[int, aioredis.Redis] = {}

    @classmethod
    def get_redis_instance(
        cls, db: int, host: str, port: int, password: Optional[str], max_connections: int
    ) -> aioredis.Redis:
        if db not in cls._instances:
            pool = aioredis.ConnectionPool(
                host=host,
                port=port,
                password=password,
                db=db,
                decode_responses=False,  # 不自动解码数据
                max_connections=max_connections,  # 限制连接数
            )
            redis_client = aioredis.Redis(connection_pool=pool)
            cls._instances[db] = redis_client
        return cls._instances[db]

    @classmethod
    async def close_all(cls):
        for redis_instance in cls._instances.values():
            # 关闭 Redis 实例（自动处理连接池）
            await redis_instance.close()
        cls._instances.clear()


class AsyncRedisStorage(Generic[T]):
    def __init__(self, redis_client: aioredis.Redis) -> None:
        """
        初始化时接收 Redis 客户端实例。
        """
        self._redis_client = redis_client

    async def set(self, key: str, data: T, expired: int = 7200) -> bool:
        if not key:
            raise ValueError("Key cannot be empty.")
        return await self._redis_client.setex(key, expired, json.dumps(data))

    async def get(self, key: str) -> Union[T, None]:
        if not key:
            raise ValueError("Key cannot be empty.")
        data = await self._redis_client.get(key)
        return json.loads(data) if data else None

    async def delete(self, key: str) -> None:
        if not key:
            raise ValueError("Key cannot be empty.")
        await self._redis_client.delete(key)

    async def enqueue_message(self, queue_name: str, message: T) -> None:
        await self._redis_client.lpush(queue_name, json.dumps(message))

    async def dequeue_message(self, queue_name: str, timeout: int = 0) -> Union[T, None]:
        message = await self._redis_client.brpop(queue_name, timeout=timeout)
        return json.loads(message[1]) if message else None

    async def get_queue_length(self, queue_name: str) -> int:
        return await self._redis_client.llen(queue_name)

    async def acquire_lock(self, key: str, timeout: int = 60) -> bool:
        if timeout <= 0:
            raise ValueError("Lock timeout must be greater than 0 seconds.")
        return await self._redis_client.set(key, "lock", expire=timeout)

    async def release_lock(self, key: str) -> None:
        await self._redis_client.delete(key)

    async def lrange_messages(self, queue_name: str) -> list:
        messages = await self._redis_client.lrange(queue_name, 0, -1)
        return [json.loads(msg) for msg in messages]


# 创建 AsyncRedisStorage 默认实例
async_redis_storage = AsyncRedisStorage(redis_client=RedisInstanceManager.get_redis_instance(
    db=0,
    host=get_env("REDIS_HOST", "localhost"),
    port=get_env("REDIS_PORT", 6379),
    password=get_env("REDIS_PASSWORD", ""),
    max_connections=10
))

