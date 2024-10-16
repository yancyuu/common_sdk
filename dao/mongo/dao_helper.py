import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from google.protobuf import json_format
from common_sdk.logging.logger import logger
from common_sdk.system.sys_env import get_env

"""异步线程安全的 MongoDB 客户端"""


class SingletonAsyncMongodbClientHelper:
    _instances = {}
    _lock = asyncio.Lock()

    def __init__(self, connect_url):
        self.connect_url = connect_url
        if connect_url:
            self.mongo_client = AsyncIOMotorClient(connect_url)
        else:
            self.mongo_client = None

    @classmethod
    async def create(cls, connect_url):
        async with cls._lock:
            if connect_url in cls._instances:
                return cls._instances[connect_url]
            else:
                instance = cls(connect_url)
                cls._instances[connect_url] = instance
                return instance


class AsyncMongodbClientHelper(SingletonAsyncMongodbClientHelper):
    _maximum_documents = 10000

    @classmethod
    async def create(cls, connect_url=None):
        return await super().create(connect_url)

    async def limit_documents(self, cursor, order_by=None, page=None, size=None):
        """
        限制查询文档的数量，支持排序和分页。
        """
        # 如果提供了 order_by 参数，则对 cursor 排序
        if order_by is not None:
            cursor = cursor.sort(order_by)

        # 设置默认 limit 值
        limit = size if size is not None else self._maximum_documents

        # 如果同时提供了 page 和 size，则计算 skip 值
        if page is not None and size is not None:
            skip = (page - 1) * size
            cursor = cursor.skip(skip)

        # 应用 limit 值
        return cursor.limit(limit)

    async def parse_documents(self, cursor, cls, order_by=None, page=None, size=None):
        """
        解析查询结果，将文档转换为指定的类实例。
        """
        cursor = await self.limit_documents(cursor, order_by=order_by, page=page, size=size)
        ret = []
        async for data in cursor:
            ret.append(self.parse_document(data, cls))
        return ret

    def parse_document(self, data, cls):
        """
        解析单个文档，将其转换为指定的类实例。
        """
        if data is None:
            return None
        return json_format.ParseDict(data, cls(), ignore_unknown_fields=True)

    def combination_contrasts(self, comparisons=None):
        """
        组合对比条件，生成 MongoDB 查询过滤器。
        """
        if comparisons is None:
            return []
        contrasts = []
        for comparison in comparisons:
            contrast = {}
            key = comparison.get("key")
            value = comparison.get("value")
            bigger = comparison.get("bigger", False)
            if None in (key, value):
                continue
            c = "$gt" if bigger else "$lt"
            contrast.update({key: {c: value}})
            contrasts.append(contrast)
        return contrasts