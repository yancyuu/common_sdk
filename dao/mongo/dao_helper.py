import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from common_sdk.util import date_utils
from common_sdk.logging.logger import logger
from common_sdk.system.sys_env import get_env


class SingletonAsyncMongodbClientHelper:
    _instances = {}
    _lock = asyncio.Lock()

    def __init__(self, connect_url):
        self.connect_url = connect_url
        self._mongo_client = None  # 延迟初始化

    @property
    async def mongo_client(self):
        if self._mongo_client is None:
            async with self._lock:
                if self._mongo_client is None:
                    self._mongo_client = AsyncIOMotorClient(
                        self.connect_url,
                        maxPoolSize=250,
                        minPoolSize=10,
                        maxIdleTimeMS=30000
                    )
        return self._mongo_client

    @classmethod
    async def get_instance(cls, connect_url):
        async with cls._lock:
            if connect_url not in cls._instances:
                cls._instances[connect_url] = cls(connect_url)
            return cls._instances[connect_url]

    @classmethod
    async def get_database(cls, connect_url, db_name):
        instance = await cls.get_instance(connect_url)
        return (await instance.mongo_client)[db_name]

    @classmethod
    async def get_collection(cls, connect_url, db_name, coll_name):
        database = await cls.get_database(connect_url, db_name)
        return database[coll_name]


class AsyncMongodbClientHelper(SingletonAsyncMongodbClientHelper):
    _maximum_documents = 10000

    def __init__(self, connect_url=None):
        super().__init__(connect_url)
        self.connect_url = connect_url or get_env('MONGODB_CONNECTION_STRING')

    async def add_or_update(self, db_name, coll_name, json_data):
        collection = await self.get_collection(self.connect_url, db_name, coll_name)
        matcher = {"id": json_data.get("id")}
        json_data['updateTime'] = date_utils.timestamp_second()
        await collection.update_one(matcher, {"$set": json_data}, upsert=True)

    async def add_or_update_many(self, db_name, coll_name, json_datas):
        collection = await self.get_collection(self.connect_url, db_name, coll_name)
        timestamp = date_utils.timestamp_second()
        operations = []
        for json_data in json_datas:
            json_data['updateTime'] = timestamp
            operations.append(
                UpdateOne({"id": json_data.get("id")}, {"$set": json_data}, upsert=True)
            )
        if operations:
            result = await collection.bulk_write(operations)
            return result.acknowledged, result.modified_count

    async def get(self, db_name, coll_name, conditions):
        collection = await self.get_collection(self.connect_url, db_name, coll_name)
        return await collection.find_one(conditions, {"_id": 0}) if conditions else None

    async def list(self, db_name, coll_name, comparisons=None, matcher=None, order_by=None, page=None, size=None):
        collection = await self.get_collection(self.connect_url, db_name, coll_name)
        matcher = matcher or {}
        contrasts = self._build_comparison_filters(comparisons)
        matcher.update(contrasts)

        cursor = collection.find(matcher, {"_id": 0})
        cursor = self.limit_documents(cursor, order_by, page, size)
        result = await cursor.to_list(length=None)
        return result

    async def delete(self, db_name, coll_name, doc_id):
        collection = await self.get_collection(self.connect_url, db_name, coll_name)
        result = await collection.delete_one({'id': doc_id})
        return result.deleted_count

    async def get_enums(self, db_name, coll_name, fields, conditions=None):
        collection = await self.get_collection(self.connect_url, db_name, coll_name)
        if not fields:
            raise ValueError("At least one field must be provided for grouping.")
        pipeline = self._build_enum_pipeline(fields, conditions)
        result = await collection.aggregate(pipeline).to_list(length=None)
        return result

    async def get_random_document(self, db_name, coll_name, size=1, conditions=None):
        """
        从 MongoDB 的指定集合中随机获取一条文档。

        :return: 随机选择的文档 (以字典形式返回)，如果集合为空则返回 None
        """
        # 构建聚合管道
        pipeline = []
        # 使用 $sample 随机获取一条文档
        if conditions:
            pipeline.append({"$match": conditions})
        pipeline.append({"$sample": {"size": size}})
        collection = await self.get_collection(self.connect_url, db_name, coll_name)
        cursor = collection.aggregate(pipeline)
        return [doc async for doc in cursor]

    def limit_documents(self, cursor, order_by=None, page=1, size=None):
        if order_by:
            cursor = cursor.sort(order_by)
        if size is not None:
            limit = size
        else:
            limit = self._maximum_documents
        if page and page > 1 and size:
            cursor = cursor.skip((page - 1) * size)
        return cursor.limit(limit)

    def _build_comparison_filters(self, comparisons):
        if not comparisons:
            return {}
        filters = {}
        for comparison in comparisons:
            key = comparison.get("key")
            value = comparison.get("value")
            if key and value is not None:
                operator = "$gt" if comparison.get("bigger", False) else "$lt"
                if key not in filters:
                    filters[key] = {}
                filters[key][operator] = value
        return filters

    def _build_enum_pipeline(self, fields, conditions):
        group_fields = {field: f"${field}" for field in fields}
        pipeline = [{"$match": conditions}] if conditions else []
        pipeline += [
            {"$group": {"_id": group_fields, "count": {"$sum": 1}}},
            {"$project": {"_id": 0, **{field: f"$_id.{field}" for field in fields}, "count": 1}}
        ]
        return pipeline