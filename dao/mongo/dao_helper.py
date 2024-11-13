import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from google.protobuf import json_format
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
    def mongo_client(self):
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
        return instance.mongo_client[db_name]

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
        collection = await SingletonAsyncMongodbClientHelper.get_collection(self.connect_url, db_name, coll_name)
        matcher = {"id": json_data.get("id")}
        json_data['updateTime'] = date_utils.timestamp_second()
        await collection.update_one(matcher, {"$set": json_data}, upsert=True)

    async def add_or_update_many(self, db_name, coll_name, json_datas):
        collection = await SingletonAsyncMongodbClientHelper.get_collection(self.connect_url, db_name, coll_name)
        operations = [
            UpdateOne({"id": json_data.get("id")}, {"$set": json_data}, upsert=True)
            for json_data in json_datas
            if json_data.update({'updateTime': date_utils.timestamp_second()})
        ]
        if operations:
            result = await collection.bulk_write(operations)
            return result.acknowledged, result.modified_count

    async def get(self, db_name, coll_name, conditions):
        collection = await SingletonAsyncMongodbClientHelper.get_collection(self.connect_url, db_name, coll_name)
        return await collection.find_one(conditions, {"_id": 0}) if conditions else None

    async def list(self, db_name, coll_name, comparisons=None, matcher=None, order_by=None, page=None, size=None):
        collection = await SingletonAsyncMongodbClientHelper.get_collection(self.connect_url, db_name, coll_name)
        matcher = matcher or {}
        contrasts = self._build_comparison_filters(comparisons)
        for contrast in contrasts:
            matcher.update(contrast)

        cursor = collection.find(matcher, {"_id": 0})
        cursor = self.limit_documents(cursor, order_by, page, size)
        result = await cursor.to_list(length=None)
        return result

    async def delete(self, db_name, coll_name, doc_id):
        collection = await SingletonAsyncMongodbClientHelper.get_collection(self.connect_url, db_name, coll_name)
        result = await collection.delete_one({'id': doc_id})
        return result.deleted_count

    async def get_enums(self, db_name, coll_name, fields, conditions=None):
        collection = await SingletonAsyncMongodbClientHelper.get_collection(self.connect_url, db_name, coll_name)
        if not fields:
            raise ValueError("At least one field must be provided for grouping.")
        pipeline = self._build_enum_pipeline(fields, conditions)
        result = await collection.aggregate(pipeline).to_list(length=None)
        return result

    def limit_documents(self, cursor, order_by=None, page=None, size=None):
        if order_by:
            cursor = cursor.sort(order_by)
        limit = size or self._maximum_documents
        if page and size:
            cursor = cursor.skip((page - 1) * size)
        return cursor.limit(limit)

    def _build_comparison_filters(self, comparisons):
        if not comparisons:
            return []
        return [
            {comparison['key']: {"$gt" if comparison.get("bigger", False) else "$lt": comparison['value']}}
            for comparison in comparisons
            if comparison.get("key") and comparison.get("value") is not None
        ]

    def _build_enum_pipeline(self, fields, conditions):
        group_fields = {field: f"${field}" for field in fields}
        pipeline = [{"$match": conditions}] if conditions else []
        pipeline += [
            {"$group": {"_id": group_fields, "count": {"$sum": 1}}},
            {"$project": {"_id": 0, **{field: f"$_id.{field}" for field in fields}, "count": 1}}
        ]
        return pipeline