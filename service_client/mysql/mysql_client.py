# -*- coding: utf-8 -*-
from common_sdk.base_class.singleton import SingletonMetaThreadSafe as SingletonMetaclass
from common_sdk.logging.logger import logger
from google.protobuf import json_format
from common_sdk.system.sys_env import get_env
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text


class SingletonAsyncSQLAlchemyClientHelper(metaclass=SingletonMetaclass):
    def __init__(self, db_url):
        self.engine = create_async_engine(db_url, echo=False)
        self.async_session = async_sessionmaker(
            bind=self.engine, class_=AsyncSession, expire_on_commit=False
        )


class AsyncSQLAlchemyClientHelper:

    def __init__(self, db_url):
        self._maximum_rows = 10000
        singleton_instance = SingletonAsyncSQLAlchemyClientHelper(db_url)
        self.engine = singleton_instance.engine
        self.async_session = singleton_instance.async_session

    @staticmethod
    def filter_data_for_model(model, data):
        """
        过滤字典数据，仅保留与模型字段匹配的键值对。
        """
        model_columns = {column.name for column in model.__table__.columns}
        filtered_data = {key: value for key, value in data.items() if key in model_columns}
        return filtered_data

