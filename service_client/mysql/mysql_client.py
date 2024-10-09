import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

"""异步线程安全的mysqlclient"""


class SingletonAsyncSQLAlchemyClientHelper:
    _instances = {}
    _lock = asyncio.Lock()

    def __init__(self, db_url):
        self.db_url = db_url
        self.engine = None
        self.async_session = None

    @classmethod
    async def create(cls, db_url):
        async with cls._lock:
            if db_url in cls._instances:
                return cls._instances[db_url]
            else:
                # 创建实例
                instance = cls(db_url)
                # 异步初始化
                instance.engine = create_async_engine(db_url, echo=False)
                instance.async_session = async_sessionmaker(
                    bind=instance.engine, class_=AsyncSession, expire_on_commit=False
                )
                # 缓存实例
                cls._instances[db_url] = instance
                return instance


class AsyncSQLAlchemyClientHelper:

    def __init__(self, db_url):
        self._maximum_rows = 10000
        self.db_url = db_url
        self.engine = None
        self.async_session = None

    @classmethod
    async def create(cls, db_url):
        instance = cls(db_url)
        singleton_instance = await SingletonAsyncSQLAlchemyClientHelper.create(db_url)
        instance.engine = singleton_instance.engine
        instance.async_session = singleton_instance.async_session
        return instance

    @staticmethod
    def filter_data_for_model(model, data):
        """
        过滤字典数据，仅保留与模型字段匹配的键值对。
        """
        model_columns = {column.name for column in model.__table__.columns}
        filtered_data = {key: value for key, value in data.items() if key in model_columns}
        return filtered_data
