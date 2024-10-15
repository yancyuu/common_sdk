import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
"""异步线程安全的 MySQL 客户端"""


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
                instance = cls(db_url)
                instance.engine = create_async_engine(db_url, echo=False)
                instance.async_session = async_sessionmaker(
                    bind=instance.engine, class_=AsyncSession, expire_on_commit=False
                )
                cls._instances[db_url] = instance
                return instance


class AsyncSQLAlchemyClientHelper(SingletonAsyncSQLAlchemyClientHelper):
    _maximum_rows = 10000

    @staticmethod
    def filter_data_for_model(model, data):
        model_columns = {column.name for column in model.__table__.columns}
        filtered_data = {key: value for key, value in data.items() if key in model_columns}
        return filtered_data