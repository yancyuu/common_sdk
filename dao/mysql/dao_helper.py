import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from contextlib import asynccontextmanager

"""异步线程安全的 MySQL 客户端"""


class SingletonAsyncSQLAlchemyClientHelper:
    _instances = {}
    _lock = asyncio.Lock()

    def __init__(self, db_url, pool_size=200, max_overflow=200, pool_recycle=300, pool_pre_ping=True):
        self.db_url = db_url
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool_recycle = pool_recycle
        self.pool_pre_ping = pool_pre_ping
        self.engine = None
        self.async_session = None

    @classmethod
    async def create(cls, db_url, pool_size=200, max_overflow=200, pool_recycle=300, pool_pre_ping=False):
        async with cls._lock:
            if db_url in cls._instances:
                return cls._instances[db_url]
            else:
                instance = cls(db_url, pool_size, max_overflow, pool_recycle, pool_pre_ping)
                instance.engine = create_async_engine(
                    db_url,
                    echo=False,
                    pool_size=pool_size,
                    max_overflow=max_overflow,
                    pool_recycle=pool_recycle,
                    pool_pre_ping=pool_pre_ping
                )
                instance.async_session = async_sessionmaker(
                    bind=instance.engine, class_=AsyncSession, expire_on_commit=False
                )
                cls._instances[db_url] = instance
                return instance

    @asynccontextmanager
    async def get_session(self):
        async with self.async_session() as session:
            try:
                yield session
                await session.commit()
            except Exception as e:
                await session.rollback()
                raise e
            finally:
                await session.close()


class AsyncSQLAlchemyClientHelper(SingletonAsyncSQLAlchemyClientHelper):
    _maximum_rows = 10000  # 行数限制

    @staticmethod
    def filter_data_for_model(model, data):
        model_columns = {column.name for column in model.__table__.columns}
        filtered_data = {key: value for key, value in data.items() if key in model_columns}
        return filtered_data

    async def execute_query(self, session, query):
        """执行查询，限制返回结果的最大行数"""
        result = await session.execute(query.limit(self._maximum_rows))
        return result.fetchall()
