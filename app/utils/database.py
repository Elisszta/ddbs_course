from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import create_async_engine


class Database:
    __slots__ = ['master_slave_engine', 'shard_engine', 'master_slave_metadata', 'shard_metadata']

    def __init__(self):
        self.master_slave_engine = None
        self.shard_engine = None
        self.master_slave_metadata = None
        self.shard_metadata = None

    async def create_engine(self, master_slave_url: str, shard_url: str, echo: bool = False, isolation_level: str = "AUTOCOMMIT"):
        self.master_slave_engine = create_async_engine(master_slave_url, echo=echo, isolation_level=isolation_level)
        self.shard_engine = create_async_engine(shard_url, echo=echo, isolation_level=isolation_level)
        self.master_slave_metadata = MetaData() # 表反射，用这玩意配上一些函数就不需要裸SQL了
        self.shard_metadata = MetaData()
        async with self.master_slave_engine.begin() as conn:
            await conn.run_sync(self.master_slave_metadata.reflect)
        async with self.shard_engine.begin() as conn:
            await conn.run_sync(self.shard_metadata.reflect)


db = Database()


async def get_master_slave_connection():
    """
    路由函数依赖：本地主从库
    :return: 本地主从库连接
    """
    async with db.master_slave_engine.connect() as conn:
        async with conn.begin():
            yield conn


async def get_master_slave_connection_no_tx():
    """
    路由函数依赖：本地主从库无自动事务版
    :return: 本地主从库连接
    """
    async with db.master_slave_engine.connect() as conn:
        yield conn


async def get_shard_connection():
    """
    路由函数依赖：本地分片库
    :return: 本地分片库连接
    """
    async with db.shard_engine.connect() as conn:
        async with conn.begin():
            yield conn


async def get_shard_connection_no_tx():
    async with db.shard_engine.connect() as conn:
        yield conn
