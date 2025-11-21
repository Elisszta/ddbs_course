from sqlalchemy.ext.asyncio import create_async_engine

from app.utils.settings import settings

master_slave_engine = create_async_engine(settings.db_master_slave_url, echo=True, isolation_level="AUTOCOMMIT")
shard_engine = create_async_engine(settings.db_shard_url, echo=True, isolation_level="AUTOCOMMIT")


async def get_master_slave_connection():
    """
    路由函数依赖：本地主从库
    :return: 本地主从库连接
    """
    async with master_slave_engine.connect() as conn:
        async with conn.begin():
            yield conn


async def get_master_slave_connection_no_tx():
    """
    路由函数依赖：本地主从库无自动事务版
    :return: 本地主从库连接
    """
    async with master_slave_engine.connect() as conn:
        yield conn


async def get_shard_connection():
    """
    路由函数依赖：本地分片库
    :return: 本地分片库连接
    """
    async with shard_engine.connect() as conn:
        async with conn.begin():
            yield conn


async def get_shard_connection_no_tx():
    async with shard_engine.connect() as conn:
        yield conn
