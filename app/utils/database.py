from sqlalchemy.ext.asyncio import create_async_engine

from app.utils.settings import settings

master_slave_engine = create_async_engine(settings.db_master_slave_url, echo=True)
shard_engine = create_async_engine(settings.db_shard_url, echo=True)


async def get_master_slave_connection():
    async with master_slave_engine.connect() as conn:
        yield conn


async def get_shard_connection():
    async with shard_engine.connect() as conn:
        yield conn
