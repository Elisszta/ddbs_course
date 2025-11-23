from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import create_async_engine, AsyncConnection

class Database:
    __slots__ = ['master_slave_engine', 'shard_engine', 'master_slave_metadata', 'shard_metadata']

    def __init__(self):
        self.master_slave_engine = None
        self.shard_engine = None
        self.master_slave_metadata = None
        self.shard_metadata = None

    async def create_engine(self, master_slave_url: str, shard_url: str, echo: bool = False):
        """
        初始化数据库引擎。
        
        :param master_slave_url: 主从库的连接 URL (通常连接 Master 以支持写入)。
        :param shard_url: 分片库的连接 URL。
        :param echo: 是否在控制台打印执行的 SQL 语句，用于调试。
        """
        self.master_slave_engine = create_async_engine(
            master_slave_url, 
            echo=echo, 
            pool_recycle=3600
        )
        self.shard_engine = create_async_engine(
            shard_url, 
            echo=echo, 
            pool_recycle=3600
        )
        
        self.master_slave_metadata = MetaData()
        self.shard_metadata = MetaData()
        
        # 初始化表反射
        async with self.master_slave_engine.begin() as conn:
            await conn.run_sync(self.master_slave_metadata.reflect)
        async with self.shard_engine.begin() as conn:
            await conn.run_sync(self.shard_metadata.reflect)


db = Database()


# ==========================================
# Dependencies
# ==========================================

async def get_master_slave_connection():
    """
    路由函数依赖：获取本地主从库连接 (带自动事务管理)。
    
    使用 `async with conn.begin()` 开启显式事务。
    - 如果路由函数成功返回 -> 自动提交 (COMMIT)。
    - 如果路由函数抛出异常 -> 自动回滚 (ROLLBACK)。
    - 适用于执行 INSERT/UPDATE/DELETE 等写入操作。
    
    :return: 处于事务上下文中的 SQLAlchemy 异步连接对象 (AsyncConnection)。
    :raises Exception: 如果数据库引擎尚未初始化。
    """
    if db.master_slave_engine is None:
        raise Exception("Database not initialized")
        
    async with db.master_slave_engine.connect() as conn:
        async with conn.begin():
            yield conn


async def get_master_slave_connection_no_tx():
    """
    路由函数依赖：获取本地主从库连接 (无自动事务)。
    
    不开启显式事务，适用于只读查询 (SELECT)，或者需要手动控制事务的场景。
    
    :return: SQLAlchemy 异步连接对象 (AsyncConnection)。
    :raises Exception: 如果数据库引擎尚未初始化。
    """
    if db.master_slave_engine is None:
        raise Exception("Database not initialized")
        
    async with db.master_slave_engine.connect() as conn:
        yield conn


async def get_shard_connection():
    """
    路由函数依赖：获取本地分片库连接 (带自动事务管理)。
    
    使用 `async with conn.begin()` 开启显式事务。
    适用于选课、退课等需要保证原子性的写入操作。
    
    :return: 处于事务上下文中的 SQLAlchemy 异步连接对象 (AsyncConnection)。
    :raises Exception: 如果数据库引擎尚未初始化。
    """
    if db.shard_engine is None:
        raise Exception("Database not initialized")
        
    async with db.shard_engine.connect() as conn:
        async with conn.begin():
            yield conn


async def get_shard_connection_no_tx():
    """
    路由函数依赖：获取本地分片库连接 (无自动事务)。
    
    不开启显式事务，适用于查询课程列表等只读操作。
    
    :return: SQLAlchemy 异步连接对象 (AsyncConnection)。
    :raises Exception: 如果数据库引擎尚未初始化。
    """
    if db.shard_engine is None:
        raise Exception("Database not initialized")
        
    async with db.shard_engine.connect() as conn:
        yield conn