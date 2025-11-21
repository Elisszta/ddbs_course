from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.routers import db_router
from app.utils.database import db
from app.utils.settings import settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.create_engine(settings.db_master_slave_url, settings.db_shard_url, echo=True)
    yield


app = FastAPI(lifespan=lifespan)
app.include_router(db_router.router)
