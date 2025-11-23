from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Annotated

import jwt
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from app.models.generic_error import err_invalid_uid
from app.models.user_model import UserLoginParams, UserLoginResp
from app.routers import course_router
from app.routers.dbprivate import shard_router, master_router
from app.utils.classify_helper import get_user_role
from app.utils.database import db, get_master_slave_connection
from app.settings import settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.create_engine(settings.db_master_slave_url, settings.db_shard_url, echo=True)
    yield


app = FastAPI(lifespan=lifespan)
app.include_router(shard_router.router)
app.include_router(master_router.router)
app.include_router(course_router.router)


@app.post('/api/v1/login')
async def login(master_slave_conn: Annotated[AsyncConnection, Depends(get_master_slave_connection)], p: UserLoginParams) -> UserLoginResp:
    if p.user_id < 1000000000 or p.user_id >= 1400000000:
        raise HTTPException(status_code=403, detail=err_invalid_uid)
    role = get_user_role(p.user_id)
    expire = datetime.now(timezone.utc) + timedelta(hours=24)
    if role == 'admin':
        encoded_jwt = jwt.encode({'exp': expire, 'uid': p.user_id}, settings.jwt_secret, algorithm='HS256')
        return UserLoginResp(token=encoded_jwt, user_id=p.user_id, role='admin', username='admin')
    username = (await master_slave_conn.execute(text(f'SELECT name FROM {role} WHERE id = :id'), {'id': p.user_id})).scalar()
    if username is None:
        raise HTTPException(status_code=403, detail=err_invalid_uid)
    encoded_jwt = jwt.encode({'exp': expire, 'uid': p.user_id}, settings.jwt_secret, algorithm='HS256')
    return UserLoginResp(token=encoded_jwt, user_id=p.user_id, role=role, username=username)
