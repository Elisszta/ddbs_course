from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Annotated

import jwt
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from app.models.generic_error import err_invalid_uid
from app.models.user_model import UserLoginParams, UserLoginResp
from app.routers import course_router, student_router, teacher_router
from app.routers.dbprivate import shard_router, master_router
from app.utils.classify_helper import get_user_role
from app.utils.database import db, get_master_slave_connection
from app.settings import settings
from app.utils.auth import get_current_student, get_current_admin_or_teacher, UserDep
from app.models.user_model import CurUser


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.create_engine(settings.db_master_slave_url, settings.db_shard_url, echo=True)
    yield


app = FastAPI(lifespan=lifespan)
app.include_router(shard_router.router)
app.include_router(master_router.router)
app.include_router(course_router.router)
app.include_router(student_router.router)
app.include_router(teacher_router.router)


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


# ================
# FOR TESTING
# ================

# # 1. 伪造一个“学生”身份 (学号 1120250001)
# async def mock_get_current_student():
#     return CurUser(user_id=1120250001, role="student")

# # 2. 伪造一个“教师”身份 (工号 1220250001)
# async def mock_get_current_teacher():
#     return CurUser(user_id=1220250001, role="teacher")

# # 3. 覆盖 FastAPI 的依赖注入
# # 当 Router 里的函数请求 get_current_student 时，FastAPI 会拦截并执行 mock_get_current_student
# app.dependency_overrides[get_current_student] = mock_get_current_student
# app.dependency_overrides[get_current_admin_or_teacher] = mock_get_current_teacher

# # 4. 同时覆盖通用的 UserDep，防止有些接口用的是通用依赖
# async def mock_user_dep():
#     # 这里默认返回学生，如果你测教师接口报错，可以临时改成返回教师
#     return CurUser(user_id=1120250001, role="student")
# app.dependency_overrides[UserDep] = mock_user_dep

# print("\n 注意: 已启用测试模式，身份验证被 Mock 覆盖，所有请求无需 Token \n")