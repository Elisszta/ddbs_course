from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Annotated

import jwt
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from app.models.generic_error import err_invalid_uid, GenericError
from app.models.user_login_model import CurUser, UserLoginParams, UserLoginResp
from app.routers import course_router, student_router, teacher_router, selection_batch_router
from app.routers.dbprivate import shard_router, master_router
from app.utils.classify_helper import get_user_role
from app.utils.database import db, get_master_slave_connection
from app.settings import settings
from app.utils.auth import get_current_student, get_current_admin_or_teacher, get_current_admin, UserDep, AdminDep, StudentDep


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
app.include_router(selection_batch_router.router)


@app.post('/api/v1/login', responses={403: {'model': GenericError, 'description': 'Insufficient permission'}})
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

# from app.models.user_login_model import CurUser
# from app.utils.auth import get_current_student, get_current_admin, get_current_user, get_current_admin_or_teacher

# # 1. 伪造一个“全能管理员”
# async def mock_admin():
#     # role 必须是 admin
#     return CurUser(user_id=1020250001, role="admin")

# # 2. 伪造一个“普通学生”
# async def mock_student():
#     # role 必须是 student
#     return CurUser(user_id=1120250001, role="student")

# # 3. 伪造一个“教师”
# async def mock_teacher():
#     return CurUser(user_id=1020251100, role="teacher")

# # 4. 启用覆盖！
# # 注意：这里的中括号里必须填函数名，不能填 StudentDep/AdminDep
# app.dependency_overrides[get_current_admin] = mock_admin
# # app.dependency_overrides[get_current_student] = mock_student
# # app.dependency_overrides[get_current_admin_or_teacher] = mock_teacher
# # app.dependency_overrides[get_current_user] = mock_student # 通用兜底

# print("\n测试模式已启动：身份验证被 Mock 覆盖\n")