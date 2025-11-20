from typing import Annotated

import jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from app.models.user_model import User
from app.utils.settings import settings

security = HTTPBearer()
CredDep = Annotated[HTTPAuthorizationCredentials, Depends(security)]


async def verify_db_api(credentials: CredDep):
    if credentials.credentials != settings.db_api_secret:
        raise HTTPException(status_code=403, detail='Invalid token')


async def get_current_user(credentials: CredDep) -> User:
    try:
        payload = jwt.decode(credentials.credentials, settings.jwt_secret, algorithms=('HS256',))
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=403, detail='Expired token')
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=403, detail='Invalid token')
    uid = payload.get('uid')
    if uid is None or type(uid) != int:
        raise HTTPException(status_code=403, detail='Invalid token')
    if uid < 1000000000 or uid >= 1400000000:
        raise HTTPException(status_code=403, detail='Invalid uid')
    if uid < 1100000000:
        return User(uid=uid, role='admin')
    if uid < 1200000000:
        return User(uid=uid, role='student')
    return User(uid=uid, role='teacher')


UserDep = Annotated[User, Depends(get_current_user)]


async def get_current_admin(user: UserDep) -> User:
    if user.role != 'admin':
        raise HTTPException(status_code=403, detail='You are not allowed to perform this action')
    return user


async def get_current_admin_or_teacher(user: UserDep) -> User:
    if user.role == 'student':
        raise HTTPException(status_code=403, detail='You are not allowed to perform this action')
    return user
