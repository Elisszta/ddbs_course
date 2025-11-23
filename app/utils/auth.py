from typing import Annotated

import jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from app.models.generic_error import err_no_permission, err_invalid_token, err_expired_token, err_invalid_uid
from app.models.user_model import CurUser
from app.utils.classify_helper import get_user_role
from app.settings import settings

security = HTTPBearer()
CredDep = Annotated[HTTPAuthorizationCredentials, Depends(security)]


async def verify_db_api(credentials: CredDep):
    """
    路由函数依赖，确保当前接口只有密钥正确才能访问
    :param credentials:
    :return:
    """
    if credentials.credentials != settings.db_api_secret:
        raise HTTPException(status_code=403, detail=err_invalid_token)


async def get_current_user(credentials: CredDep) -> CurUser:
    """
    路由函数依赖，确保当前接口只有登录后才能访问
    :param credentials:
    :return: 当前用户
    """
    try:
        payload = jwt.decode(credentials.credentials, settings.jwt_secret, algorithms=('HS256',))
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=403, detail=err_expired_token)
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=403, detail=err_invalid_token)
    uid = payload.get('uid')
    if uid is None or type(uid) != int:
        raise HTTPException(status_code=403, detail=err_invalid_token)
    if uid < 1000000000 or uid >= 1400000000:
        raise HTTPException(status_code=403, detail=err_invalid_uid)
    return CurUser(user_id=uid, role=get_user_role(uid))


UserDep = Annotated[CurUser, Depends(get_current_user)]


async def get_current_admin(cur_user: UserDep) -> CurUser:
    """
    路由函数依赖，确保当前接口只有管理员才能访问
    :param cur_user:
    :return: 当前用户
    """
    if cur_user.role != 'admin':
        raise HTTPException(status_code=403, detail=err_no_permission)
    return cur_user


async def get_current_admin_or_teacher(cur_user: UserDep) -> CurUser:
    """
    路由函数依赖，确保当前接口只有管理员或教师才能访问
    :param cur_user:
    :return: 当前用户
    """
    if cur_user.role == 'student':
        raise HTTPException(status_code=403, detail=err_no_permission)
    return cur_user


async def get_current_student(cur_user: UserDep) -> CurUser:
    """
    路由函数依赖，确保当前接口只有管理员或教师才能访问
    :param cur_user:
    :return: 当前用户
    """
    if cur_user.role != 'student':
        raise HTTPException(status_code=403, detail=err_no_permission)
    return cur_user
