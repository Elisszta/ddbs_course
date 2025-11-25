from typing import Literal

from pydantic import BaseModel


class CurUser(BaseModel):
    user_id: int
    role: Literal['teacher', 'student', 'admin']


class UserLoginParams(BaseModel):
    user_id: int
    password: str


class UserLoginResp(BaseModel):
    token: str
    user_id: int
    role: Literal['teacher', 'student', 'admin']
    username: str
