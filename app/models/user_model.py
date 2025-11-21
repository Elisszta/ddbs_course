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


class StudentResp(BaseModel):
    stu_id: int
    name: str
    sex: Literal['M', 'F']
    age: int
    current_campus: Literal['A', 'B', 'C']


class StudentQueryResp(BaseModel):
    total: int
    result: list[StudentResp]
