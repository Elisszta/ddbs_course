from typing import Literal, Optional

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


class StudentCreateParams(BaseModel):
    id: int
    name: str
    sex: Literal['M', 'F']
    age: int
    current_campus: Literal['A', 'B', 'C']


class StudentUpdateParams(BaseModel):
    name: Optional[str] = None
    sex: Optional[Literal['M', 'F']] = None
    age: Optional[int] = None
    current_campus: Optional[Literal['A', 'B', 'C']] = None


class StudentSimpleResp(BaseModel):
    id: int
    name: str


class TeacherCreateParams(BaseModel):
    id: int
    name: str
    sex: Literal['M', 'F']
    age: int


class TeacherUpdateParams(BaseModel):
    name: Optional[str] = None
    sex: Optional[Literal['M', 'F']] = None
    age: Optional[int] = None


class TeacherSimpleResp(BaseModel):
    id: int
    name: str
    