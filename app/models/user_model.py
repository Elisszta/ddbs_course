from typing import Literal

from pydantic import BaseModel


class CurUser(BaseModel):
    uid: int
    role: Literal['teacher', 'student', 'admin']


class StudentResp(BaseModel):
    stu_id: int
    name: str
    sex: Literal['M', 'F']
    age: int
    current_campus: Literal['A', 'B', 'C']


class StudentQueryResp(BaseModel):
    total: int
    result: list[StudentResp]
