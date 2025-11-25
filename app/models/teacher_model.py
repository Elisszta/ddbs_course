from typing import Literal, Optional

from pydantic import Field, BaseModel


class TeacherCreateParams(BaseModel):
    year: int = Field(ge=1900, le=2999)
    name: str = Field(min_length=2, max_length=255)
    sex: Literal['M', 'F']
    age: int = Field(ge=0)


class TeacherUpdateParams(BaseModel):
    name: Optional[str] = Field(min_length=2, max_length=255, default=None)
    sex: Optional[Literal['M', 'F']] = None
    age: Optional[int] = Field(ge=0, default=None)


class TeacherResp(BaseModel):
    teacher_id: int
    name: str
    sex: Literal['M', 'F']
    age: int


class TeacherQueryResp(BaseModel):
    total: int
    result: list[TeacherResp]
