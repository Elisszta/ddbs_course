from typing import Literal, Optional

from pydantic import Field, BaseModel


class StudentCreateParams(BaseModel):
    year: int = Field(ge=1900, le=2999)
    name: str = Field(min_length=2, max_length=255)
    sex: Literal['M', 'F']
    age: int = Field(ge=0)
    current_campus: Literal['A', 'B', 'C']


class StudentUpdateParams(BaseModel):
    name: Optional[str] = Field(min_length=2, max_length=255, default=None)
    sex: Optional[Literal['M', 'F']] = None
    age: Optional[int] = Field(ge=0, default=None)
    current_campus: Optional[Literal['A', 'B', 'C']] = None


class StudentResp(BaseModel):
    stu_id: int
    name: str
    sex: Literal['M', 'F']
    age: int
    current_campus: Literal['A', 'B', 'C']


class StudentQueryResp(BaseModel):
    total: int
    result: list[StudentResp]