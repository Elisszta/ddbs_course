from typing import Literal

from pydantic import BaseModel, Field


class CourseUpdateParams(BaseModel):
    name: str = Field(min_length=2, max_length=255)
    capacity: int = Field(gt=0)
    tids: list[int] = Field(min_length=1)


class CourseCreateParams(CourseUpdateParams):
    campus: Literal['A', 'B', 'C']


class CourseCreateResp(BaseModel):
    cid: int


class CourseResp(BaseModel):
    cid: int
    teachers: str
    name: str
    capacity: int
    num_selected: int
    campus: Literal['A', 'B', 'C']


class CourseQueryResp(BaseModel):
    total: int
    result: list[CourseResp]


class CourseStudentResp(CourseResp):
    is_selected: bool


class CourseStudentQueryResp(BaseModel):
    total: int
    result: list[CourseStudentResp]
