from typing import Literal

from pydantic import BaseModel, Field


class CourseUpdateParams(BaseModel):
    name: str = Field(min_length=2, max_length=255)
    capacity: int = Field(gt=0)
    teacher_ids: list[int] = Field(min_length=1)


class CourseCreateParams(CourseUpdateParams):
    campus: Literal['A', 'B', 'C']


class CourseCreateResp(BaseModel):
    course_id: int


class CourseResp(BaseModel):
    course_id: int
    teachers: str
    name: str
    capacity: int
    num_selected: int
    campus: Literal['A', 'B', 'C']


class CourseStudentResp(CourseResp):
    is_selected: bool


class CourseQueryResp(BaseModel):
    total: int
    result: list[CourseResp]


class CourseStudentQueryResp(BaseModel):
    total: int
    result: list[CourseStudentResp]


class CourseSelectParams(BaseModel):
    course_id: int
    stu_id: int
