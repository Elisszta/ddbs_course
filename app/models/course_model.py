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
