from datetime import datetime
from typing import Literal, Self

from pydantic import BaseModel, model_validator, Field


class SelectionBatchResp(BaseModel):
    batch_id: int
    name: str
    begin_time: datetime
    end_time: datetime
    status: Literal['past', 'current', 'future']


class SelectionBatchQueryResp(BaseModel):
    total: int
    result: list[SelectionBatchResp]


class SelectionBatchCreateParams(BaseModel):
    name: str = Field(min_length=2, max_length=255)
    begin_time: datetime
    end_time: datetime

    @model_validator(mode='after')
    def validate_times(self) -> Self:
        now = datetime.now()
        if self.begin_time <= now:
            raise ValueError('begin_time 必须是未来的时间')
        if self.end_time <= now:
            raise ValueError('end_time 必须是未来的时间')
        if self.begin_time >= self.end_time:
            raise ValueError('begin_time 必须小于 end_time')
        return self
