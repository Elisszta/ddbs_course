from typing import Literal

from pydantic import BaseModel


class CurUser(BaseModel):
    uid: int
    role: Literal['teacher', 'student', 'admin']