from typing import Literal

from pydantic import BaseModel


class User(BaseModel):
    uid: int
    role: Literal['teacher', 'student', 'admin']