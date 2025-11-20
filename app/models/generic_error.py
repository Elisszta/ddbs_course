from pydantic import BaseModel


class GenericError(BaseModel):
    detail: str | None = None