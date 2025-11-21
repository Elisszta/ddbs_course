from fastapi import APIRouter

from app.models.generic_error import GenericError

router = APIRouter(
    prefix='/api/v1/courses',
    tags=['Course API'],
    responses={403: {'model': GenericError, 'description': 'Insufficient permission'}}
)


