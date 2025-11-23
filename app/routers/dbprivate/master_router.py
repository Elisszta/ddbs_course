from fastapi import APIRouter, Depends

from app.models.generic_error import GenericError
from app.utils.auth import verify_db_api

router = APIRouter(
    prefix='/api-private/v1',
    tags=['Cross Site Master DB Private API'],
    responses={403: {'model': GenericError, 'description': 'Insufficient permission'}},
    dependencies=(Depends(verify_db_api),)
)

