from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette.responses import JSONResponse

from app.models.generic_error import GenericError
from app.models.selection_batch_model import SelectionBatchQueryResp, SelectionBatchResp, \
    SelectionBatchCreateParams
from app.routers.dbprivate.master_router import create_selection_batch_private, delete_selection_batch_private
from app.settings import settings
from app.utils.auth import UserDep, AdminDep, get_current_admin
from app.utils.database import get_master_slave_connection
from app.utils.remote_call import remote_db_call

router = APIRouter(
    prefix='/api/v1/selection-batches',
    tags=['Selection Batch API'],
    responses={403: {'model': GenericError, 'description': 'Insufficient permission'}}
)

MasterSlaveConnDep = Annotated[AsyncConnection, Depends(get_master_slave_connection)]


@router.get('')
async def get_selection_batch(cur_user: UserDep, master_slave_conn: MasterSlaveConnDep) -> SelectionBatchQueryResp:
    await master_slave_conn.execute(text("SET time_zone = '+8:00'"))    # 设置成东八区
    if cur_user.role == 'admin':
        resp_result = [SelectionBatchResp(batch_id=row[0], name=row[1], begin_time=row[2], end_time=row[3], status=row[4]) for row in (await master_slave_conn.execute(text(
            'SELECT id, name, begin_time, end_time, CASE '
                "WHEN NOW() < begin_time THEN 'future' "
                "WHEN NOW() BETWEEN begin_time AND end_time THEN 'current' "
                "WHEN NOW() > end_time THEN 'past' "
                "ELSE 'undefined' "
            'END AS status FROM selection_batch ORDER BY begin_time'
        ))).all()]
        return SelectionBatchQueryResp(total=len(resp_result), result=resp_result)
    resp_result = [SelectionBatchResp(batch_id=row[0], name=row[1], begin_time=row[2], end_time=row[3], status=row[4]) for row in (await master_slave_conn.execute(text(
        "(SELECT id, name, begin_time, end_time, 'past' AS status FROM selection_batch WHERE end_time < NOW() ORDER BY end_time DESC LIMIT 1) UNION ALL "
        "(SELECT id, name, begin_time, end_time, 'current' AS status FROM selection_batch WHERE NOW() BETWEEN begin_time AND end_time ORDER BY begin_time ASC) UNION ALL "
        "(SELECT id, name, begin_time, end_time, 'future' AS status FROM selection_batch WHERE begin_time > NOW() ORDER BY begin_time ASC LIMIT 2)"
    ))).all()]
    return SelectionBatchQueryResp(total=len(resp_result), result=resp_result)


@router.post('', status_code=201, dependencies=(Depends(get_current_admin),))
async def create_selection_batch(admin: AdminDep, master_slave_conn: MasterSlaveConnDep, p: SelectionBatchCreateParams) -> SelectionBatchResp:
    if settings.is_master():
        return await create_selection_batch_private(master_slave_conn, p)
    code, resp = await remote_db_call(settings.campus_a_web_url + '/api-private/v1/selection-batches', method='POST', json=p.model_dump())
    if code != 201:
        detail = resp.get('detail') if isinstance(resp, dict) else str(resp)
        raise HTTPException(status_code=code or 502, detail=detail)
    return JSONResponse(status_code=code, content=resp)


@router.delete('/{batch_id}', status_code=204)
async def delete_selection_batch(admin: AdminDep, master_slave_conn: MasterSlaveConnDep, batch_id: int):
    if settings.is_master():
        await delete_selection_batch_private(master_slave_conn, batch_id)
        return
    code, resp = await remote_db_call(f"{settings.campus_a_web_url.rstrip('/')}/api-private/v1/selection-batches/{batch_id}", method='DELETE')
    if code != 204:
        detail = resp.get('detail') if isinstance(resp, dict) else str(resp)
        raise HTTPException(status_code=code or 502, detail=detail)
