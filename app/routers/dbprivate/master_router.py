from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection

from app.models.generic_error import GenericError, err_user_exist, err_selection_batch_not_exist
from app.models.selection_batch_model import SelectionBatchCreateParams, SelectionBatchResp
from app.models.user_model import StudentCreateParams, TeacherCreateParams
from app.utils.auth import verify_db_api
from app.utils.database import get_master_slave_connection

# 定义主从库连接依赖
MasterSlaveConnDep = Annotated[AsyncConnection, Depends(get_master_slave_connection)]

# 定义 Router，所有接口受 verify_db_api 保护（即必须携带 Secret）
router = APIRouter(
    prefix='/api-private/v1',
    tags=['Cross Site Master DB Private API'],
    responses={403: {'model': GenericError, 'description': 'Insufficient permission'}},
    dependencies=(Depends(verify_db_api),)
)


@router.post('/students', status_code=201)
async def create_student_private(conn: MasterSlaveConnDep, p: StudentCreateParams):
    """
    [私有接口] 接收远程写入：添加学生
    该接口运行在 Master 节点，接收 Slave 节点转发过来的写入请求。
    对应 init.sql 中的 student 表结构: id, name, sex, age, current_campus

    :param conn: SQLAlchemy 异步数据库连接对象 (AsyncConnection)。
                 通过依赖注入获取的主库连接，具有 INSERT/UPDATE/DELETE 权限。
    :param p: 学生创建参数模型 (StudentCreateParams)。
              包含 id, name, sex, age, current_campus 字段。
              会被 model_dump() 转换为字典，自动匹配 SQL 语句中的命名参数 (如 :name)。
    :return: JSON 响应，包含成功提示信息 {"msg": "success"}。
    """
    try:
        # 执行 SQL 写入主库
        await conn.execute(
            text('INSERT INTO student (id, name, sex, age, current_campus) VALUES (:id, :name, :sex, :age, :current_campus)'),
            p.model_dump()
        )
    except IntegrityError:
        # 捕获主键冲突
        raise HTTPException(status_code=409, detail=err_user_exist)
    
    return {"msg": "success"}


@router.post('/teachers', status_code=201)
async def create_teacher_private(conn: MasterSlaveConnDep, p: TeacherCreateParams):
    """
    [私有接口] 接收远程写入：添加教师
    该接口运行在 Master 节点，接收 Slave 节点转发过来的写入请求。
    对应 init.sql 中的 teacher 表结构: id, name, sex, age

    :param conn: SQLAlchemy 异步数据库连接对象 (AsyncConnection)。
                 通过依赖注入获取的主库连接，具有 INSERT/UPDATE/DELETE 权限。
    :param p: 教师创建参数模型 (TeacherCreateParams)。
              包含 id, name, sex, age 字段。
              会被 model_dump() 转换为字典，自动匹配 SQL 语句中的命名参数。
    :return: JSON 响应，包含成功提示信息 {"msg": "success"}。
    """
    try:
        # 执行 SQL 写入主库
        await conn.execute(
            text('INSERT INTO teacher (id, name, sex, age) VALUES (:id, :name, :sex, :age)'),
            p.model_dump()
        )
        await conn.commit()
    except IntegrityError:
        # 捕获主键冲突
        raise HTTPException(status_code=409, detail=err_user_exist)
    
    return {"msg": "success"}


@router.post('/selection-batches', status_code=201)
async def create_selection_batch_private(conn: MasterSlaveConnDep, p: SelectionBatchCreateParams) -> SelectionBatchResp:
    await conn.execute(text('INSERT INTO selection_batch (name, begin_time, end_time) VALUES (:name, :begin_time, :end_time)'), p.model_dump())
    return SelectionBatchResp(batch_id=(await conn.execute(text('SELECT LAST_INSERT_ID()'))).scalar(), name=p.name, begin_time=p.begin_time, end_time=p.end_time, status='future')


@router.delete('/selection-batches/{batch_id}', status_code=204)
async def delete_selection_batch_private(conn: MasterSlaveConnDep, batch_id: int):
    await conn.execute(text('DELETE FROM selection_batch WHERE id = :id'), {'id': batch_id})
