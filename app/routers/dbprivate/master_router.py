from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection

from app.models.generic_error import GenericError, err_student_not_exist, err_teacher_not_exist, err_teacher_id_full, \
    err_student_id_conflict, err_student_id_full, err_teacher_id_conflict
from app.models.selection_batch_model import SelectionBatchCreateParams, SelectionBatchResp
from app.models.student_model import StudentCreateParams, StudentUpdateParams, StudentResp
from app.models.teacher_model import TeacherCreateParams, TeacherUpdateParams, TeacherResp
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


async def gen_user_id(conn: MasterSlaveConnDep, table: str, year: int):
    if table == 'teacher':
        id_base = 1200000000
    else:
        id_base = 1100000000
    min_id = id_base + year * 10000
    params = {"id_min": min_id, "id_max": id_base + (year + 1) * 10000}
    max_id = (await conn.execute(text(f'SELECT MAX(id) FROM {table} WHERE id >= :id_min AND id < :id_max'), params)).scalar()
    if max_id is None:
        return min_id
    if max_id % 10000 < 9999:
        return max_id + 1
    ids = (await conn.execute(text(f'SELECT id FROM {table} WHERE id >= :id_min AND id < :id_max ORDER BY id'), params)).scalars().all()
    if ids[0] != min_id:
        return min_id
    for i in range(len(ids) - 1):
        if ids[i + 1] != ids[i] + 1:
            return ids[i] + 1
    return None


@router.post('/students', status_code=201)
async def create_student_private(conn: MasterSlaveConnDep, p: StudentCreateParams) -> StudentResp:
    """
    [私有接口] 接收远程写入：添加学生
    该接口运行在 Master 节点，接收 Slave 节点转发过来的写入请求。
    对应 init.sql 中的 student 表结构: id, name, sex, age, current_campus

    :param conn: SQLAlchemy 异步数据库连接对象 (AsyncConnection)。
                 通过依赖注入获取的主库连接，具有 INSERT/UPDATE/DELETE 权限。
    :param p: 学生创建参数模型 (StudentCreateParams)。
              包含 id, name, sex, age, current_campus 字段。
              会被 model_dump() 转换为字典，自动匹配 SQL 语句中的命名参数 (如 :name)。
    :return: JSON 响应，完整的学生信息。
    """
    # 生成id
    # 无锁，如果真的有并发插入导致id重了，那就返回409让用户重试呗
    new_id = await gen_user_id(conn, 'student', p.year)
    if new_id is None:
        raise HTTPException(status_code=409, detail=err_student_id_full)
    try:
        # 执行 SQL 写入主库
        await conn.execute(
            text('INSERT INTO student (id, name, sex, age, current_campus) VALUES (:id, :name, :sex, :age, :current_campus)'),
            {'id': new_id, 'name': p.name, 'sex': p.sex, 'age': p.age, 'current_campus': p.current_campus}
        )
    except IntegrityError:
        # 捕获主键冲突
        raise HTTPException(status_code=409, detail=err_student_id_conflict)
    return StudentResp(stu_id=new_id, name=p.name, sex=p.sex, age=p.age, current_campus=p.current_campus)


@router.delete('/students/{student_id}', status_code=204)
async def delete_student_private(conn: MasterSlaveConnDep, student_id: int):
    """[私有接口] 在主库删除学生"""
    result = await conn.execute(text("DELETE FROM student WHERE id = :id"), {"id": student_id})
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail=err_student_not_exist)


@router.put('/students/{student_id}', status_code=204)
async def update_student_private(conn: MasterSlaveConnDep, student_id: int, p: StudentUpdateParams):
    """【私有接口】在主库更新学生"""
    updates: list[str] = []
    params: dict[str, Any] = {"id": student_id}
    
    # 动态构建 SQL SET 子句
    if p.name is not None:
        updates.append("name = :name")
        params["name"] = p.name
    if p.sex is not None:
        updates.append("sex = :sex")
        params["sex"] = p.sex
    if p.age is not None:
        updates.append("age = :age")
        params["age"] = p.age
    if p.current_campus is not None:
        updates.append("current_campus = :current_campus")
        params["current_campus"] = p.current_campus
    
    # 情况 1: 没有字段需要更新
    if not updates: 
        # 即使无更新字段，也要检查 ID 是否存在，以符合 RESTful 语义
        check = await conn.execute(text("SELECT 1 FROM student WHERE id = :id"), {"id": student_id})
        if check.scalar() is None:
            raise HTTPException(status_code=404, detail=err_student_not_exist)
        return

    # 执行更新
    sql = f"UPDATE student SET {', '.join(updates)} WHERE id = :id"
    result = await conn.execute(text(sql), params)
    
    # 情况 2: 执行了 SQL 但 rowcount 为 0
    # 可能是数据完全没变，也可能是 ID 不存在
    if result.rowcount == 0:
        check = await conn.execute(text("SELECT 1 FROM student WHERE id = :id"), {"id": student_id})
        if check.scalar() is None:
            raise HTTPException(status_code=404, detail=err_student_not_exist)


@router.post('/teachers', status_code=201)
async def create_teacher_private(conn: MasterSlaveConnDep, p: TeacherCreateParams) -> TeacherResp:
    """
    [私有接口] 接收远程写入：添加教师
    该接口运行在 Master 节点，接收 Slave 节点转发过来的写入请求。
    对应 init.sql 中的 teacher 表结构: id, name, sex, age

    :param conn: SQLAlchemy 异步数据库连接对象 (AsyncConnection)。
                 通过依赖注入获取的主库连接，具有 INSERT/UPDATE/DELETE 权限。
    :param p: 教师创建参数模型 (TeacherCreateParams)。
              包含 id, name, sex, age 字段。
              会被 model_dump() 转换为字典，自动匹配 SQL 语句中的命名参数。
    :return: JSON 响应，完整的教师信息。
    """
    # 生成id
    # 无锁，如果真的有并发插入导致id重了，那就返回409让用户重试呗
    new_id = await gen_user_id(conn, 'teacher', p.year)
    if new_id is None:
        raise HTTPException(status_code=409, detail=err_teacher_id_full)
    try:
        # 执行 SQL 写入主库
        await conn.execute(
            text('INSERT INTO teacher (id, name, sex, age) VALUES (:id, :name, :sex, :age)'),
            {'id': new_id, 'name': p.name, 'sex': p.sex, 'age': p.age}
        )
    except IntegrityError:
        # 捕获主键冲突
        raise HTTPException(status_code=409, detail=err_teacher_id_conflict)
    return TeacherResp(teacher_id=new_id, name=p.name, sex=p.sex, age=p.age)


@router.delete('/teachers/{teacher_id}', status_code=204)
async def delete_teacher_private(conn: MasterSlaveConnDep, teacher_id: int):
    """【私有接口】在主库删除教师"""
    result = await conn.execute(text("DELETE FROM teacher WHERE id = :id"), {"id": teacher_id})
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail=err_teacher_not_exist)


@router.put('/teachers/{teacher_id}', status_code=204)
async def update_teacher_private(conn: MasterSlaveConnDep, teacher_id: int, p: TeacherUpdateParams):
    """【私有接口】在主库更新教师"""
    updates: list[str] = []
    params: dict[str, Any] = {"id": teacher_id}

    if p.name is not None:
        updates.append("name = :name")
        params["name"] = p.name
    if p.sex is not None:
        updates.append("sex = :sex")
        params["sex"] = p.sex
    if p.age is not None:
        updates.append("age = :age")
        params["age"] = p.age

    if not updates:
        # 如果没有字段需要更新，但仍需确认 ID 是否存在
        check = await conn.execute(text("SELECT 1 FROM teacher WHERE id = :id"), {"id": teacher_id})
        if check.scalar() is None:
            raise HTTPException(status_code=404, detail=err_teacher_not_exist)
        return

    sql = f"UPDATE teacher SET {', '.join(updates)} WHERE id = :id"
    result = await conn.execute(text(sql), params)

    # 如果 rowcount 为 0，可能是数据没变，也可能是 ID 不存在
    # 需要进一步查询确认
    if result.rowcount == 0:
        check = await conn.execute(text("SELECT 1 FROM teacher WHERE id = :id"), {"id": teacher_id})
        if check.scalar() is None:
            raise HTTPException(status_code=404, detail=err_teacher_not_exist)


@router.post('/selection-batches', status_code=201)
async def create_selection_batch_private(conn: MasterSlaveConnDep, p: SelectionBatchCreateParams) -> SelectionBatchResp:
    await conn.execute(text('INSERT INTO selection_batch (name, begin_time, end_time) VALUES (:name, :begin_time, :end_time)'), p.model_dump())
    return SelectionBatchResp(batch_id=(await conn.execute(text('SELECT LAST_INSERT_ID()'))).scalar(), name=p.name, begin_time=p.begin_time, end_time=p.end_time, status='future')


@router.delete('/selection-batches/{batch_id}', status_code=204)
async def delete_selection_batch_private(conn: MasterSlaveConnDep, batch_id: int):
    await conn.execute(text('DELETE FROM selection_batch WHERE id = :id'), {'id': batch_id})
