from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import Select, text
from sqlalchemy.ext.asyncio import AsyncConnection

from app.models.course_model import CourseCreateParams, CourseUpdateParams, CourseCreateResp
from app.models.generic_error import GenericError
from app.utils.auth import verify_db_api
from app.utils.database import get_master_slave_connection, get_shard_connection
from app.utils.settings import settings

MasterSlaveConn = Annotated[AsyncConnection, Depends(get_master_slave_connection)]
ShardConn = Annotated[AsyncConnection, Depends(get_shard_connection)]


router = APIRouter(
    prefix='/api/private/v1',
    tags=['DB Cross Site API'],
    responses={403: {'model': GenericError, 'description': 'Insufficient permission'}},
    dependencies=(Depends(verify_db_api),)
)


@router.get('/courses')
async def get_courses(
        master_slave_conn: MasterSlaveConn,
        shard_conn: ShardConn,
        course: int | str | None = None,
        teacher: int | str | None = None,
        only_not_full: bool = False
):
    pass


@router.get('/courses-student')
async def get_courses_student(
        master_slave_conn: MasterSlaveConn,
        shard_conn: ShardConn,
        stu_id: int,
        course: int | str | None = None,
        teacher: int | str | None = None,
        only_not_full: bool = False,
        only_selected: bool = False,
):
    pass


async def gen_course_id(shard_conn: ShardConn) -> int | None:
    min_id = settings.current_min_cid()
    result = await shard_conn.execute(text('SELECT MAX(id) FROM course'))
    max_id = result.scalar()
    if max_id is None:
        return min_id
    if max_id % 100000 < 99999:
        return max_id + 1
    result = await shard_conn.execute(text('SELECT id FROM course ORDER BY id'))
    ids = result.scalars().fetchall()
    if ids[0] != min_id:
        return min_id
    for i in range(len(ids) - 1):
        if ids[i + 1] != ids[i] + 1:
            return ids[i] + 1
    return None


@router.post('/courses', status_code=201)
async def create_course(master_slave_conn: MasterSlaveConn, shard_conn: ShardConn, p: CourseCreateParams) -> CourseCreateResp:
    """
    创建课程路由函数。若课程校区就在本地，可直接原地调用该函数
    :param master_slave_conn: 本地主从库连接
    :param shard_conn: 本地分片库连接
    :param p: 课程创建参数
    :return:
    """
    # 生成id
    await shard_conn.execute(text('LOCK TABLES course WRITE'))  # 课程表全表写锁，确保id生成安全
    new_id = await gen_course_id(shard_conn)
    if new_id is None:
        raise HTTPException(status_code=409, detail='No course id available')
    # 检查教师是否存在
    await master_slave_conn.execute(text('LOCK TABLES teacher READ'))  # 教师表全表读锁防止幻读（mysql默认事务隔离级别不足以阻止幻读）
    for tid in p.tids:
        result = await master_slave_conn.execute(text('SELECT 1 FROM teacher WHERE id = :id'), {'id': tid})
        if result.scalar() is None:
            raise HTTPException(status_code=404, detail=f'No teacher with id {tid}')
    # 插入课程
    await shard_conn.execute(text('LOCK TABLES teach WRITE'))  # 教学表全表写锁防止幻读
    await shard_conn.execute(text('INSERT INTO course(id, name, capacity, num_selected, campus) VALUES (:cid, :name, :capacity, :num_selected, :campus)'), {
        'cid': new_id,
        'name': p.name,
        'capacity': p.capacity,
        'num_selected': 0,
        'campus': p.campus,
    })
    # 插入教学
    await shard_conn.execute(text('INSERT INTO teach(tid, cid) VALUES (:tid, :cid)'), [{'tid': tid, 'cid': new_id} for tid in p.tids])
    await shard_conn.commit()
    return CourseCreateResp(cid=new_id)


@router.delete('/courses/{course_id}', status_code=204)
async def delete_course(shard_conn: ShardConn, course_id: int):
    """
    删除课程路由函数。若课程校区就在本地，可直接原地调用该函数
    :param shard_conn: 本地分片库连接
    :param course_id: 课程id
    :return:
    """
    await shard_conn.execute(text('DELETE FROM course WHERE id = :cid'), {'cid': course_id})


@router.put('/courses/{course_id}', status_code=204)
async def update_course(shard_conn: ShardConn, course_id: int, p: CourseUpdateParams):
    """
    更新课程路由函数。若课程校区就在本地，可直接原地调用该函数
    :param shard_conn: 本地分片库连接
    :param course_id: 课程id
    :param p: 课程更新参数
    :return:
    """
    result = await shard_conn.execute(text('SELECT num_selected FROM course WHERE id = :cid FOR UPDATE'), {'cid': course_id})  # 行级锁启动
    num_selected = result.scalar()
    if num_selected is None:
        raise HTTPException(status_code=404, detail='Course does not exist')
    if p.capacity < num_selected:
        raise HTTPException(status_code=409, detail='Course capacity conflict')
    await shard_conn.execute(text('LOCK TABLES teach WRITE'))  # 教学表全表写锁防止幻读
    await shard_conn.execute(text('UPDATE course SET name = :name, capacity = :capacity WHERE id = :cid'), {'name': p.name, 'capacity': p.capacity, 'cid': course_id})
    await shard_conn.execute(text('DELETE FROM teach WHERE id = :cid'), {'cid': course_id})
    await shard_conn.execute(text('INSERT INTO teach(tid, cid) VALUES (:tid, :cid)'), [{'tid': tid, 'cid': course_id} for tid in p.tids])
