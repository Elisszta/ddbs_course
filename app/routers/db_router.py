from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from app.models.course_model import CourseCreateParams, CourseUpdateParams, CourseCreateResp, CourseStudentQueryResp, \
    CourseStudentResp, CourseResp, CourseQueryResp
from app.models.generic_error import GenericError
from app.utils.auth import verify_db_api
from app.utils.database import get_master_slave_connection, get_shard_connection, get_master_slave_connection_no_tx
from app.utils.settings import settings

MasterSlaveConnNoTx = Annotated[AsyncConnection, Depends(get_master_slave_connection_no_tx)]
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
        master_slave_conn: MasterSlaveConnNoTx,
        shard_conn: ShardConn,
        course: int | str | None = None,
        teacher: int | str | None = None,
        only_not_full: bool = False
) -> CourseQueryResp:
    """
    教师管理员课程查询路由函数。若课程校区就在本地，可直接原地调用该函数
    :param master_slave_conn: 本地主从库连接，不自动事务
    :param shard_conn: 本地分片库连接
    :param course: 课程id或课程关键词或空
    :param teacher: 教师id或教师名或空
    :param only_not_full: 是否只查询未满
    :return: 查询结果
    """
    # 由于主从复制gtid临时表限制，这里的master_slave_conn必须手动控制事务
    await master_slave_conn.execute(text('CREATE TEMPORARY TABLE tmp_tid (tid INT NOT NULL)'))  # 可以保证从分片库导过来的tid条数小于主从库的教师表条数，所以temp_tid是驱动表，所以就不建索引了
    await shard_conn.execute(text('CREATE TEMPORARY TABLE tmp_tid_name (tid INT NOT NULL, name VARCHAR(255) NOT NULL)'))
    # 啥条件都没限定的查询
    if course is None and teacher is None and not only_not_full:
        distinct_tid_result = await shard_conn.execute(text('SELECT DISTINCT tid FROM teach'))
        table_name = 'teach'
    else:
        await shard_conn.execute(text('CREATE TEMPORARY TABLE temp_cid_tid (cid INT NOT NULL, tid INT NOT NULL, INDEX idx_tid (tid))'))
        params = {}
        join_str = ''
        where_part: list[str] = []
        if type(course) is int:
            where_part.append('c.id = :cid')
            params['cid'] = course
        elif type(course) is str:
            where_part.append("c.name LIKE CONCAT('%', :name, '%')")
            params['name'] = course
        if type(teacher) is int:
            join_str = 'JOIN teach t ON c.id = t.cid'
            where_part.append('t.tid = :tid')
            params['tid'] = teacher
        elif type(teacher) is str:
            join_str = 'JOIN teach t ON c.id = t.cid'
            where_part.append('t.tid IN :tid')
            result = await master_slave_conn.execute(text('SELECT id FROM teacher WHERE name = :name'), {'name': teacher})
            params['tid'] = result.scalars().all()
        if only_not_full:
            where_part.append('c.capacity > c.num_selected')
        await shard_conn.execute(text(f'INSERT INTO temp_cid_tid SELECT tmp.id, t.tid FROM (course c {join_str} WHERE {" AND ".join(where_part)}) tmp JOIN teach t ON tmp.id = t.cid'), params)
        distinct_tid_result = await shard_conn.execute(text('SELECT DISTINCT tid FROM temp_cid_tid'))
        table_name = 'temp_cid_tid'
    await master_slave_conn.execute(text('INSERT INTO tmp_tid(tid) VALUES (:tid)'), [{'tid': tid} for tid in distinct_tid_result.scalars().all()])
    result = await master_slave_conn.execute(text('SELECT t.id, t.name FROM tmp_tid tmp JOIN teacher t ON t.id = tmp.tid'))
    await shard_conn.execute(text('INSERT INTO tmp_tid_name (tid, name) VALUES (:tid, :name)'), [{'tid': row[0], 'name': row[1]} for row in result.all()])
    result = await shard_conn.execute(text("SELECT c.id, GROUP_CONCAT(tmp.name, ', ') AS teachers, c.name, c.capacity, c.num_selected, c.campus FROM course c "
                                           f'JOIN {table_name} t ON c.id = t.cid '
                                           'JOIN tmp_tid_name tmp ON t.tid = tmp.tid '
                                           'GROUP BY c.id'))
    resp_result = [CourseResp(cid=row[0], teachers=row[1], name=row[2], capacity=row[3], num_selected=row[4], campus=row[5]) for row in result.all()]
    return CourseQueryResp(total=len(resp_result), result=resp_result)


@router.get('/courses-student')
async def get_courses_student(
        master_slave_conn: MasterSlaveConnNoTx,
        shard_conn: ShardConn,
        stu_id: int,
        course: int | str | None = None,
        teacher: int | str | None = None,
        only_not_full: bool = False,
        only_selected: bool = False,
) -> CourseStudentQueryResp:
    """
    学生课程查询路由函数。若课程校区就在本地，可直接原地调用该函数
    :param master_slave_conn: 本地主从库连接，不自动事务
    :param shard_conn: 本地分片库连接
    :param stu_id: 学生id
    :param course: 课程id或课程关键词或空
    :param teacher: 教师id或教师名或空
    :param only_not_full: 是否只查询未满
    :param only_selected: 是否只查询已选
    :return: 查询结果
    """
    # 由于主从复制gtid临时表限制，这里的master_slave_conn必须手动控制事务
    await master_slave_conn.execute(text('CREATE TEMPORARY TABLE tmp_tid (tid INT NOT NULL)'))  # 可以保证从分片库导过来的tid条数小于主从库的教师表条数，所以temp_tid是驱动表，所以就不建索引了
    await shard_conn.execute(text('CREATE TEMPORARY TABLE tmp_tid_name (tid INT NOT NULL, name VARCHAR(255) NOT NULL)'))
    # 啥条件都没限定的查询
    if course is None and teacher is None and not only_not_full and not only_selected:
        distinct_tid_result = await shard_conn.execute(text('SELECT DISTINCT tid FROM teach'))
        table_name = 'teach'
    else:
        await shard_conn.execute(text('CREATE TEMPORARY TABLE temp_cid_tid (cid INT NOT NULL, tid INT NOT NULL, INDEX idx_tid (tid))'))
        params = {}
        join_part: list[str] = []
        where_part: list[str] = []
        if type(course) is int:
            where_part.append('c.id = :cid')
            params['cid'] = course
        elif type(course) is str:
            where_part.append("c.name LIKE CONCAT('%', :name, '%')")
            params['name'] = course
        if type(teacher) is int:
            join_part.append('JOIN teach t ON c.id = t.cid')
            where_part.append('t.tid = :tid')
            params['tid'] = teacher
        elif type(teacher) is str:
            join_part.append('JOIN teach t ON c.id = t.cid')
            where_part.append('t.tid IN :tid')
            result = await master_slave_conn.execute(text('SELECT id FROM teacher WHERE name = :name'), {'name': teacher})
            params['tid'] = result.scalars().all()
        if only_not_full:
            where_part.append('c.capacity > c.num_selected')
        if only_selected:
            join_part.append('JOIN learn l ON c.id = l.cid')
            where_part.append('l.sid = :sid')
            params['sid'] = stu_id
        await shard_conn.execute(text(f'INSERT INTO temp_cid_tid SELECT tmp.id, t.tid FROM (course c {" ".join(join_part)} WHERE {" AND ".join(where_part)}) tmp JOIN teach t ON tmp.id = t.cid'), params)
        distinct_tid_result = await shard_conn.execute(text('SELECT DISTINCT tid FROM temp_cid_tid'))
        table_name = 'temp_cid_tid'
    await master_slave_conn.execute(text('INSERT INTO tmp_tid(tid) VALUES (:tid)'), [{'tid': tid} for tid in distinct_tid_result.scalars().all()])
    result = await master_slave_conn.execute(text('SELECT t.id, t.name FROM tmp_tid tmp JOIN teacher t ON t.id = tmp.tid'))
    await shard_conn.execute(text('INSERT INTO tmp_tid_name (tid, name) VALUES (:tid, :name)'), [{'tid': row[0], 'name': row[1]} for row in result.all()])
    result = await shard_conn.execute(text("SELECT c.id, GROUP_CONCAT(tmp.name, ', ') AS teachers, c.name, c.capacity, c.num_selected, c.campus, CASE "
                                               'WHEN l.sid IS NULL THEN false '
                                               'ELSE true AS is_selected '
                                           'FROM course c '
                                           f'JOIN {table_name} t ON c.id = t.cid '
                                           'JOIN tmp_tid_name tmp ON t.tid = tmp.tid '
                                           "LEFT JOIN learn l ON l.sid = :sid AND tmp1.id = l.cid "
                                           'GROUP BY c.id'), {'sid': stu_id})
    resp_result = [CourseStudentResp(cid=row[0], teachers=row[1], name=row[2], capacity=row[3], num_selected=row[4], campus=row[5], is_selected=row[6]) for row in result.all()]
    return CourseStudentQueryResp(total=len(resp_result), result=resp_result)
    # await shard_conn.execute(text(
    #     'INSERT INTO temp_result '
    #     'SELECT tmp.id, t.tid FROM ('
    #         'SELECT c.id FROM course c '
    #         'JOIN teach t ON c.id = t.cid '         # teacher?
    #         'JOIN learn l ON c.id = l.cid '         # only_selected?
    #         'WHERE l.sid = ? '                      # only_selected?
    #         "AND t.tid IN ? "                       # teacher?
    #         "AND c.name LIKE CONCAT('%', ?, '%') "  # course?
    #         "AND c.capacity > c.num_selected"       # only_not_full?
    #     ') tmp '
    #     'JOIN teach t ON tmp.id = t.cid'
    # ))


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
    课程创建路由函数。若课程校区就在本地，可直接原地调用该函数
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
    return CourseCreateResp(cid=new_id)


@router.delete('/courses/{course_id}', status_code=204)
async def delete_course(shard_conn: ShardConn, course_id: int):
    """
    课程删除路由函数。若课程校区就在本地，可直接原地调用该函数
    :param shard_conn: 本地分片库连接
    :param course_id: 课程id
    :return:
    """
    await shard_conn.execute(text('DELETE FROM course WHERE id = :cid'), {'cid': course_id})


@router.put('/courses/{course_id}', status_code=204)
async def update_course(shard_conn: ShardConn, course_id: int, p: CourseUpdateParams):
    """
    课程更新路由函数。若课程校区就在本地，可直接原地调用该函数
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
