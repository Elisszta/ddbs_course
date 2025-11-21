from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection

from app.models.course_model import CourseCreateParams, CourseUpdateParams, CourseCreateResp, CourseStudentQueryResp, \
    CourseStudentResp, CourseResp, CourseQueryResp
from app.models.generic_error import GenericError, err_course_cap_conflict, err_course_not_exist, \
    err_course_id_conflict, err_course_id_full, err_teacher_not_exist, err_student_not_exist
from app.models.user_model import StudentQueryResp, StudentResp
from app.utils.auth import verify_db_api
from app.utils.database import get_master_slave_connection, get_shard_connection, get_master_slave_connection_no_tx, \
    get_shard_connection_no_tx
from app.utils.settings import settings

MasterSlaveConnNoTxDep = Annotated[AsyncConnection, Depends(get_master_slave_connection_no_tx)]
MasterSlaveConnDep = Annotated[AsyncConnection, Depends(get_master_slave_connection)]
ShardConnNoTxDep = Annotated[AsyncConnection, Depends(get_shard_connection_no_tx)]
ShardConnDep = Annotated[AsyncConnection, Depends(get_shard_connection)]


router = APIRouter(
    prefix='/api/private/v1',
    tags=['DB Cross Site API'],
    responses={403: {'model': GenericError, 'description': 'Insufficient permission'}},
    dependencies=(Depends(verify_db_api),)
)


# 删用户，要把teach表或learn表相关条目删了，如果是learn表的，对应课程已选人数要减少
@router.delete('/users/{user_id}', status_code=204)
async def delete_user(shard_conn: ShardConnNoTxDep, user_id: int):
    """
    用户删除分库路由函数。删除用户时必须原地调用+所有远程http调用
    :param shard_conn: 本地分片库连接，不自动事务
    :param user_id: 用户id
    :return:
    """
    if user_id >= 1200000000:
        # teacher
        async with shard_conn.begin():
            await shard_conn.execute(text('DELETE FROM teach WHERE tid = :tid'), {'tid': user_id})
            return
    # student
    # 有点邪恶了，循环事务测试以防止并发插入（默认隔离级别会有幻读）
    while True:
        async with shard_conn.begin():
            course_ids = (await shard_conn.execute(text('SELECT cid FROM learn WHERE sid = :sid FOR UPDATE'), {'sid': user_id})).scalars().all()
            for course_id in course_ids:
                num_selected = (await shard_conn.execute(text('SELECT num_selected FROM course WHERE id = :id FOR UPDATE'), {'id': course_id})).scalar()
                if num_selected is not None and num_selected > 0:
                    await shard_conn.execute(text('UPDATE course SET num_selected = :num WHERE id = :id'), {'num': num_selected - 1, 'id': course_id})
            if (await shard_conn.execute(text('DELETE FROM learn WHERE sid = :sid'), {'sid': user_id})).rowcount == 0:
                return


@router.post('/users/{stu_id}/select', status_code=204)
async def select_course(master_slave_conn: MasterSlaveConnDep, shard_conn: ShardConnDep, stu_id: int, course_id: int):
    """
    选课分库路由函数。若课程校区就在本地，可直接原地调用该函数
    :param master_slave_conn: 本地主从库连接
    :param shard_conn: 本地分片库连接
    :param course_id: 课程id
    :param stu_id: 学生id
    :return:
    """
    if (await master_slave_conn.execute(text('SELECT 1 FROM student WHERE id = :id'), {'id': stu_id})).scalar() is None:
        raise HTTPException(status_code=404, detail=err_student_not_exist)  # 学生不存在
    row = (await shard_conn.execute(text('SELECT num_selected, capacity FROM course WHERE id = :id FOR UPDATE'), {'id': course_id})).one_or_none()  # 表锁
    if row is None:
        raise HTTPException(status_code=404, detail=err_course_not_exist)   # 课程不存在
    if row[0] >= row[1]:
        raise HTTPException(status_code=409, detail=err_course_cap_conflict)    # 课程已满
    await shard_conn.execute(text('UPDATE course SET num_selected = :num WHERE id = :id'), {'num': row[0] + 1, 'id': course_id})
    await shard_conn.execute(text('INSERT INTO learn(cid, sid) VALUES (:cid, :sid)'), {'cid': course_id, 'sid': stu_id})


@router.post('/users/{std_id}/deselect', status_code=204)
async def deselect_course(master_slave_conn: MasterSlaveConnDep, shard_conn: ShardConnDep, stu_id: int, course_id: int):
    """
    退课分库路由函数。若课程校区就在本地，可直接原地调用该函数
    :param master_slave_conn: 本地主从库连接
    :param shard_conn: 本地分片库连接
    :param course_id: 课程id
    :param stu_id: 学生id
    :return:
    """
    if (await master_slave_conn.execute(text('SELECT 1 FROM student WHERE id = :id'), {'id': stu_id})).scalar() is None:
        raise HTTPException(status_code=404, detail=err_student_not_exist)  # 学生不存在
    num_selected = (await shard_conn.execute(text('SELECT num_selected FROM course WHERE id = :id FOR UPDATE'), {'id': course_id})).scalar() # 表锁
    if num_selected is None:
        raise HTTPException(status_code=404, detail=err_course_not_exist)  # 课程不存在
    await shard_conn.execute(text('UPDATE course SET num_selected = :num WHERE id = :id'), {'num': num_selected - 1, 'id': course_id})
    await shard_conn.execute(text('DELETE FROM learn WHERE cid = :cid AND sid = :sid'), {'cid': course_id, 'sid': stu_id})


@router.get('/courses/{course_id}/students')
async def get_course_students(master_slave_conn: MasterSlaveConnNoTxDep, shard_conn: ShardConnDep, course_id: int) -> StudentQueryResp:
    """
    查课程学生分库路由函数。若课程校区就在本地，可直接原地调用该函数
    :param master_slave_conn: 本地主从库连接，不自动事务
    :param shard_conn: 本地分片库连接
    :param course_id: 课程id
    :return: 学生查询结果
    """
    if (await shard_conn.execute(text('SELECT 1 FROM course WHERE id = :id'), {'id': course_id})).scalar() is None:
        raise HTTPException(status_code=404, detail=err_course_not_exist)
    # 直接把学生id发过去连接
    await master_slave_conn.execute(text('CREATE TEMPORARY TABLE tmp_sid (sid INT NOT NULL)'))  # tmp_sid是驱动表，所以就不建索引了
    student_ids = (await shard_conn.execute(text('SELECT DISTINCT sid FROM learn WHERE cid = :cid'), {'cid': course_id})).scalars().all()
    await master_slave_conn.execute(text('INSERT INTO tmp_sid(sid) VALUES (:sid)'), [{'sid': student_id} for student_id in student_ids])
    students = (await master_slave_conn.execute(text('SELECT s.id, s.name, s.sex, s.age, s.current_campus FROM student s JOIN tmp_sid tmp ON s.id = tmp.sid'))).all()
    resp_result = [StudentResp(stu_id=row[0], name=row[1], sex=row[2], age=row[3], current_campus=row[4]) for row in students]
    return StudentQueryResp(total=len(resp_result), result=resp_result)


async def build_course_filter_sql(master_slave_conn: AsyncConnection, course: int | str | None, teacher: int | str | None, only_not_full: bool, stu_id: int | None = None, only_selected: bool = False) -> tuple[str | None, str | None, dict | None]:
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
        teacher_ids = (await master_slave_conn.execute(text('SELECT id FROM teacher WHERE name = :name'), {'name': teacher})).scalars().all()
        if len(teacher_ids) == 0:
            # 没有符合条件的教师，没有必要进行后续的查询了
            return None, None, None
        join_part.append('JOIN teach t ON c.id = t.cid')
        where_part.append(f"t.tid IN ({','.join([f':tid{i}' for i in range(len(teacher_ids))])})")
        for i in range(len(teacher_ids)):
            params[f'tid{i}'] = teacher_ids[i]
    if only_not_full:
        where_part.append('c.capacity > c.num_selected')
    if only_selected:
        join_part.append('JOIN learn l ON c.id = l.cid')
        where_part.append('l.sid = :sid')
        params['sid'] = stu_id
    return ' '.join(join_part), ' AND '.join(where_part), params


@router.get('/courses')
async def get_courses(
        master_slave_conn: MasterSlaveConnNoTxDep,
        shard_conn: ShardConnDep,
        course: int | str | None = None,
        teacher: int | str | None = None,
        only_not_full: bool = False
) -> CourseQueryResp:
    """
    教师管理员课程查询分库路由函数。若课程校区就在本地，可直接原地调用该函数
    :param master_slave_conn: 本地主从库连接，不自动事务
    :param shard_conn: 本地分片库连接
    :param course: 课程id或课程关键词或空
    :param teacher: 教师id或教师名或空
    :param only_not_full: 是否只查询未满
    :return: 课程查询结果
    """
    # 由于主从复制gtid临时表限制，这里的master_slave_conn必须手动控制事务
    await master_slave_conn.execute(text('CREATE TEMPORARY TABLE tmp_tid (tid INT NOT NULL)'))  # 可以保证从分片库导过来的tid条数小于主从库的教师表条数，所以temp_tid是驱动表，所以就不建索引了
    await shard_conn.execute(text('CREATE TEMPORARY TABLE tmp_tid_name (tid INT NOT NULL, name VARCHAR(255) NOT NULL)'))
    # 使用半连接策略
    # 啥条件都没限定的查询
    if course is None and teacher is None and not only_not_full:
        distinct_teacher_ids = (await shard_conn.execute(text('SELECT DISTINCT tid FROM teach'))).scalars().all()
        table_name = 'teach'
    else:
        join_sql, where_sql, params = build_course_filter_sql(master_slave_conn, course, teacher, only_not_full)
        if params is None:
            return CourseQueryResp(total=0, result=[])
        await shard_conn.execute(text('CREATE TEMPORARY TABLE tmp_cid_tid (cid INT NOT NULL, tid INT NOT NULL, INDEX idx_tid (tid))'))
        await shard_conn.execute(text(f'INSERT INTO tmp_cid_tid SELECT tmp.id, t.tid FROM (course c {join_sql} WHERE {where_sql}) tmp JOIN teach t ON tmp.id = t.cid'), params)
        distinct_teacher_ids = (await shard_conn.execute(text('SELECT DISTINCT tid FROM tmp_cid_tid'))).scalars().all()
        table_name = 'tmp_cid_tid'
    await master_slave_conn.execute(text('INSERT INTO tmp_tid(tid) VALUES (:tid)'), [{'tid': teacher_id} for teacher_id in distinct_teacher_ids])
    result = await master_slave_conn.execute(text('SELECT t.id, t.name FROM tmp_tid tmp JOIN teacher t ON t.id = tmp.tid'))
    await shard_conn.execute(text('INSERT INTO tmp_tid_name (tid, name) VALUES (:tid, :name)'), [{'tid': row[0], 'name': row[1]} for row in result.all()])
    result = await shard_conn.execute(text("SELECT c.id, GROUP_CONCAT(tmp.name, ', ') AS teachers, c.name, c.capacity, c.num_selected, c.campus FROM course c "
                                           f'JOIN {table_name} t ON c.id = t.cid '
                                           'JOIN tmp_tid_name tmp ON t.tid = tmp.tid '
                                           'GROUP BY c.id'))
    resp_result = [CourseResp(course_id=row[0], teachers=row[1], name=row[2], capacity=row[3], num_selected=row[4], campus=row[5]) for row in result.all()]
    return CourseQueryResp(total=len(resp_result), result=resp_result)


@router.get('/courses/student')
async def get_courses_student(
        master_slave_conn: MasterSlaveConnNoTxDep,
        shard_conn: ShardConnDep,
        stu_id: int,
        course: int | str | None = None,
        teacher: int | str | None = None,
        only_not_full: bool = False,
        only_selected: bool = False,
) -> CourseStudentQueryResp:
    """
    学生课程查询分库路由函数。若课程校区就在本地，可直接原地调用该函数
    :param master_slave_conn: 本地主从库连接，不自动事务
    :param shard_conn: 本地分片库连接
    :param stu_id: 学生id
    :param course: 课程id或课程关键词或空
    :param teacher: 教师id或教师名或空
    :param only_not_full: 是否只查询未满
    :param only_selected: 是否只查询已选
    :return: 课程查询结果
    """
    # 由于主从复制gtid临时表限制，这里的master_slave_conn必须手动控制事务
    await master_slave_conn.execute(text('CREATE TEMPORARY TABLE tmp_tid (tid INT NOT NULL)'))  # 可以保证从分片库导过来的tid条数小于主从库的教师表条数，所以temp_tid是驱动表，所以就不建索引了
    await shard_conn.execute(text('CREATE TEMPORARY TABLE tmp_tid_name (tid INT NOT NULL, name VARCHAR(255) NOT NULL)'))
    # 使用半连接策略
    # 啥条件都没限定的查询
    if course is None and teacher is None and not only_not_full and not only_selected:
        distinct_teachers_id = (await shard_conn.execute(text('SELECT DISTINCT tid FROM teach'))).scalars().all()
        table_name = 'teach'
    else:
        join_sql, where_sql, params = build_course_filter_sql(master_slave_conn, course, teacher, only_not_full, stu_id, only_selected)
        if params is None:
            return CourseStudentQueryResp(total=0, result=[])
        await shard_conn.execute(text('CREATE TEMPORARY TABLE tmp_cid_tid (cid INT NOT NULL, tid INT NOT NULL, INDEX idx_tid (tid))'))
        await shard_conn.execute(text(f'INSERT INTO tmp_cid_tid SELECT tmp.id, t.tid FROM (course c {join_sql} WHERE {where_sql}) tmp JOIN teach t ON tmp.id = t.cid'), params)
        distinct_teachers_id = (await shard_conn.execute(text('SELECT DISTINCT tid FROM tmp_cid_tid'))).scalars().all()
        table_name = 'tmp_cid_tid'
    await master_slave_conn.execute(text('INSERT INTO tmp_tid(tid) VALUES (:tid)'), [{'tid': teacher_id} for teacher_id in distinct_teachers_id])
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
    resp_result = [CourseStudentResp(course_id=row[0], teachers=row[1], name=row[2], capacity=row[3], num_selected=row[4], campus=row[5], is_selected=row[6]) for row in result.all()]
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


async def gen_course_id(shard_conn: ShardConnDep) -> int | None:
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
async def create_course(master_slave_conn: MasterSlaveConnDep, shard_conn: ShardConnDep, p: CourseCreateParams) -> CourseCreateResp:
    """
    课程创建分库路由函数。若课程校区就在本地，可直接原地调用该函数
    :param master_slave_conn: 本地主从库连接
    :param shard_conn: 本地分片库连接
    :param p: 课程创建参数
    :return:
    """
    # 检查教师是否存在
    if (await master_slave_conn.execute(text('SELECT COUNT(*) FROM teacher WHERE id IN :ids'), {'ids': p.teacher_ids})).scalar() != len(p.teacher_ids):
        raise HTTPException(status_code=404, detail=err_teacher_not_exist)
    # 生成id
    # 无锁，如果真的有并发插入导致id重了，那就返回409让用户重试呗
    new_id = await gen_course_id(shard_conn)
    if new_id is None:
        raise HTTPException(status_code=409, detail=err_course_id_full)
    # 插入课程
    try:
        await shard_conn.execute(text('INSERT INTO course(id, name, capacity, num_selected, campus) VALUES (:id, :name, :capacity, :num_selected, :campus)'), {
            'id': new_id,
            'name': p.name,
            'capacity': p.capacity,
            'num_selected': 0,
            'campus': p.campus,
        })
    except IntegrityError:
        raise HTTPException(status_code=409, detail=err_course_id_conflict)
    # 插入教学
    await shard_conn.execute(text('INSERT INTO teach(tid, cid) VALUES (:tid, :cid)'), [{'tid': teacher_id, 'cid': new_id} for teacher_id in p.teacher_ids])
    return CourseCreateResp(course_id=new_id)


@router.delete('/courses/{course_id}', status_code=204)
async def delete_course(shard_conn: ShardConnDep, course_id: int):
    """
    课程删除分库路由函数。若课程校区就在本地，可直接原地调用该函数
    :param shard_conn: 本地分片库连接
    :param course_id: 课程id
    :return:
    """
    await shard_conn.execute(text('DELETE FROM course WHERE id = :id'), {'id': course_id})


@router.put('/courses/{course_id}', status_code=204)
async def update_course(shard_conn: ShardConnDep, course_id: int, p: CourseUpdateParams):
    """
    课程更新分库路由函数。若课程校区就在本地，可直接原地调用该函数
    :param shard_conn: 本地分片库连接
    :param course_id: 课程id
    :param p: 课程更新参数
    :return:
    """
    num_selected = (await shard_conn.execute(text('SELECT num_selected FROM course WHERE id = :cid FOR UPDATE'), {'cid': course_id})).scalar()  # 行级锁启动
    if num_selected is None:
        raise HTTPException(status_code=404, detail=err_course_not_exist)
    if p.capacity < num_selected:
        raise HTTPException(status_code=409, detail=err_course_cap_conflict)
    await shard_conn.execute(text('UPDATE course SET name = :name, capacity = :capacity WHERE id = :id'), {'name': p.name, 'capacity': p.capacity, 'id': course_id})
    await shard_conn.execute(text('DELETE FROM teach WHERE id = :id'), {'id': course_id})
    await shard_conn.execute(text('INSERT INTO teach(tid, cid) VALUES (:tid, :cid)'), [{'tid': teacher_id, 'cid': course_id} for teacher_id in p.teacher_ids])
