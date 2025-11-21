import asyncio
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette.responses import JSONResponse

from app.models.course_model import CourseQueryResp, CourseStudentQueryResp, CourseCreateParams, CourseCreateResp, \
    CourseUpdateParams
from app.models.generic_error import GenericError
from app.models.user_model import CurUser
from app.routers import db_router
from app.utils.auth import get_current_user, get_current_admin
from app.utils.database import get_master_slave_connection_no_tx, get_master_slave_connection, \
    get_shard_connection_no_tx, get_shard_connection
from app.utils.remote_call import remote_call
from app.utils.settings import settings

MasterSlaveConnNoTxDep = Annotated[AsyncConnection, Depends(get_master_slave_connection_no_tx)]
MasterSlaveConnDep = Annotated[AsyncConnection, Depends(get_master_slave_connection)]
ShardConnNoTxDep = Annotated[AsyncConnection, Depends(get_shard_connection_no_tx)]
ShardConnDep = Annotated[AsyncConnection, Depends(get_shard_connection)]
CurUserDep = Annotated[CurUser, Depends(get_current_user)]
AdminDep = Annotated[CurUser, Depends(get_current_admin)]


router = APIRouter(
    prefix='/api/v1/courses',
    tags=['Course API'],
    responses={403: {'model': GenericError, 'description': 'Insufficient permission'}}
)


def get_course_campus(course_id: int) -> str:
    course_campus = course_id // 10000
    if course_campus == 10:
        return 'A'
    if course_campus == 11:
        return 'B'
    return 'C'


@router.get('/')
async def get_courses(
        cur_user: CurUserDep,
        master_slave_conn: MasterSlaveConnNoTxDep,
        shard_conn: ShardConnDep,
        campus: set[Literal['A', 'B', 'C']] = Query(min_length=1),
        course: int | str | None = None,
        teacher: int | str | None = None,
        only_not_full: bool = False
) -> CourseQueryResp:
    params = {'campus': campus, 'course': course, 'teacher': teacher, 'only_not_full': only_not_full}
    current_campus = settings.current_campus()
    if type(course) is int:
        # 特判课程id查询，因为课程id可以直接得出位于哪个分库
        course_campus = get_course_campus(course)
        if course_campus not in campus:
            return CourseQueryResp(total=0, result=[])
        if course_campus == current_campus:
            return await db_router.get_courses(master_slave_conn, shard_conn, course, teacher, only_not_full)   # 本地
        # 远程
        code, resp = await remote_call(settings.get_campus_web_url(course_campus) + '/api/private/v1/courses', params=params)
        if code is not None and 200 <= code < 300:
            return resp
        return CourseQueryResp(total=0, result=[])
    # 其他情况视情况分配到远程或本地
    tasks = []
    if current_campus in campus:
        tasks.append(db_router.get_courses(master_slave_conn, shard_conn, course, teacher, only_not_full))
        campus.discard(current_campus)
    for c in campus:
        tasks.append(remote_call(settings.get_campus_web_url(c) + '/api/private/v1/courses', params=params))
    results = await asyncio.gather(*tasks)
    final_list = []
    for result in results:
        if type(result) is CourseQueryResp:
            final_list.extend(result.result)
        else:
            code, resp = result
            if code is not None and 200 <= code < 300:
                final_list.extend(resp.result)
    return CourseQueryResp(total=len(final_list), result=final_list)



@router.get('/student')
async def get_courses_student(
        cur_user: CurUserDep,
        master_slave_conn: MasterSlaveConnNoTxDep,
        shard_conn: ShardConnDep,
        campus: set[Literal['A', 'B', 'C']] = Query(min_length=1),
        course: int | str | None = None,
        teacher: int | str | None = None,
        only_not_full: bool = False,
        only_selected: bool = False,
) -> CourseStudentQueryResp:
    params = {'stu_id': cur_user.uid, 'campus': campus, 'course': course, 'teacher': teacher, 'only_not_full': only_not_full, 'only_selected': only_selected}
    current_campus = settings.current_campus()
    if type(course) is int:
        # 特判课程id查询，因为课程id可以直接得出位于哪个分库
        course_campus = get_course_campus(course)
        if course_campus not in campus:
            return CourseStudentQueryResp(total=0, result=[])
        if course_campus == current_campus:
            return await db_router.get_courses_student(master_slave_conn, shard_conn, cur_user.uid, course, teacher, only_not_full, only_selected)   # 本地
        # 远程
        code, resp = await remote_call(settings.get_campus_web_url(course_campus) + '/api/private/v1/courses/student', params=params)
        if code is not None and 200 <= code < 300:
            return resp
        return CourseStudentQueryResp(total=0, result=[])
    # 其他情况视情况分配到远程或本地
    tasks = []
    if current_campus in campus:
        tasks.append(db_router.get_courses_student(master_slave_conn, shard_conn, cur_user.uid, course, teacher, only_not_full, only_selected))
        campus.discard(current_campus)
    for c in campus:
        tasks.append(remote_call(settings.get_campus_web_url(c) + '/api/private/v1/courses/student', params=params))
    results = await asyncio.gather(*tasks)
    final_list = []
    for result in results:
        if type(result) is CourseQueryResp:
            final_list.extend(result.result)
        else:
            code, resp = result
            if code is not None and 200 <= code < 300:
                final_list.extend(resp.result)
    return CourseStudentQueryResp(total=len(final_list), result=final_list)


@router.post('/', status_code=201)
async def create_course(cur_user: AdminDep, master_slave_conn: MasterSlaveConnDep, shard_conn: ShardConnDep, p: CourseCreateParams) -> CourseCreateResp:
    if p.campus == settings.current_campus():
        return await db_router.create_course(master_slave_conn, shard_conn, p)
    code, resp = await remote_call(settings.get_campus_web_url(p.campus) + '/api/private/v1/courses', method='POST', json=p)
    return JSONResponse(status_code=code, content=resp)


@router.delete('/{course_id}', status_code=204)
async def delete_course(cur_user: AdminDep, shard_conn: ShardConnDep, course_id: int):
    course_campus = get_course_campus(course_id)
    if course_campus == settings.current_campus():
        return await db_router.delete_course(shard_conn, course_id)
    code, resp = await remote_call(settings.get_campus_web_url(course_campus) + f'/api/private/v1/courses/{course_id}', method='DELETE')
    return JSONResponse(status_code=code, content=resp)


@router.put('/{course_id}', status_code=204)
async def update_course(cur_user: AdminDep, shard_conn: ShardConnDep, course_id: int, p: CourseUpdateParams):
    course_campus = get_course_campus(course_id)
    if course_campus == settings.current_campus():
        return await db_router.update_course(shard_conn, course_id, p)
    code, resp = await remote_call(settings.get_campus_web_url(course_campus) + f'/api/private/v1/courses/{course_id}', method='PUT', json=p)
    return JSONResponse(status_code=code, content=resp)
