import asyncio
from string import Template
from typing import Annotated, Literal, Callable, Coroutine, Any

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette.responses import JSONResponse

from app.models.course_model import CourseQueryResp, CourseCreateParams, CourseCreateResp, CourseUpdateParams
from app.models.generic_error import GenericError, err_no_permission, err_selection_time, err_bad_gateway
from app.models.user_model import CurUser, StudentQueryResp
from app.routers import db_router
from app.utils.auth import get_current_user, get_current_admin, get_current_admin_or_teacher, get_current_student
from app.utils.classify_helper import get_course_campus
from app.utils.database import get_master_slave_connection_no_tx, get_master_slave_connection, \
    get_shard_connection_no_tx, get_shard_connection
from app.utils.remote_call import remote_call
from app.utils.settings import settings


MasterSlaveConnNoTxDep = Annotated[AsyncConnection, Depends(get_master_slave_connection_no_tx)]
MasterSlaveConnDep = Annotated[AsyncConnection, Depends(get_master_slave_connection)]
ShardConnNoTxDep = Annotated[AsyncConnection, Depends(get_shard_connection_no_tx)]
ShardConnDep = Annotated[AsyncConnection, Depends(get_shard_connection)]
UserDep = Annotated[CurUser, Depends(get_current_user)]
AdminDep = Annotated[CurUser, Depends(get_current_admin)]
AdminTeacherDep = Annotated[CurUser, Depends(get_current_admin_or_teacher)]
StudentDep = Annotated[CurUser, Depends(get_current_student)]


router = APIRouter(
    prefix='/api/v1/courses',
    tags=['Course API'],
    responses={403: {'model': GenericError, 'description': 'Insufficient permission'}}
)


@router.get('/')
async def query_courses(
        cur_user: UserDep,
        master_slave_conn: MasterSlaveConnNoTxDep,
        shard_conn: ShardConnDep,
        campus: set[Literal['A', 'B', 'C']] = Query(min_length=1),
        course: int | str | None = None,
        teacher: int | str | None = None,
        only_not_full: bool | None = None,
        only_selected: bool | None = None,
) -> CourseQueryResp:
    if cur_user.role == 'student':
        params = {'course': course, 'teacher': teacher, 'only_not_full': only_not_full, 'only_selected': only_selected, 'stu_id': cur_user.user_id}
        local_task = db_router.query_courses(master_slave_conn, shard_conn, course, teacher, only_not_full, only_selected, cur_user.user_id)
    else:
        params = {'course': course, 'teacher': teacher, 'only_not_full': only_not_full}
        local_task = db_router.query_courses(master_slave_conn, shard_conn, course, teacher, only_not_full)
    current_campus = settings.current_campus()
    if type(course) is int:
        # 特判课程id查询，因为课程id可以直接得出位于哪个分库
        course_campus = get_course_campus(course)
        if course_campus not in campus:
            return CourseQueryResp(total=0, result=[])
        if course_campus == current_campus:
            return await local_task # 本地
        # 远程
        code, resp = await remote_call(settings.get_campus_web_url(course_campus) + '/api-private/v1/courses', params=params)
        if code is not None and 200 <= code < 300:
            return resp
        return CourseQueryResp(total=0, result=[])
    # 其他情况视情况分配到远程或本地
    tasks = []
    if current_campus in campus:
        tasks.append(local_task)
        campus.discard(current_campus)
    for c in campus:
        tasks.append(remote_call(settings.get_campus_web_url(c) + '/api-private/v1/courses', params=params))
    final_list = []
    for task_result in await asyncio.gather(*tasks):
        if type(task_result) is CourseQueryResp:
            final_list.extend(task_result.result)
        else:
            code, resp = task_result
            if code is not None and 200 <= code < 300:
                final_list.extend(resp.result)
    return CourseQueryResp(total=len(final_list), result=final_list)


@router.post('/', status_code=201, responses={
    404: {'model': GenericError, 'description': 'Teacher does not exist'},
    409: {'model': GenericError, 'description': 'Course id conflict or full'},
    502: {'model': GenericError, 'description': 'Remote not responding'}
})
async def create_course(cur_user: AdminDep, master_slave_conn: MasterSlaveConnDep, shard_conn: ShardConnDep, p: CourseCreateParams) -> CourseCreateResp:
    if p.campus == settings.current_campus():
        return await db_router.create_course(master_slave_conn, shard_conn, p)
    code, resp = await remote_call(settings.get_campus_web_url(p.campus) + '/api-private/v1/courses', method='POST', json=p)
    if code is None:
        raise HTTPException(status_code=502, detail=err_bad_gateway)
    return JSONResponse(status_code=code, content=resp)


@router.delete('/{course_id}', status_code=204, responses={502: {'model': GenericError, 'description': 'Remote not responding'}})
async def delete_course(cur_user: AdminDep, shard_conn: ShardConnDep, course_id: int):
    course_campus = get_course_campus(course_id)
    if course_campus == settings.current_campus():
        return await db_router.delete_course(shard_conn, course_id)
    code, resp = await remote_call(settings.get_campus_web_url(course_campus) + f'/api-private/v1/courses/{course_id}', method='DELETE')
    if code is None:
        raise HTTPException(status_code=502, detail=err_bad_gateway)
    return JSONResponse(status_code=code, content=resp)


@router.put('/{course_id}', status_code=204, responses={
    404: {'model': GenericError, 'description': 'Course or teacher does not exist'},
    409: {'model': GenericError, 'description': 'Course capacity conflict'},
    502: {'model': GenericError, 'description': 'Remote not responding'}
})
async def update_course(cur_user: AdminDep, master_slave_conn: MasterSlaveConnDep, shard_conn: ShardConnDep, course_id: int, p: CourseUpdateParams):
    course_campus = get_course_campus(course_id)
    if course_campus == settings.current_campus():
        return await db_router.update_course(master_slave_conn, shard_conn, course_id, p)
    code, resp = await remote_call(settings.get_campus_web_url(course_campus) + f'/api-private/v1/courses/{course_id}', method='PUT', json=p)
    if code is None:
        raise HTTPException(status_code=502, detail=err_bad_gateway)
    return JSONResponse(status_code=code, content=resp)


@router.get('/{course_id}/students', responses={
    404: {'model': GenericError, 'description': 'Course does not exist'},
    502: {'model': GenericError, 'description': 'Remote not responding'}
})
async def get_course_students(cur_user: AdminTeacherDep, master_slave_conn: MasterSlaveConnNoTxDep, shard_conn: ShardConnDep, course_id: int) -> StudentQueryResp:
    course_campus = get_course_campus(course_id)
    if course_campus == settings.current_campus():
        return await db_router.get_course_students(master_slave_conn, shard_conn, course_id)
    code, resp = await remote_call(settings.get_campus_web_url(course_campus) + f'/api-private/v1/courses/{course_id}/students')
    if code is None:
        raise HTTPException(status_code=502, detail=err_bad_gateway)
    return JSONResponse(status_code=code, content=resp)


async def select_or_deselect_course(
        cur_user: CurUser,
        master_slave_conn: AsyncConnection,
        shard_conn: AsyncConnection,
        course_id: int,
        stu_id: int | None,
        local_func: Callable[[AsyncConnection, AsyncConnection, int, int], Coroutine[Any, Any, None]],
        remote_path: Template
):
    # stu_id参数为空，表示学生选退课，id从cur_user获取
    # stu_id非空，表示管理员帮学生选退课，id从stu_id获取
    if stu_id is None:
        if cur_user.role != 'student':
            raise HTTPException(status_code=403, detail=err_no_permission)
        stu_id = cur_user.user_id
    elif cur_user.role != 'admin':
        raise HTTPException(status_code=403, detail=err_no_permission)
    # 学生选课检查选课时段
    if cur_user.role == 'student' and (await master_slave_conn.execute(text('SELECT 1 FROM selection_batch WHERE NOW() BETWEEN begin_time AND end_time'))).scalar() is None:
        raise HTTPException(status_code=403, detail=err_selection_time)
    course_campus = get_course_campus(course_id)
    if course_campus == settings.current_campus():
        return await local_func(master_slave_conn, shard_conn, course_id, stu_id)
    code, resp = await remote_call(settings.get_campus_web_url(course_campus) + remote_path.substitute(course_id=course_id), method='POST', params={'stu_id': stu_id})
    if code is None:
        raise HTTPException(status_code=502, detail=err_bad_gateway)
    return JSONResponse(status_code=code, content=resp)


@router.post('/{course_id}/select', status_code=204, responses={
    404: {'model': GenericError, 'description': 'Course or student does not exist'},
    409: {'model': GenericError, 'description': 'Course capacity conflict'},
    502: {'model': GenericError, 'description': 'Remote not responding'}
})
async def select_course(cur_user: UserDep, master_slave_conn: MasterSlaveConnDep, shard_conn: ShardConnDep, course_id: int, stu_id: int | None = None):
    return select_or_deselect_course(cur_user, master_slave_conn, shard_conn, course_id, stu_id, db_router.select_course, Template('/api-private/v1/courses/${course_id}/select'))


@router.post('/{course_id}/deselect', status_code=204, responses={
    404: {'model': GenericError, 'description': 'Course or student does not exist'},
    502: {'model': GenericError, 'description': 'Remote not responding'}
})
async def deselect_course(cur_user: UserDep, master_slave_conn: MasterSlaveConnDep, shard_conn: ShardConnDep, course_id: int, stu_id: int | None = None):
    return select_or_deselect_course(cur_user, master_slave_conn, shard_conn, course_id, stu_id, db_router.deselect_course, Template('/api-private/v1/courses/${course_id}/deselect'))
