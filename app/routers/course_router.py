import asyncio
from string import Template
from typing import Annotated, Literal, Callable, Coroutine, Any

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette.responses import JSONResponse

from app.models.course_model import CourseQueryResp, CourseCreateParams, CourseCreateResp, CourseUpdateParams
from app.models.generic_error import GenericError, err_no_permission, err_selection_time
from app.models.user_login_model import CurUser
from app.models.student_model import StudentQueryResp
from app.routers.dbprivate.shard_router import query_courses_private, create_course_private, delete_course_private, \
    update_course_private, get_course_students_private, select_course_private, deselect_course_private
from app.utils.auth import UserDep, AdminDep, AdminTeacherDep
from app.utils.classify_helper import get_course_campus
from app.utils.database import get_master_slave_connection, get_shard_connection
from app.utils.remote_call import remote_db_call
from app.settings import settings


MasterSlaveConnDep = Annotated[AsyncConnection, Depends(get_master_slave_connection)]
ShardConnDep = Annotated[AsyncConnection, Depends(get_shard_connection)]


router = APIRouter(
    prefix='/api/v1/courses',
    tags=['Course API'],
    responses={403: {'model': GenericError, 'description': 'Insufficient permission'}}
)


def get_query_local_task(
        cur_user: UserDep,
        master_slave_conn: MasterSlaveConnDep,
        shard_conn: ShardConnDep,
        course: int | str | None = None,
        teacher: int | str | None = None,
        only_not_full: bool | None = None,
        only_selected: bool | None = None,
) -> Coroutine[Any, Any, CourseQueryResp]:
    if cur_user.role == 'student':
        return query_courses_private(master_slave_conn, shard_conn, course, teacher, only_not_full, only_selected, cur_user.user_id)
    return query_courses_private(master_slave_conn, shard_conn, course, teacher, only_not_full, only_selected)


def get_query_remote_task(
        cur_user: UserDep,
        campus: str,
        course: int | str | None = None,
        teacher: int | str | None = None,
        only_not_full: bool | None = None,
        only_selected: bool | None = None,
) -> Coroutine[Any, Any, tuple[int, Any] | tuple[None, str]]:
    params = {}
    if cur_user.role == 'student':
        params['stu_id'] = cur_user.user_id
    if course is not None:
        params['course'] = course
    if teacher is not None:
        params['teacher'] = teacher
    if only_not_full is not None:
        params['only_not_full'] = str(only_not_full)
    if only_selected is not None:
        params['only_selected'] = str(only_selected)
    return remote_db_call(settings.get_campus_web_url(campus) + '/api-private/v1/courses', params=params)


@router.get('')
async def query_courses(
        cur_user: UserDep,
        master_slave_conn: MasterSlaveConnDep,
        shard_conn: ShardConnDep,
        campus: set[Literal['A', 'B', 'C']] = Query(min_length=1),
        course: int | str | None = None,
        teacher: int | str | None = None,
        only_not_full: bool | None = None,
        only_selected: bool | None = None,
) -> CourseQueryResp:
    # 傻逼fastapi，怎么参数 int | str 永远返回str
    if type(course) == str:
        try: course = int(course)
        except: pass
    if type(teacher) == str:
        try: teacher = int(teacher)
        except: pass
    current_campus = settings.current_campus()
    if type(course) == int:
        # 特判课程id查询，因为课程id可以直接得出位于哪个分库
        course_campus = get_course_campus(course)
        if course_campus not in campus:
            return CourseQueryResp(total=0, result=[])
        if course_campus == current_campus:
            return await get_query_local_task(cur_user, master_slave_conn, shard_conn, course, teacher, only_not_full, only_selected) # 本地
        # 远程
        code, resp = await get_query_remote_task(cur_user, course_campus, course, teacher, only_not_full, only_selected)
        if code == 200:
            return resp
        return CourseQueryResp(total=0, result=[])
    # 其他情况视情况分配到远程或本地
    tasks = []
    if current_campus in campus:
        tasks.append(get_query_local_task(cur_user, master_slave_conn, shard_conn, course, teacher, only_not_full, only_selected))
        campus.discard(current_campus)
    for c in campus:
        tasks.append(get_query_remote_task(cur_user, c, course, teacher, only_not_full, only_selected))
    final_list = []
    for task_result in await asyncio.gather(*tasks):
        if type(task_result) is CourseQueryResp:
            final_list.extend(task_result.result)
        else:
            code, resp = task_result
            if code == 200:
                final_list.extend(resp['result'])
    return CourseQueryResp(total=len(final_list), result=final_list)


@router.post('', status_code=201, responses={
    404: {'model': GenericError, 'description': 'Teacher does not exist'},
    409: {'model': GenericError, 'description': 'Course id conflict or full'},
    502: {'model': GenericError, 'description': 'Remote not responding'}
})
async def create_course(cur_user: AdminDep, master_slave_conn: MasterSlaveConnDep, shard_conn: ShardConnDep, p: CourseCreateParams) -> CourseCreateResp:
    if p.campus == settings.current_campus():
        return await create_course_private(master_slave_conn, shard_conn, p)
    json_dict = p.model_dump()
    json_dict['teacher_ids'] = list(json_dict['teacher_ids'])
    code, resp = await remote_db_call(settings.get_campus_web_url(p.campus) + '/api-private/v1/courses', method='POST', json=json_dict)
    if code != 201:
        detail = resp.get('detail') if isinstance(resp, dict) else str(resp)
        raise HTTPException(status_code=code or 502, detail=detail)
    return JSONResponse(status_code=code, content=resp)


@router.delete('/{course_id}', status_code=204, responses={502: {'model': GenericError, 'description': 'Remote not responding'}})
async def delete_course(cur_user: AdminDep, shard_conn: ShardConnDep, course_id: int):
    course_campus = get_course_campus(course_id)
    if course_campus == settings.current_campus():
        await delete_course_private(shard_conn, course_id)
        return
    code, resp = await remote_db_call(settings.get_campus_web_url(course_campus) + f'/api-private/v1/courses/{course_id}', method='DELETE')
    if code != 204:
        detail = resp.get('detail') if isinstance(resp, dict) else str(resp)
        raise HTTPException(status_code=code or 502, detail=detail)


@router.put('/{course_id}', status_code=204, responses={
    404: {'model': GenericError, 'description': 'Course or teacher does not exist'},
    409: {'model': GenericError, 'description': 'Course capacity conflict'},
    502: {'model': GenericError, 'description': 'Remote not responding'}
})
async def update_course(cur_user: AdminDep, master_slave_conn: MasterSlaveConnDep, shard_conn: ShardConnDep, p: CourseUpdateParams, course_id: int):
    course_campus = get_course_campus(course_id)
    if course_campus == settings.current_campus():
        await update_course_private(master_slave_conn, shard_conn, course_id, p)
        return
    code, resp = await remote_db_call(settings.get_campus_web_url(course_campus) + f'/api-private/v1/courses/{course_id}', method='PUT', json=p.model_dump())
    if code != 204:
        detail = resp.get('detail') if isinstance(resp, dict) else str(resp)
        raise HTTPException(status_code=code or 502, detail=detail)


@router.get('/{course_id}/students', responses={
    404: {'model': GenericError, 'description': 'Course does not exist'},
    502: {'model': GenericError, 'description': 'Remote not responding'}
})
async def get_course_students(cur_user: AdminTeacherDep, master_slave_conn: MasterSlaveConnDep, shard_conn: ShardConnDep, course_id: int) -> StudentQueryResp:
    course_campus = get_course_campus(course_id)
    if course_campus == settings.current_campus():
        return await get_course_students_private(master_slave_conn, shard_conn, course_id)
    code, resp = await remote_db_call(settings.get_campus_web_url(course_campus) + f'/api-private/v1/courses/{course_id}/students')
    if code != 200:
        detail = resp.get('detail') if isinstance(resp, dict) else str(resp)
        raise HTTPException(status_code=code or 502, detail=detail)
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
        await local_func(master_slave_conn, shard_conn, course_id, stu_id)
        return
    code, resp = await remote_db_call(settings.get_campus_web_url(course_campus) + remote_path.substitute(course_id=course_id), method='POST', params={'stu_id': stu_id})
    if code != 204:
        detail = resp.get('detail') if isinstance(resp, dict) else str(resp)
        raise HTTPException(status_code=code or 502, detail=detail)


@router.post('/{course_id}/select', status_code=204, responses={
    404: {'model': GenericError, 'description': 'Course or student does not exist'},
    409: {'model': GenericError, 'description': 'Course capacity conflict or already selected'},
    502: {'model': GenericError, 'description': 'Remote not responding'}
})
async def select_course(cur_user: UserDep, master_slave_conn: MasterSlaveConnDep, shard_conn: ShardConnDep, course_id: int, stu_id: int | None = None):
    await select_or_deselect_course(cur_user, master_slave_conn, shard_conn, course_id, stu_id, select_course_private, Template('/api-private/v1/courses/${course_id}/select'))


@router.post('/{course_id}/deselect', status_code=204, responses={
    404: {'model': GenericError, 'description': 'Course or student does not exist'},
    502: {'model': GenericError, 'description': 'Remote not responding'}
})
async def deselect_course(cur_user: UserDep, master_slave_conn: MasterSlaveConnDep, shard_conn: ShardConnDep, course_id: int, stu_id: int | None = None):
    await select_or_deselect_course(cur_user, master_slave_conn, shard_conn, course_id, stu_id, deselect_course_private, Template('/api-private/v1/courses/${course_id}/deselect'))
