import asyncio
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette.responses import JSONResponse

from app.models.generic_error import GenericError
from app.models.teacher_model import TeacherCreateParams, TeacherUpdateParams, TeacherResp, TeacherQueryResp
from app.routers.dbprivate import master_router
from app.routers.dbprivate.shard_router import delete_user_private
from app.settings import settings
from app.utils.auth import AdminDep
from app.utils.database import get_master_slave_connection, get_shard_connection
from app.utils.remote_call import remote_db_call

# 定义数据库连接依赖
MasterSlaveConnDep = Annotated[AsyncConnection, Depends(get_master_slave_connection)]
ShardConnDep = Annotated[AsyncConnection, Depends(get_shard_connection)]

router = APIRouter(prefix="/api/v1/teachers", tags=["Teacher"], responses={403: {'model': GenericError, 'description': 'Insufficient permission'}})


# =================================================================
# 1. 管理员添加教师 (Master写)
# =================================================================
@router.post("", status_code=201, responses={
    409: {'model': GenericError, 'description': 'Teacher id conflict or full'},
    502: {'model': GenericError, 'description': 'Remote not responding'}
})
async def add_teacher(
    conn: MasterSlaveConnDep, 
    teacher: TeacherCreateParams, 
    user: AdminDep
) -> TeacherResp:
    """
    管理员添加教师接口。
    
    逻辑：教师档案数据必须写入全局主库(Master)。
    - 如果当前节点是 Master：直接执行本地 INSERT SQL。
    - 如果当前节点是 Slave：通过私有接口转发请求给 Master。
    
    :param conn: 主从库连接对象 (MasterSlaveConnDep)。
                 如果是主库，用于直接执行 INSERT SQL。
    :param teacher: 教师创建参数 (TeacherCreateParams)。
                 包含 id, name, sex, age。
    :param user: 当前管理员用户 (AdminDep)。
                 鉴权依赖，确保只有管理员可以执行此操作。
    :return: 完整的教师。
    """
    if settings.is_master():
        # 🌟 本地调用：复用 master_router 逻辑
        return await master_router.create_teacher_private(conn, teacher)
    # 远程转发
    master_url = settings.campus_a_web_url
    if not master_url:
        raise HTTPException(status_code=500, detail="Master URL not configured")

    target_url = f"{master_url.rstrip('/')}/api-private/v1/teachers"
    status, result = await remote_db_call(url=target_url, method="POST", json=teacher.model_dump())

    if status != 201:
        detail = result.get('detail') if isinstance(result, dict) else str(result)
        raise HTTPException(status_code=status or 502, detail=detail)

    return JSONResponse(status_code=status, content=result)


# =================================================================
# 2. 管理员删除教师 (Master写 + 广播清理 Shard)
# =================================================================
@router.delete("/{teacher_id}", status_code=204, responses={
    404: {'model': GenericError, 'description': 'Teacher does not exist'},
    502: {'model': GenericError, 'description': 'Remote not responding'}
})
async def delete_teacher(
    teacher_id: int,
    conn: MasterSlaveConnDep,
    shard_conn: ShardConnDep,
    user: AdminDep
):
    """
    管理员删除教师。
    逻辑：
    1. Master: 删除 teacher 表中的档案。
    2. Master: 广播通知所有校区清理该教师的任课记录 (teach 表)。
    """
    # 分两步走，先清主从库再清分片库。如果同时进行且主库失败而分库成功，数据就不一致了
    if settings.is_master():
        await master_router.delete_teacher_private(conn, teacher_id)
    else:
        # 远程转发给 Master
        master_url = settings.campus_a_web_url
        if not master_url:
            raise HTTPException(status_code=500, detail="Master URL not configured")
        target_url = f"{master_url.rstrip('/')}/api-private/v1/teachers/{teacher_id}"
        status, result = await remote_db_call(url=target_url, method="DELETE")
        if status != 204:
            detail = result.get('detail') if isinstance(result, dict) else str(result)
            raise HTTPException(status_code=status or 502, detail=detail)

    # 2. 广播清理所有分片库 (A, B, C) 的任课记录
    tasks = []
    for campus in ['A', 'B', 'C']:
        target_url = settings.get_campus_web_url(campus)
        if target_url is None:
            # 本地分片清理 (调用 delete_user_private，它也适用于 teacher)
            tasks.append(delete_user_private(shard_conn, teacher_id))
        else:
            # 远程分片清理
            full_url = f"{target_url.rstrip('/')}/api-private/v1/users/{teacher_id}"
            tasks.append(remote_db_call(url=full_url, method="DELETE"))
    # 总之无论发生了什么，都不能抛异常
    for task_result in await asyncio.gather(*tasks, return_exceptions=True):
        if isinstance(task_result, Exception):
            print(f"Delete user task failed: {task_result}")
        elif task_result is not None:
            code, resp = task_result
            if code != 204:
                detail = resp.get('detail') if isinstance(resp, dict) else str(resp)
                print(f"Delete user task failed: {code or 502}: {detail}")


# =================================================================
# 3. 管理员修改教师 (Master写)
# =================================================================
@router.put("/{teacher_id}", status_code=204, responses={
    404: {'model': GenericError, 'description': 'Teacher does not exist'},
    502: {'model': GenericError, 'description': 'Remote not responding'}
})
async def update_teacher(
    teacher_id: int, 
    teacher: TeacherUpdateParams, 
    conn: MasterSlaveConnDep, 
    user: AdminDep
):
    """
    管理员修改教师信息。
    """
    if settings.is_master():
        # 本地调用
        await master_router.update_teacher_private(conn, teacher_id, teacher)
        return
    # 远程转发
    master_url = settings.campus_a_web_url
    if not master_url:
        raise HTTPException(status_code=500, detail="Master URL not configured")

    target_url = f"{master_url.rstrip('/')}/api-private/v1/teachers/{teacher_id}"
    status, result = await remote_db_call(url=target_url, method="PUT", json=teacher.model_dump(exclude_unset=True))

    if status != 204:
        detail = result.get('detail') if isinstance(result, dict) else str(result)
        raise HTTPException(status_code=status or 502, detail=detail)


# =================================================================
# 5. 简单搜索 (Search) - 返回 ID 和 名字
# =================================================================
@router.get("")
async def search_teacher(
    conn: MasterSlaveConnDep, 
    user: AdminDep,
    id: int | None = None,
    name: str | None = None
) -> TeacherQueryResp:
    """
    简单查询教师 (用于前端下拉框等)。
    """
    sql = "SELECT id, name, sex, age FROM teacher WHERE 1=1"
    params = {}

    if id is not None:
        sql += " AND id = :id"
        params["id"] = id
    
    if name is not None:
        sql += " AND name LIKE :name"
        params["name"] = f"%{name}%"

    # sql += " LIMIT 20"
    rows = (await conn.execute(text(sql), params)).all()
    resp_result = [TeacherResp(teacher_id=row.id, name=row.name, sex=row.sex, age=row.age) for row in rows]
    return TeacherQueryResp(total=len(rows), result=resp_result)


# # =================================================================
# # 2. 教师/管理员添加课程 (Shard写)
# # =================================================================
# @router.post("/courses", status_code=201)
# async def add_course(
#     conn_ms: MasterSlaveConnDep,
#     conn_shard: ShardConnDep,
#     course: CourseCreateParams,
#     user: AdminTeacherDep
# ):
#     """
#     添加课程接口。
    
#     权限逻辑：
#     - 如果是管理员 (admin)：可以随意指定 teacher_ids (例如安排其他老师上课)。
#     - 如果是教师 (teacher)：强制只能创建自己的课程 (teacher_ids 必须且只能包含自己)。
    
#     分片逻辑：
#     1. 判断课程所属校区 (course.campus)。
#     2. 如果是本地校区 -> 直接调用 shard_router.create_course 执行本地事务。
#     3. 如果是远程校区 -> 转发请求到对应校区的私有接口。
    
#     :param conn_ms: 主从库连接。
#                     本地调用时传递给 shard_router，用于校验 teacher_id 是否存在。
#     :param conn_shard: 分片库连接。
#                     本地调用时传递给 shard_router，用于写入课程数据。
#     :param course: 课程创建参数 (CourseCreateParams)。
#                    包含 name, capacity, campus, teacher_ids[]。
#     :param user: 当前用户 (AdminTeacherDep)。
#                  管理员或教师均可创建课程。
#     :return: 包含新创建课程 ID 的响应对象。
#     """
    
#     # === 权限控制：教师只能给自己排课 ===
#     if user.role == 'teacher':
#         # 强制覆盖 teacher_ids 为当前用户 ID
#         course.teacher_ids = [user.user_id]
        
#     # === 分片路由逻辑 ===
#     target_url = settings.get_campus_web_url(course.campus)
    
#     # Case A: 本地调用 (目标校区 = 当前校区)
#     if target_url is None:
#         # 直接复用 shard_router 的逻辑，它包含了完整的事务处理和 ID 生成
#         return await shard_router.create_course(
#             master_slave_conn=conn_ms,
#             shard_conn=conn_shard,
#             p=course
#         )
    
#     # Case B: 远程调用 (目标校区 != 当前校区)
#     else:
#         # 拼接远程私有接口地址 (对应 shard_router 在 private 里的路径)
#         full_url = f"{target_url.rstrip('/')}/api-private/v1/courses"
        
#         status, result = await remote_db_call(
#             url=full_url, 
#             method="POST", 
#             json=course.model_dump()
#         )
        
#         if status != 201:
#             detail = result.get('detail') if isinstance(result, dict) else str(result)
#             raise HTTPException(status_code=status or 500, detail=detail)
            
#         return result
