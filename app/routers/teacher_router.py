from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from app.models.course_model import CourseCreateParams
from app.models.generic_error import err_user_exist
from app.models.user_model import TeacherCreateParams
from app.routers.dbprivate import shard_router
from app.settings import settings
from app.utils.auth import AdminTeacherDep, AdminDep
from app.utils.database import get_master_slave_connection, get_shard_connection
from app.utils.remote_call import remote_db_call

# 定义数据库连接依赖
MasterSlaveConnDep = Annotated[AsyncConnection, Depends(get_master_slave_connection)]
ShardConnDep = Annotated[AsyncConnection, Depends(get_shard_connection)]

router = APIRouter(prefix="/api/v1/teachers", tags=["Teacher"])


# =================================================================
# 1. 管理员添加教师 (Master写)
# =================================================================
@router.post("", status_code=201)
async def add_teacher(
    conn: MasterSlaveConnDep, 
    teacher: TeacherCreateParams, 
    user: AdminDep
):
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
    :return: 成功消息 {"msg": "Teacher created"}。
    """
    if settings.is_master():
        try:
            # 本地写入 (database.py 已配置自动事务提交，无需手动 commit)
            await conn.execute(
                text('INSERT INTO teacher (id, name, sex, age) VALUES (:id, :name, :sex, :age)'),
                teacher.model_dump()
            )
            db_id = (await conn.execute(text("SELECT @@server_id"))).scalar()
            print(f"\n实际上写入的数据库 Server ID 是: {db_id}\n")
        except Exception:
            raise HTTPException(status_code=409, detail=err_user_exist)
    else:
        # 远程转发
        master_url = settings.campus_a_web_url
        if not master_url:
            raise HTTPException(status_code=500, detail="Master URL not configured")
        
        target_url = f"{master_url.rstrip('/')}/api-private/v1/teachers"
        status, result = await remote_db_call(url=target_url, method="POST", json=teacher.model_dump())
        
        if status != 201:
            detail = result.get('detail') if isinstance(result, dict) else str(result)
            raise HTTPException(status_code=status or 500, detail=detail)
            
    return {"msg": "Teacher created"}


# =================================================================
# 2. 教师/管理员添加课程 (Shard写)
# =================================================================
@router.post("/courses", status_code=201)
async def add_course(
    conn_ms: MasterSlaveConnDep,
    conn_shard: ShardConnDep,
    course: CourseCreateParams,
    user: AdminTeacherDep
):
    """
    添加课程接口。
    
    权限逻辑：
    - 如果是管理员 (admin)：可以随意指定 teacher_ids (例如安排其他老师上课)。
    - 如果是教师 (teacher)：强制只能创建自己的课程 (teacher_ids 必须且只能包含自己)。
    
    分片逻辑：
    1. 判断课程所属校区 (course.campus)。
    2. 如果是本地校区 -> 直接调用 shard_router.create_course 执行本地事务。
    3. 如果是远程校区 -> 转发请求到对应校区的私有接口。
    
    :param conn_ms: 主从库连接。
                    本地调用时传递给 shard_router，用于校验 teacher_id 是否存在。
    :param conn_shard: 分片库连接。
                    本地调用时传递给 shard_router，用于写入课程数据。
    :param course: 课程创建参数 (CourseCreateParams)。
                   包含 name, capacity, campus, teacher_ids[]。
    :param user: 当前用户 (AdminTeacherDep)。
                 管理员或教师均可创建课程。
    :return: 包含新创建课程 ID 的响应对象。
    """
    
    # === 权限控制：教师只能给自己排课 ===
    if user.role == 'teacher':
        # 强制覆盖 teacher_ids 为当前用户 ID
        course.teacher_ids = [user.user_id]
        
    # === 分片路由逻辑 ===
    target_url = settings.get_campus_web_url(course.campus)
    
    # Case A: 本地调用 (目标校区 = 当前校区)
    if target_url is None:
        # 直接复用 shard_router 的逻辑，它包含了完整的事务处理和 ID 生成
        return await shard_router.create_course(
            master_slave_conn=conn_ms,
            shard_conn=conn_shard,
            p=course
        )
    
    # Case B: 远程调用 (目标校区 != 当前校区)
    else:
        # 拼接远程私有接口地址 (对应 shard_router 在 private 里的路径)
        full_url = f"{target_url.rstrip('/')}/api-private/v1/courses"
        
        status, result = await remote_db_call(
            url=full_url, 
            method="POST", 
            json=course.model_dump()
        )
        
        if status != 201:
            detail = result.get('detail') if isinstance(result, dict) else str(result)
            raise HTTPException(status_code=status or 500, detail=detail)
            
        return result


# todo 重复了
# =================================================================
# 3. 查询某门课的学生名单 (Shard读)
# =================================================================
@router.get("/courses/{course_id}/students")
async def get_course_students(
    course_id: int, 
    user: AdminTeacherDep,
    ms_conn: MasterSlaveConnDep,
    shard_conn: ShardConnDep
):
    """
    查询某门课程的学生名单接口。
    
    该操作需要跨库查询（从分片库查选课记录，从主库查学生详情）。
    逻辑：根据 course_id 判断课程所在校区。
    - 本地校区：调用 shard_router.get_course_students 执行本地逻辑。
    - 远程校区：转发 GET 请求到对应分校区的私有接口。
    
    :param course_id: 课程ID。前两位 (10/11/12) 决定了数据在哪个分库。
    :param user: 当前用户 (AdminTeacherDep)。
                 允许管理员或教师调用。
    :param ms_conn: 主从库连接。
                 本地调用时传递给 shard_router，用于最后一步查询学生详细信息(姓名等)。
    :param shard_conn: 分片库连接。
                 本地调用时传递给 shard_router，用于查询 learn 表获取学号列表。
    :return: 学生列表数据。
    """
    course_prefix = str(course_id)[:2]
    campus_map = {'10': 'A', '11': 'B', '12': 'C'}
    
    # 简单校验 ID 格式
    if course_prefix not in campus_map:
        return {"total": 0, "result": []}
    
    target_campus = campus_map[course_prefix]
    target_url = settings.get_campus_web_url(target_campus)
    
    # Case A: 本地调用
    if target_url is None:
        return await shard_router.get_course_students(
            master_slave_conn=ms_conn,
            shard_conn=shard_conn,
            course_id=course_id
        )
    
    # Case B: 远程调用
    else:
        full_url = f"{target_url.rstrip('/')}/api-private/v1/courses/{course_id}/students"
        status, result = await remote_db_call(url=full_url)
        
        if status != 200:
             detail = result.get('detail') if isinstance(result, dict) else str(result)
             raise HTTPException(status_code=status or 500, detail=detail)
        return result


# todo 重复了
# =================================================================
# 4. 查询我教的课 (聚合查询 - 暂未实现)
# =================================================================
@router.get("/my-courses")
async def get_taught_courses(
    user: AdminTeacherDep,
):
    """
    查询我教的课。
    注意：这是一个全分片聚合查询，需要遍历 A, B, C 三个校区。
    """
    # TODO: 实现并发调用三个校区的 /api-private/v1/teachers/{tid}/courses 接口并合并结果
    return {"code": 0, "data": [], "msg": "Aggregated search not implemented yet"}