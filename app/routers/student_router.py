from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from app.models.course_model import CourseQueryResp
from app.models.generic_error import err_course_id_invalid, err_user_exist
from app.models.user_model import StudentCreateParams
from app.routers.dbprivate import shard_router  # 导入私有路由模块以便本地直接调用
from app.settings import settings
from app.utils.auth import StudentDep, AdminDep
from app.utils.database import get_master_slave_connection, get_shard_connection
from app.utils.remote_call import remote_db_call

# 定义数据库连接依赖，用于本地直接调用私有函数
MasterSlaveConnDep = Annotated[AsyncConnection, Depends(get_master_slave_connection)]
ShardConnDep = Annotated[AsyncConnection, Depends(get_shard_connection)]

router = APIRouter(prefix="/student", tags=["Student"])

# =======================
# 1. 管理员添加学生 (Master写)
# =======================
@router.post("/add", status_code=201)
async def add_student(
    conn: MasterSlaveConnDep, 
    student: StudentCreateParams, 
    user: AdminDep
):
    """
    管理员添加学生接口。
    
    根据当前节点是否为主库(Master)，决定是本地写入还是转发请求。
    
    :param conn: 主从库连接对象 (MasterSlaveConnDep)。
                 如果是主库，用于直接执行 INSERT SQL。
    :param student: 学生创建参数 (StudentCreateParams)。
                 包含 id, name, sex, age, current_campus。
    :param user: 当前管理员用户 (AdminDep)。
                 鉴权依赖，保证只有管理员角色可调用此接口。
    :return: 成功消息 {"msg": "Student created"}。
    """
    if settings.is_master():
        # 本地直接写入 (Master)
        try:
            # 适配 init.sql: 包含 sex, age, current_campus
            await conn.execute(
                text('INSERT INTO student (id, name, sex, age, current_campus) VALUES (:id, :name, :sex, :age, :current_campus)'),
                student.model_dump()
            )

            # db_id = (await conn.execute(text("SELECT @@server_id"))).scalar()
            # print(f"\n实际上写入的数据库 Server ID 是: {db_id}\n")
            
        except Exception:
            # 实际生产中应区分 IntegrityError
            raise HTTPException(status_code=409, detail=err_user_exist)
    else:
        # 远程转发给 Master
        master_url = settings.campus_a_web_url
        if not master_url:
            raise HTTPException(status_code=500, detail="Master URL not configured")
        
        target_url = f"{master_url.rstrip('/')}/api-private/v1/students"
        status, result = await remote_db_call(url=target_url, method="POST", json=student.model_dump())
        
        if status != 201:
            detail = result.get('detail') if isinstance(result, dict) else str(result)
            raise HTTPException(status_code=status or 500, detail=detail)
            
    return {"msg": "Student created"}

# =======================
# 2. 查询全校课程 (Shard读)
# =======================
@router.get("/courses", response_model=CourseQueryResp)
async def list_courses(
    user: StudentDep,
    ms_conn: MasterSlaveConnDep,
    shard_conn: ShardConnDep,
    campus: str = Query("A", description="校区: A/B/C"),
    keyword: str | None = None,
    only_not_full: bool = False,
    only_selected: bool = False
):
    """
    查询全校课程接口。
    
    支持按校区、关键词、容量、是否已选进行筛选。
    逻辑：判断目标 campus 参数所指的校区地址。
    - 如果是当前校区：直接调用 shard_router.query_courses 进行本地查询。
    - 如果是其他校区：远程调用对应校区的私有接口。
    
    :param user: 当前登录学生 (StudentDep)。
                 用于在开启 only_selected=True 时查询该学生是否已选。
    :param ms_conn: 主从库连接。
                 本地调用时传递给 shard_router，用于关联查询教师名称等主库信息。
    :param shard_conn: 分片库连接。
                 本地调用时传递给 shard_router，用于查询课程表。
    :param campus: 目标校区 (A/B/C)。
                 核心路由参数，决定了连接本地数据库还是进行远程转发。
    :param keyword: 课程名称关键词过滤。
    :param only_not_full: 是否只显示未满员的课程 (capacity > num_selected)。
    :param only_selected: 是否只显示当前学生已选的课程。
    :return: 课程列表数据 (CourseQueryResp)。
    """
    target_url = settings.get_campus_web_url(campus)
    
    # === 本地调用 ===
    if target_url is None:
        # 直接调用 shard_router 中的函数，传入本地连接
        return await shard_router.query_courses(
            master_slave_conn=ms_conn,
            shard_conn=shard_conn,
            course=keyword,
            teacher=None,
            only_not_full=only_not_full,
            only_selected=only_selected,
            stu_id=user.user_id
        )
    
    # === 远程调用 ===
    else:
        full_url = f"{target_url.rstrip('/')}/api-private/v1/courses"
        # 构造参数
        params = {
            "only_not_full": str(only_not_full).lower(),
            "only_selected": str(only_selected).lower()
        }
        if keyword: params["course"] = keyword
        # 如果只看已选，必须传 stu_id 给远程
        if only_selected: params["stu_id"] = user.user_id 
        
        status, result = await remote_db_call(url=full_url, params=params)
        
        if status != 200:
             detail = result.get('detail') if isinstance(result, dict) else str(result)
             raise HTTPException(status_code=status or 500, detail=detail)
        return result

# =======================
# 3. 抢课 (Shard写)
# =======================
@router.post("/courses/{course_id}/select", status_code=204)
async def select_course(
    course_id: int, 
    user: StudentDep,
    ms_conn: MasterSlaveConnDep,
    shard_conn: ShardConnDep
):
    """
    学生抢课接口。
    
    逻辑：根据 course_id 的前缀判断课程所属校区。
    - 本地校区：直接调用 shard_router.select_course，执行悲观锁+事务写入。
    - 远程校区：转发 HTTP POST 请求到对应分校区的私有接口。
    
    :param course_id: 课程ID (例如 100001)。
                      前两位(10/11/12)标识了课程所在的校区(A/B/C)。
    :param user: 当前操作学生 (StudentDep)。
    :param ms_conn: 主从库连接。本地调用时用于校验学生是否存在。
    :param shard_conn: 分片库连接。本地调用时用于执行选课事务。
    :return: 204 No Content (成功)。
    """
    # 1. 解析课程ID确定校区
    course_prefix = str(course_id)[:2]
    campus_map = {'10': 'A', '11': 'B', '12': 'C'}
    if course_prefix not in campus_map:
        raise HTTPException(status_code=400, detail=err_course_id_invalid)
    
    target_campus = campus_map[course_prefix]
    target_url = settings.get_campus_web_url(target_campus)

    # === 本地调用 ===
    if target_url is None:
        # 注意：shard_router 必须处理 select_time 字段的写入 (例如使用 NOW())
        await shard_router.select_course(
            master_slave_conn=ms_conn,
            shard_conn=shard_conn,
            course_id=course_id,
            stu_id=user.user_id
        )
        return

    # === 远程调用 ===
    else:
        full_url = f"{target_url.rstrip('/')}/api-private/v1/courses/{course_id}/select"
        # query param 传 stu_id
        status, result = await remote_db_call(
            url=full_url, 
            method="POST", 
            params={"stu_id": user.user_id}
        )
        
        if status != 204:
            detail = result.get('detail') if isinstance(result, dict) else str(result)
            raise HTTPException(status_code=status or 500, detail=detail)
        return

# =======================
# 4. 退课 (Shard写)
# =======================
@router.post("/courses/{course_id}/drop", status_code=204)
async def drop_course(
    course_id: int, 
    user: StudentDep,
    ms_conn: MasterSlaveConnDep,
    shard_conn: ShardConnDep
):
    """
    学生退课接口。
    
    逻辑与抢课类似，解析ID后进行本地事务删除或远程转发。
    
    :param course_id: 课程ID。
    :param user: 当前操作学生。
    :param ms_conn: 主从库连接。
    :param shard_conn: 分片库连接。
    :return: 204 No Content (成功)。
    """
    course_prefix = str(course_id)[:2]
    campus_map = {'10': 'A', '11': 'B', '12': 'C'}
    if course_prefix not in campus_map:
        raise HTTPException(status_code=400, detail=err_course_id_invalid)
    
    target_campus = campus_map[course_prefix]
    target_url = settings.get_campus_web_url(target_campus)

    if target_url is None:
        await shard_router.deselect_course(
            master_slave_conn=ms_conn,
            shard_conn=shard_conn,
            course_id=course_id,
            stu_id=user.user_id
        )
        return
    else:
        full_url = f"{target_url.rstrip('/')}/api-private/v1/courses/{course_id}/deselect"
        status, result = await remote_db_call(
            url=full_url, 
            method="POST", 
            params={"stu_id": user.user_id}
        )
        if status != 204:
            detail = result.get('detail') if isinstance(result, dict) else str(result)
            raise HTTPException(status_code=status or 500, detail=detail)
        return