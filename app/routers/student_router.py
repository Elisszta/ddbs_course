from typing import Annotated, List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from app.models.generic_error import err_student_not_exist
from app.models.user_model import StudentCreateParams, StudentUpdateParams, StudentSimpleResp
from app.routers.dbprivate import shard_router, master_router
from app.settings import settings
from app.utils.auth import AdminDep
from app.utils.database import get_master_slave_connection, get_shard_connection
from app.utils.remote_call import remote_db_call

# 定义数据库连接依赖，用于本地直接调用私有函数
MasterSlaveConnDep = Annotated[AsyncConnection, Depends(get_master_slave_connection)]
ShardConnDep = Annotated[AsyncConnection, Depends(get_shard_connection)]

router = APIRouter(prefix="/api/v1/students", tags=["Student"])

# =======================
# 1. 管理员添加学生 (Master写)
# =======================
@router.post("", status_code=201)
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
        return await master_router.create_student_private(conn, student)
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


# ==========================================
#  2. 管理员删除学生 (Master写 + 广播清理)
# ==========================================
@router.delete("/{student_id}", status_code=204)
async def delete_student(
    student_id: int, 
    conn: MasterSlaveConnDep, 
    shard_conn: ShardConnDep,
    user: AdminDep
):
    """
    管理员删除学生。
    逻辑：Master 删除档案，并通知所有分片库清理选课记录。
    """
    if settings.is_master():
        # 1. 本地 Master 删除档案
        result = await conn.execute(text("DELETE FROM student WHERE id = :id"), {"id": student_id})
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail=err_student_not_exist)
            
        # 2. 广播清理所有分片库 (A, B, C) 的选课记录
        for campus in ['A', 'B', 'C']:
            target_url = settings.get_campus_web_url(campus)
            
            if target_url is None:
                # 本地分片清理 (直接调用 shard_router 方法)
                await shard_router.delete_user(shard_conn, student_id)
            else:
                # 远程分片清理 (调用 shard_router 的私有接口)
                full_url = f"{target_url.rstrip('/')}/api-private/v1/users/{student_id}"
                await remote_db_call(url=full_url, method="DELETE")
                
    else:
        # 远程转发给 Master (由 Master 负责删除和广播)
        master_url = settings.campus_a_web_url
        if not master_url:
            raise HTTPException(status_code=500, detail="Master URL not configured")
        
        target_url = f"{master_url.rstrip('/')}/api-private/v1/students/{student_id}"
        status, result = await remote_db_call(url=target_url, method="DELETE")
        
        if status != 204:
            detail = result.get('detail') if isinstance(result, dict) else str(result)
            raise HTTPException(status_code=status or 500, detail=detail)
    
    return None


# ==========================================
#  3. 管理员修改学生 (Master写)
# ==========================================
@router.put("/{student_id}", status_code=204)
async def update_student(
    student_id: int, 
    student: StudentUpdateParams, 
    conn: MasterSlaveConnDep, 
    user: AdminDep
):
    """
    管理员修改学生信息。
    """
    if settings.is_master():
        await master_router.update_student_private(conn, student_id, student)
    else:
        # 远程转发
        master_url = settings.campus_a_web_url
        if not master_url:
            raise HTTPException(status_code=500, detail="Master URL not configured")
        
        target_url = f"{master_url.rstrip('/')}/api-private/v1/students/{student_id}"
        status, result = await remote_db_call(url=target_url, method="PUT", json=student.model_dump(exclude_unset=True))
        
        if status != 204:
            detail = result.get('detail') if isinstance(result, dict) else str(result)
            raise HTTPException(status_code=status or 500, detail=detail)
            
    return None


# ==========================================
#  4. 管理员查询学生 (ID查名字 或 名字查ID)
# ==========================================
@router.get("/search", response_model=List[StudentSimpleResp])
async def search_student(
    conn: MasterSlaveConnDep, 
    user: AdminDep,
    id: int | None = None,
    name: str | None = None
):
    """
    查询学生信息 (只返回 ID 和 姓名)。
    - 如果提供 id：查对应的名字。
    - 如果提供 name：查对应的 ID (可能多个)。
    - 如果都不提供：返回空列表。
    逻辑：直接读取本地数据库 (student表已同步)。
    """
    if id is None and name is None:
        return []

    sql = "SELECT id, name FROM student WHERE 1=1"
    params = {}

    if id is not None:
        sql += " AND id = :id"
        params["id"] = id
    
    if name is not None:
        sql += " AND name LIKE :name"
        params["name"] = f"%{name}%" # 支持模糊搜索

    # 限制返回数量防止爆炸
    sql += " LIMIT 20"

    rows = (await conn.execute(text(sql), params)).all()
    
    return [StudentSimpleResp(id=row.id, name=row.name) for row in rows]
