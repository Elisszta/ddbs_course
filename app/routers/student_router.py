from fastapi import APIRouter, HTTPException, Query
from app.routers.course_router import StudentDep
from app.settings import settings
# 引入你的数据库连接池或执行函数，例如:
# from app.db import execute_master, execute_shard_a, execute_shard_b, execute_shard_c

router = APIRouter(prefix="/student", tags=["Student"])

def get_shard_by_campus(campus: str):
    """辅助函数：根据校区获取对应的数据库连接/URL"""
    # 实际项目中这里应该返回对应的数据库连接对象
    if campus == 'A': return "DB_SHARD_A"
    if campus == 'B': return "DB_SHARD_B"
    return "DB_SHARD_C"

def get_shard_by_course_id(course_id: int):
    """辅助函数：根据课程ID前缀判断在哪个分库"""
    # 假设 ID 格式：10xxxxx -> A, 11xxxxx -> B, 12xxxxx -> C
    prefix = str(course_id)[:2]
    if prefix == '10': return "DB_SHARD_A"
    if prefix == '11': return "DB_SHARD_B"
    if prefix == '12': return "DB_SHARD_C"
    raise HTTPException(status_code=400, detail="无效的课程ID")

# =======================
# 1. 查询选课批次
# =======================
@router.get("/batches")
async def get_batches(user: StudentDep):
    """
    学生查询抢课时间段
    读取：主从库 (Master/Slave)
    """
    sql = "SELECT * FROM selection_batch WHERE end_time > NOW()"
    # TODO: result = await execute_master(sql)
    result = [{"id": 1, "name": "第一轮选课", "status": "open"}] # 模拟数据
    return {"code": 0, "data": result}

# =======================
# 2. 查询全校课程
# =======================
@router.get("/courses")
async def list_courses(
    user: StudentDep, 
    campus: str = Query("A", description="校区: A/B/C"),
    keyword: str | None = None,
    only_not_full: bool = False
):
    """
    查询指定校区的课程
    读取：指定的分片库 (Shard)
    """
    # 1. 确定去哪个库查
    target_db = get_shard_by_campus(campus)
    
    # 2. 构建 SQL
    sql = "SELECT * FROM course WHERE 1=1"
    params = {}
    if keyword:
        sql += " AND name LIKE :keyword"
        params['keyword'] = f"%{keyword}%"
    if only_not_full:
        sql += " AND num_selected < capacity"
        
    # TODO: courses = await execute_shard(target_db, sql, params)
    
    # 模拟数据
    courses = [
        {"id": 100001, "name": "分布式数据库", "capacity": 100, "num_selected": 50, "campus": "A"}
    ]
    
    # 3. (可选优化) 如果要显示“我是否已选”，需要再去查 learn 表
    # 这里为了性能通常交给前端去匹配，或者再发一次查询
    
    return {"code": 0, "data": courses}

# =======================
# 3. 查询我已选的课
# =======================
@router.get("/my-courses")
async def get_my_courses(user: StudentDep):
    """
    查询我选的所有课
    读取：遍历所有分片库 (Shard A + B + C)
    """
    my_courses = []
    
    # 因为学生可以在任意校区选课，必须遍历所有分库
    shards = ["DB_SHARD_A", "DB_SHARD_B", "DB_SHARD_C"]
    
    sql = """
        SELECT c.id, c.name, c.campus, c.capacity, l.select_time 
        FROM learn l
        JOIN course c ON l.cid = c.id
        WHERE l.sid = :uid
    """
    
    for shard in shards:
        # TODO: rows = await execute_shard(shard, sql, {"uid": user.user_id})
        rows = [] # 模拟结果
        my_courses.extend(rows)
        
    return {"code": 0, "data": my_courses}

# =======================
# 4. 抢课 (核心逻辑)
# =======================
@router.post("/courses/{course_id}/select")
async def select_course(course_id: int, user: StudentDep):
    """
    抢课接口
    写入：指定分片库 (开启事务)
    """
    # 1. 判断去哪个库
    target_db = get_shard_by_course_id(course_id)
    
    # 2. (可选) 检查当前是否在选课时间内 (读主库 check selection_batch)
    
    # 3. 开启分库事务
    # async with target_db.transaction():
    try:
        # A. 悲观锁查询课程余量
        # SELECT capacity, num_selected FROM course WHERE id = :cid FOR UPDATE
        
        # B. 判断是否已满
        # if num_selected >= capacity: raise Error("课程已满")
        
        # C. 写入 learn 表
        # INSERT INTO learn (sid, cid, select_time) VALUES (:uid, :cid, NOW())
        
        # D. 更新课程表
        # UPDATE course SET num_selected = num_selected + 1 WHERE id = :cid
        
        # E. 提交事务
        pass
    except Exception as e:
        # 回滚
        raise HTTPException(status_code=400, detail="抢课失败: " + str(e))
        
    return {"code": 0, "msg": "选课成功"}

# =======================
# 5. 退课
# =======================
@router.post("/courses/{course_id}/drop")
async def drop_course(course_id: int, user: StudentDep):
    """
    退课接口
    写入：指定分片库 (开启事务)
    """
    target_db = get_shard_by_course_id(course_id)
    
    # async with target_db.transaction():
    # DELETE FROM learn WHERE sid=:uid AND cid=:cid
    # UPDATE course SET num_selected = num_selected - 1 WHERE id=:cid
    
    return {"code": 0, "msg": "退课成功"}