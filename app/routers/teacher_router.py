from fastapi import APIRouter, HTTPException
from app.routers.course_router import AdminTeacherDep

router = APIRouter(prefix="/teacher", tags=["Teacher"])

# =======================
# 1. 查询我教的课
# =======================
@router.get("/my-courses")
async def get_taught_courses(user: AdminTeacherDep):
    """
    查询教师自己教的课
    读取：遍历所有分片库 (Shard A + B + C)
    """
    results = []
    shards = ["DB_SHARD_A", "DB_SHARD_B", "DB_SHARD_C"]
    
    # 注意：这里用的是 user.user_id，从 auth.py 解析出来的 int ID
    sql = """
        SELECT c.* FROM teach t
        JOIN course c ON t.cid = c.id
        WHERE t.tid = :tid
    """
    
    for shard in shards:
        # TODO: rows = await execute_shard(shard, sql, {"tid": user.user_id})
        rows = [] # 模拟
        results.extend(rows)
        
    return {"code": 0, "data": results}

# =======================
# 2. 查询某门课的学生名单 (跨库难点)
# =======================
@router.get("/courses/{course_id}/students")
async def get_course_students(course_id: int, user: AdminTeacherDep):
    """
    查询课程下的学生名单
    第一步：在分片库查出学生ID列表
    第二步：在主库根据ID列表查学生详情
    """
    # 1. 确定课程在哪个分库
    prefix = str(course_id)[:2]
    # 简化的分片逻辑
    if prefix == '10': target_db = "DB_SHARD_A"
    elif prefix == '11': target_db = "DB_SHARD_B"
    else: target_db = "DB_SHARD_C"
    
    # 2. 【分库查询】拿到所有选课学生的 ID
    sql_ids = "SELECT sid FROM learn WHERE cid = :cid"
    # TODO: student_ids_rows = await execute_shard(target_db, sql_ids, {"cid": course_id})
    # 假设结果是: [{'sid': 1120250001}, {'sid': 1120250002}]
    student_ids = [1120250001, 1120250002] # 模拟数据
    
    if not student_ids:
        return {"code": 0, "data": []}
        
    # 3. 【主库查询】根据 ID 查学生详细信息 (IN 查询)
    # 注意处理 list 转 sql string 的格式
    sql_details = f"SELECT id, name, sex, age, current_campus FROM student WHERE id IN ({','.join(map(str, student_ids))})"
    
    # TODO: students = await execute_master(sql_details)
    students = [
        {"id": 1120250001, "name": "小明", "sex": "M", "current_campus": "A"}
    ]
    
    return {"code": 0, "data": students}