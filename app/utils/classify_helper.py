from typing import Literal

from app.settings import settings


def get_course_campus(course_id: int) -> Literal['A', 'B', 'C']:
    course_campus = course_id // 100000
    if course_campus == 10:
        return 'A'
    if course_campus == 11:
        return 'B'
    if course_campus == 12:
        return 'C'
    return settings.current_campus()


def get_user_role(user_id: int) -> Literal['admin', 'student', 'teacher']:
    user_role = user_id // 100000000
    if user_role == 10:
        return 'admin'
    if user_role == 11:
        return 'student'
    return 'teacher'
