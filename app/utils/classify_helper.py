from typing import Literal


def get_course_campus(course_id: int) -> Literal['A', 'B', 'C']:
    course_campus = course_id // 100000
    if course_campus == 10:
        return 'A'
    if course_campus == 11:
        return 'B'
    return 'C'


def get_user_role(user_id: int) -> Literal['admin', 'student', 'teacher']:
    user_role = user_id // 100000000
    if user_role == 10:
        return 'admin'
    if user_role == 11:
        return 'student'
    return 'teacher'
