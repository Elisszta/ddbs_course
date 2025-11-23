from pydantic import BaseModel


class BizError(BaseModel):
    code: int
    msg: str


class GenericError(BaseModel):
    detail: BizError | str


err_course_cap_conflict = BizError(code=10001, msg='Course capacity conflict').model_dump()
err_course_id_conflict = BizError(code=10002, msg='Course id conflict').model_dump()
err_course_id_full = BizError(code=10003, msg='No course id available').model_dump()
err_course_already_selected = BizError(code=10004, msg='You have already selected the course').model_dump()
err_course_not_exist = BizError(code=20001, msg='Course dose not exist').model_dump()
err_teacher_not_exist = BizError(code=20002, msg='Teacher dose not exist').model_dump()
err_student_not_exist = BizError(code=20003, msg='Student dose not exist').model_dump()
err_no_permission = BizError(code=30001, msg='You are not allowed to do this').model_dump()
err_invalid_uid = BizError(code=30002, msg='Invalid user id').model_dump()
err_invalid_token = BizError(code=30003, msg='Invalid token').model_dump()
err_expired_token = BizError(code=30004, msg='Expired token').model_dump()
err_selection_time = BizError(code=30005, msg='Course selection time has not arrived').model_dump()
err_bad_gateway = BizError(code=40001, msg='Bad gateway').model_dump()
