/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { StudentCreateParams } from '../models/StudentCreateParams';
import type { StudentSimpleResp } from '../models/StudentSimpleResp';
import type { StudentUpdateParams } from '../models/StudentUpdateParams';
import type { CancelablePromise } from '../core/CancelablePromise';
import { OpenAPI } from '../core/OpenAPI';
import { request as __request } from '../core/request';
export class StudentService {
    /**
     * Add Student
     * 管理员添加学生接口。
     *
     * 根据当前节点是否为主库(Master)，决定是本地写入还是转发请求。
     *
     * :param conn: 主从库连接对象 (MasterSlaveConnDep)。
     * 如果是主库，用于直接执行 INSERT SQL。
     * :param student: 学生创建参数 (StudentCreateParams)。
     * 包含 id, name, sex, age, current_campus。
     * :param user: 当前管理员用户 (AdminDep)。
     * 鉴权依赖，保证只有管理员角色可调用此接口。
     * :return: 成功消息 {"msg": "Student created"}。
     * @param requestBody
     * @returns any Successful Response
     * @throws ApiError
     */
    public static addStudentApiV1StudentsPost(
        requestBody: StudentCreateParams,
    ): CancelablePromise<any> {
        return __request(OpenAPI, {
            method: 'POST',
            url: '/api/v1/students',
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                422: `Validation Error`,
            },
        });
    }
    /**
     * Delete Student
     * 管理员删除学生。
     * 逻辑：Master 删除档案，并通知所有分片库清理选课记录。
     * @param studentId
     * @returns void
     * @throws ApiError
     */
    public static deleteStudentApiV1StudentsStudentIdDelete(
        studentId: number,
    ): CancelablePromise<void> {
        return __request(OpenAPI, {
            method: 'DELETE',
            url: '/api/v1/students/{student_id}',
            path: {
                'student_id': studentId,
            },
            errors: {
                422: `Validation Error`,
            },
        });
    }
    /**
     * Update Student
     * 管理员修改学生信息。
     * @param studentId
     * @param requestBody
     * @returns void
     * @throws ApiError
     */
    public static updateStudentApiV1StudentsStudentIdPut(
        studentId: number,
        requestBody: StudentUpdateParams,
    ): CancelablePromise<void> {
        return __request(OpenAPI, {
            method: 'PUT',
            url: '/api/v1/students/{student_id}',
            path: {
                'student_id': studentId,
            },
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                422: `Validation Error`,
            },
        });
    }
    /**
     * Search Student
     * 查询学生信息 (只返回 ID 和 姓名)。
     * - 如果提供 id：查对应的名字。
     * - 如果提供 name：查对应的 ID (可能多个)。
     * - 如果都不提供：返回空列表。
     * 逻辑：直接读取本地数据库 (student表已同步)。
     * @param id
     * @param name
     * @returns StudentSimpleResp Successful Response
     * @throws ApiError
     */
    public static searchStudentApiV1StudentsSearchGet(
        id?: (number | null),
        name?: (string | null),
    ): CancelablePromise<Array<StudentSimpleResp>> {
        return __request(OpenAPI, {
            method: 'GET',
            url: '/api/v1/students/search',
            query: {
                'id': id,
                'name': name,
            },
            errors: {
                422: `Validation Error`,
            },
        });
    }
}
