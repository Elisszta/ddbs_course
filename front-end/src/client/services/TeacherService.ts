/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { TeacherCreateParams } from '../models/TeacherCreateParams';
import type { TeacherQueryResp } from '../models/TeacherQueryResp';
import type { TeacherResp } from '../models/TeacherResp';
import type { TeacherUpdateParams } from '../models/TeacherUpdateParams';
import type { CancelablePromise } from '../core/CancelablePromise';
import { OpenAPI } from '../core/OpenAPI';
import { request as __request } from '../core/request';
export class TeacherService {
    /**
     * Add Teacher
     * 管理员添加教师接口。
     *
     * 逻辑：教师档案数据必须写入全局主库(Master)。
     * - 如果当前节点是 Master：直接执行本地 INSERT SQL。
     * - 如果当前节点是 Slave：通过私有接口转发请求给 Master。
     *
     * :param conn: 主从库连接对象 (MasterSlaveConnDep)。
     * 如果是主库，用于直接执行 INSERT SQL。
     * :param teacher: 教师创建参数 (TeacherCreateParams)。
     * 包含 id, name, sex, age。
     * :param user: 当前管理员用户 (AdminDep)。
     * 鉴权依赖，确保只有管理员可以执行此操作。
     * :return: 完整的教师。
     * @param requestBody
     * @returns TeacherResp Successful Response
     * @throws ApiError
     */
    public static addTeacherApiV1TeachersPost(
        requestBody: TeacherCreateParams,
    ): CancelablePromise<TeacherResp> {
        return __request(OpenAPI, {
            method: 'POST',
            url: '/api/v1/teachers',
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                403: `Insufficient permission`,
                409: `Teacher id conflict or full`,
                422: `Validation Error`,
                502: `Remote not responding`,
            },
        });
    }
    /**
     * Search Teacher
     * 简单查询教师 (用于前端下拉框等)。
     * @param id
     * @param name
     * @returns TeacherQueryResp Successful Response
     * @throws ApiError
     */
    public static searchTeacherApiV1TeachersGet(
        id?: (number | null),
        name?: (string | null),
    ): CancelablePromise<TeacherQueryResp> {
        return __request(OpenAPI, {
            method: 'GET',
            url: '/api/v1/teachers',
            query: {
                'id': id,
                'name': name,
            },
            errors: {
                403: `Insufficient permission`,
                422: `Validation Error`,
            },
        });
    }
    /**
     * Delete Teacher
     * 管理员删除教师。
     * 逻辑：
     * 1. Master: 删除 teacher 表中的档案。
     * 2. Master: 广播通知所有校区清理该教师的任课记录 (teach 表)。
     * @param teacherId
     * @returns void
     * @throws ApiError
     */
    public static deleteTeacherApiV1TeachersTeacherIdDelete(
        teacherId: number,
    ): CancelablePromise<void> {
        return __request(OpenAPI, {
            method: 'DELETE',
            url: '/api/v1/teachers/{teacher_id}',
            path: {
                'teacher_id': teacherId,
            },
            errors: {
                403: `Insufficient permission`,
                404: `Teacher does not exist`,
                422: `Validation Error`,
                502: `Remote not responding`,
            },
        });
    }
    /**
     * Update Teacher
     * 管理员修改教师信息。
     * @param teacherId
     * @param requestBody
     * @returns void
     * @throws ApiError
     */
    public static updateTeacherApiV1TeachersTeacherIdPut(
        teacherId: number,
        requestBody: TeacherUpdateParams,
    ): CancelablePromise<void> {
        return __request(OpenAPI, {
            method: 'PUT',
            url: '/api/v1/teachers/{teacher_id}',
            path: {
                'teacher_id': teacherId,
            },
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                403: `Insufficient permission`,
                404: `Teacher does not exist`,
                422: `Validation Error`,
                502: `Remote not responding`,
            },
        });
    }
}
