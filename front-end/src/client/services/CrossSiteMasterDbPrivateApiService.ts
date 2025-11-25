/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { SelectionBatchCreateParams } from '../models/SelectionBatchCreateParams';
import type { SelectionBatchResp } from '../models/SelectionBatchResp';
import type { StudentCreateParams } from '../models/StudentCreateParams';
import type { StudentResp } from '../models/StudentResp';
import type { StudentUpdateParams } from '../models/StudentUpdateParams';
import type { TeacherCreateParams } from '../models/TeacherCreateParams';
import type { TeacherResp } from '../models/TeacherResp';
import type { TeacherUpdateParams } from '../models/TeacherUpdateParams';
import type { CancelablePromise } from '../core/CancelablePromise';
import { OpenAPI } from '../core/OpenAPI';
import { request as __request } from '../core/request';
export class CrossSiteMasterDbPrivateApiService {
    /**
     * Create Student Private
     * [私有接口] 接收远程写入：添加学生
     * 该接口运行在 Master 节点，接收 Slave 节点转发过来的写入请求。
     * 对应 init.sql 中的 student 表结构: id, name, sex, age, current_campus
     *
     * :param conn: SQLAlchemy 异步数据库连接对象 (AsyncConnection)。
     * 通过依赖注入获取的主库连接，具有 INSERT/UPDATE/DELETE 权限。
     * :param p: 学生创建参数模型 (StudentCreateParams)。
     * 包含 id, name, sex, age, current_campus 字段。
     * 会被 model_dump() 转换为字典，自动匹配 SQL 语句中的命名参数 (如 :name)。
     * :return: JSON 响应，完整的学生信息。
     * @param requestBody
     * @returns StudentResp Successful Response
     * @throws ApiError
     */
    public static createStudentPrivateApiPrivateV1StudentsPost(
        requestBody: StudentCreateParams,
    ): CancelablePromise<StudentResp> {
        return __request(OpenAPI, {
            method: 'POST',
            url: '/api-private/v1/students',
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                403: `Insufficient permission`,
                422: `Validation Error`,
            },
        });
    }
    /**
     * Delete Student Private
     * [私有接口] 在主库删除学生
     * @param studentId
     * @returns void
     * @throws ApiError
     */
    public static deleteStudentPrivateApiPrivateV1StudentsStudentIdDelete(
        studentId: number,
    ): CancelablePromise<void> {
        return __request(OpenAPI, {
            method: 'DELETE',
            url: '/api-private/v1/students/{student_id}',
            path: {
                'student_id': studentId,
            },
            errors: {
                403: `Insufficient permission`,
                422: `Validation Error`,
            },
        });
    }
    /**
     * Update Student Private
     * 【私有接口】在主库更新学生
     * @param studentId
     * @param requestBody
     * @returns void
     * @throws ApiError
     */
    public static updateStudentPrivateApiPrivateV1StudentsStudentIdPut(
        studentId: number,
        requestBody: StudentUpdateParams,
    ): CancelablePromise<void> {
        return __request(OpenAPI, {
            method: 'PUT',
            url: '/api-private/v1/students/{student_id}',
            path: {
                'student_id': studentId,
            },
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                403: `Insufficient permission`,
                422: `Validation Error`,
            },
        });
    }
    /**
     * Create Teacher Private
     * [私有接口] 接收远程写入：添加教师
     * 该接口运行在 Master 节点，接收 Slave 节点转发过来的写入请求。
     * 对应 init.sql 中的 teacher 表结构: id, name, sex, age
     *
     * :param conn: SQLAlchemy 异步数据库连接对象 (AsyncConnection)。
     * 通过依赖注入获取的主库连接，具有 INSERT/UPDATE/DELETE 权限。
     * :param p: 教师创建参数模型 (TeacherCreateParams)。
     * 包含 id, name, sex, age 字段。
     * 会被 model_dump() 转换为字典，自动匹配 SQL 语句中的命名参数。
     * :return: JSON 响应，完整的教师信息。
     * @param requestBody
     * @returns TeacherResp Successful Response
     * @throws ApiError
     */
    public static createTeacherPrivateApiPrivateV1TeachersPost(
        requestBody: TeacherCreateParams,
    ): CancelablePromise<TeacherResp> {
        return __request(OpenAPI, {
            method: 'POST',
            url: '/api-private/v1/teachers',
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                403: `Insufficient permission`,
                422: `Validation Error`,
            },
        });
    }
    /**
     * Delete Teacher Private
     * 【私有接口】在主库删除教师
     * @param teacherId
     * @returns void
     * @throws ApiError
     */
    public static deleteTeacherPrivateApiPrivateV1TeachersTeacherIdDelete(
        teacherId: number,
    ): CancelablePromise<void> {
        return __request(OpenAPI, {
            method: 'DELETE',
            url: '/api-private/v1/teachers/{teacher_id}',
            path: {
                'teacher_id': teacherId,
            },
            errors: {
                403: `Insufficient permission`,
                422: `Validation Error`,
            },
        });
    }
    /**
     * Update Teacher Private
     * 【私有接口】在主库更新教师
     * @param teacherId
     * @param requestBody
     * @returns void
     * @throws ApiError
     */
    public static updateTeacherPrivateApiPrivateV1TeachersTeacherIdPut(
        teacherId: number,
        requestBody: TeacherUpdateParams,
    ): CancelablePromise<void> {
        return __request(OpenAPI, {
            method: 'PUT',
            url: '/api-private/v1/teachers/{teacher_id}',
            path: {
                'teacher_id': teacherId,
            },
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                403: `Insufficient permission`,
                422: `Validation Error`,
            },
        });
    }
    /**
     * Create Selection Batch Private
     * @param requestBody
     * @returns SelectionBatchResp Successful Response
     * @throws ApiError
     */
    public static createSelectionBatchPrivateApiPrivateV1SelectionBatchesPost(
        requestBody: SelectionBatchCreateParams,
    ): CancelablePromise<SelectionBatchResp> {
        return __request(OpenAPI, {
            method: 'POST',
            url: '/api-private/v1/selection-batches',
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                403: `Insufficient permission`,
                422: `Validation Error`,
            },
        });
    }
    /**
     * Delete Selection Batch Private
     * @param batchId
     * @returns void
     * @throws ApiError
     */
    public static deleteSelectionBatchPrivateApiPrivateV1SelectionBatchesBatchIdDelete(
        batchId: number,
    ): CancelablePromise<void> {
        return __request(OpenAPI, {
            method: 'DELETE',
            url: '/api-private/v1/selection-batches/{batch_id}',
            path: {
                'batch_id': batchId,
            },
            errors: {
                403: `Insufficient permission`,
                422: `Validation Error`,
            },
        });
    }
}
