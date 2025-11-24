/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { CourseCreateParams } from '../models/CourseCreateParams';
import type { CourseCreateResp } from '../models/CourseCreateResp';
import type { CourseQueryResp } from '../models/CourseQueryResp';
import type { CourseUpdateParams } from '../models/CourseUpdateParams';
import type { StudentQueryResp } from '../models/StudentQueryResp';
import type { CancelablePromise } from '../core/CancelablePromise';
import { OpenAPI } from '../core/OpenAPI';
import { request as __request } from '../core/request';
export class CrossSiteShardDbPrivateApiService {
    /**
     * Delete User Private
     * 用户删除分库路由函数。删除用户时必须原地调用+所有远程http调用
     * :param shard_conn: 本地分片库连接
     * :param user_id: 用户id
     * :return:
     * @param userId
     * @returns void
     * @throws ApiError
     */
    public static deleteUserPrivateApiPrivateV1UsersUserIdDelete(
        userId: number,
    ): CancelablePromise<void> {
        return __request(OpenAPI, {
            method: 'DELETE',
            url: '/api-private/v1/users/{user_id}',
            path: {
                'user_id': userId,
            },
            errors: {
                403: `Insufficient permission`,
                422: `Validation Error`,
            },
        });
    }
    /**
     * Select Course Private
     * 选课分库路由函数。若课程校区就在本地，可直接原地调用该函数
     * :param master_slave_conn: 本地主从库连接
     * :param shard_conn: 本地分片库连接
     * :param course_id: 课程id
     * :param stu_id: 学生id
     * :return:
     * @param courseId
     * @param stuId
     * @returns void
     * @throws ApiError
     */
    public static selectCoursePrivateApiPrivateV1CoursesCourseIdSelectPost(
        courseId: number,
        stuId: number,
    ): CancelablePromise<void> {
        return __request(OpenAPI, {
            method: 'POST',
            url: '/api-private/v1/courses/{course_id}/select',
            path: {
                'course_id': courseId,
            },
            query: {
                'stu_id': stuId,
            },
            errors: {
                403: `Insufficient permission`,
                422: `Validation Error`,
            },
        });
    }
    /**
     * Deselect Course Private
     * 退课分库路由函数。若课程校区就在本地，可直接原地调用该函数
     * :param master_slave_conn: 本地主从库连接
     * :param shard_conn: 本地分片库连接
     * :param course_id: 课程id
     * :param stu_id: 学生id
     * :return:
     * @param courseId
     * @param stuId
     * @returns void
     * @throws ApiError
     */
    public static deselectCoursePrivateApiPrivateV1CoursesCourseIdDeselectPost(
        courseId: number,
        stuId: number,
    ): CancelablePromise<void> {
        return __request(OpenAPI, {
            method: 'POST',
            url: '/api-private/v1/courses/{course_id}/deselect',
            path: {
                'course_id': courseId,
            },
            query: {
                'stu_id': stuId,
            },
            errors: {
                403: `Insufficient permission`,
                422: `Validation Error`,
            },
        });
    }
    /**
     * Get Course Students Private
     * 查课程学生分库路由函数。若课程校区就在本地，可直接原地调用该函数
     * :param master_slave_conn: 本地主从库连接
     * :param shard_conn: 本地分片库连接
     * :param course_id: 课程id
     * :return: 学生查询结果
     * @param courseId
     * @returns StudentQueryResp Successful Response
     * @throws ApiError
     */
    public static getCourseStudentsPrivateApiPrivateV1CoursesCourseIdStudentsGet(
        courseId: number,
    ): CancelablePromise<StudentQueryResp> {
        return __request(OpenAPI, {
            method: 'GET',
            url: '/api-private/v1/courses/{course_id}/students',
            path: {
                'course_id': courseId,
            },
            errors: {
                403: `Insufficient permission`,
                422: `Validation Error`,
            },
        });
    }
    /**
     * Query Courses Private
     * 课程查询分库路由函数。若课程校区就在本地，可直接原地调用该函数
     * :param master_slave_conn: 本地主从库连接
     * :param shard_conn: 本地分片库连接
     * :param course: 课程id或课程关键词或空
     * :param teacher: 教师id或教师名或空
     * :param only_not_full: 是否只查询未满或空
     * :param only_selected: 是否只查询已选或空。若为True，则必须提供学生id
     * :param stu_id: 学生id或空。若提供学生id，查询结果中将包括该学生是否已选的信息
     * :return: 课程查询结果
     * @param course
     * @param teacher
     * @param onlyNotFull
     * @param onlySelected
     * @param stuId
     * @returns CourseQueryResp Successful Response
     * @throws ApiError
     */
    public static queryCoursesPrivateApiPrivateV1CoursesGet(
        course?: (number | string | null),
        teacher?: (number | string | null),
        onlyNotFull?: (boolean | null),
        onlySelected?: (boolean | null),
        stuId?: (number | null),
    ): CancelablePromise<CourseQueryResp> {
        return __request(OpenAPI, {
            method: 'GET',
            url: '/api-private/v1/courses',
            query: {
                'course': course,
                'teacher': teacher,
                'only_not_full': onlyNotFull,
                'only_selected': onlySelected,
                'stu_id': stuId,
            },
            errors: {
                403: `Insufficient permission`,
                422: `Validation Error`,
            },
        });
    }
    /**
     * Create Course Private
     * 课程创建分库路由函数。若课程校区就在本地，可直接原地调用该函数
     * :param master_slave_conn: 本地主从库连接
     * :param shard_conn: 本地分片库连接
     * :param p: 课程创建参数
     * :return:
     * @param requestBody
     * @returns CourseCreateResp Successful Response
     * @throws ApiError
     */
    public static createCoursePrivateApiPrivateV1CoursesPost(
        requestBody: CourseCreateParams,
    ): CancelablePromise<CourseCreateResp> {
        return __request(OpenAPI, {
            method: 'POST',
            url: '/api-private/v1/courses',
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                403: `Insufficient permission`,
                422: `Validation Error`,
            },
        });
    }
    /**
     * Delete Course Private
     * 课程删除分库路由函数。若课程校区就在本地，可直接原地调用该函数
     * :param shard_conn: 本地分片库连接
     * :param course_id: 课程id
     * :return:
     * @param courseId
     * @returns void
     * @throws ApiError
     */
    public static deleteCoursePrivateApiPrivateV1CoursesCourseIdDelete(
        courseId: number,
    ): CancelablePromise<void> {
        return __request(OpenAPI, {
            method: 'DELETE',
            url: '/api-private/v1/courses/{course_id}',
            path: {
                'course_id': courseId,
            },
            errors: {
                403: `Insufficient permission`,
                422: `Validation Error`,
            },
        });
    }
    /**
     * Update Course Private
     * 课程更新分库路由函数。若课程校区就在本地，可直接原地调用该函数
     * :param master_slave_conn: 本地主从库连接
     * :param shard_conn: 本地分片库连接
     * :param course_id: 课程id
     * :param p: 课程更新参数
     * :return:
     * @param courseId
     * @param requestBody
     * @returns void
     * @throws ApiError
     */
    public static updateCoursePrivateApiPrivateV1CoursesCourseIdPut(
        courseId: number,
        requestBody: CourseUpdateParams,
    ): CancelablePromise<void> {
        return __request(OpenAPI, {
            method: 'PUT',
            url: '/api-private/v1/courses/{course_id}',
            path: {
                'course_id': courseId,
            },
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                403: `Insufficient permission`,
                422: `Validation Error`,
            },
        });
    }
}
