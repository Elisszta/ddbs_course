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
export class CourseApiService {
    /**
     * Query Courses
     * @param campus
     * @param course
     * @param teacher
     * @param onlyNotFull
     * @param onlySelected
     * @returns CourseQueryResp Successful Response
     * @throws ApiError
     */
    public static queryCoursesApiV1CoursesGet(
        campus: Array<'A' | 'B' | 'C'>,
        course?: (number | string | null),
        teacher?: (number | string | null),
        onlyNotFull?: (boolean | null),
        onlySelected?: (boolean | null),
    ): CancelablePromise<CourseQueryResp> {
        return __request(OpenAPI, {
            method: 'GET',
            url: '/api/v1/courses',
            query: {
                'campus': campus,
                'course': course,
                'teacher': teacher,
                'only_not_full': onlyNotFull,
                'only_selected': onlySelected,
            },
            errors: {
                403: `Insufficient permission`,
                422: `Validation Error`,
            },
        });
    }
    /**
     * Create Course
     * @param requestBody
     * @returns CourseCreateResp Successful Response
     * @throws ApiError
     */
    public static createCourseApiV1CoursesPost(
        requestBody: CourseCreateParams,
    ): CancelablePromise<CourseCreateResp> {
        return __request(OpenAPI, {
            method: 'POST',
            url: '/api/v1/courses',
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                403: `Insufficient permission`,
                404: `Teacher does not exist`,
                409: `Course id conflict or full`,
                422: `Validation Error`,
                502: `Remote not responding`,
            },
        });
    }
    /**
     * Delete Course
     * @param courseId
     * @returns void
     * @throws ApiError
     */
    public static deleteCourseApiV1CoursesCourseIdDelete(
        courseId: number,
    ): CancelablePromise<void> {
        return __request(OpenAPI, {
            method: 'DELETE',
            url: '/api/v1/courses/{course_id}',
            path: {
                'course_id': courseId,
            },
            errors: {
                403: `Insufficient permission`,
                422: `Validation Error`,
                502: `Remote not responding`,
            },
        });
    }
    /**
     * Update Course
     * @param courseId
     * @param requestBody
     * @returns void
     * @throws ApiError
     */
    public static updateCourseApiV1CoursesCourseIdPut(
        courseId: number,
        requestBody: CourseUpdateParams,
    ): CancelablePromise<void> {
        return __request(OpenAPI, {
            method: 'PUT',
            url: '/api/v1/courses/{course_id}',
            path: {
                'course_id': courseId,
            },
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                403: `Insufficient permission`,
                404: `Course or teacher does not exist`,
                409: `Course capacity conflict`,
                422: `Validation Error`,
                502: `Remote not responding`,
            },
        });
    }
    /**
     * Get Course Students
     * @param courseId
     * @returns StudentQueryResp Successful Response
     * @throws ApiError
     */
    public static getCourseStudentsApiV1CoursesCourseIdStudentsGet(
        courseId: number,
    ): CancelablePromise<StudentQueryResp> {
        return __request(OpenAPI, {
            method: 'GET',
            url: '/api/v1/courses/{course_id}/students',
            path: {
                'course_id': courseId,
            },
            errors: {
                403: `Insufficient permission`,
                404: `Course does not exist`,
                422: `Validation Error`,
                502: `Remote not responding`,
            },
        });
    }
    /**
     * Select Course
     * @param courseId
     * @param stuId
     * @returns void
     * @throws ApiError
     */
    public static selectCourseApiV1CoursesCourseIdSelectPost(
        courseId: number,
        stuId?: (number | null),
    ): CancelablePromise<void> {
        return __request(OpenAPI, {
            method: 'POST',
            url: '/api/v1/courses/{course_id}/select',
            path: {
                'course_id': courseId,
            },
            query: {
                'stu_id': stuId,
            },
            errors: {
                403: `Insufficient permission`,
                404: `Course or student does not exist`,
                409: `Course capacity conflict or already selected`,
                422: `Validation Error`,
                502: `Remote not responding`,
            },
        });
    }
    /**
     * Deselect Course
     * @param courseId
     * @param stuId
     * @returns void
     * @throws ApiError
     */
    public static deselectCourseApiV1CoursesCourseIdDeselectPost(
        courseId: number,
        stuId?: (number | null),
    ): CancelablePromise<void> {
        return __request(OpenAPI, {
            method: 'POST',
            url: '/api/v1/courses/{course_id}/deselect',
            path: {
                'course_id': courseId,
            },
            query: {
                'stu_id': stuId,
            },
            errors: {
                403: `Insufficient permission`,
                404: `Course or student does not exist`,
                422: `Validation Error`,
                502: `Remote not responding`,
            },
        });
    }
}
