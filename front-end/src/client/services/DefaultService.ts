/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { UserLoginParams } from '../models/UserLoginParams';
import type { UserLoginResp } from '../models/UserLoginResp';
import type { CancelablePromise } from '../core/CancelablePromise';
import { OpenAPI } from '../core/OpenAPI';
import { request as __request } from '../core/request';
export class DefaultService {
    /**
     * Login
     * @param requestBody
     * @returns UserLoginResp Successful Response
     * @throws ApiError
     */
    public static loginApiV1LoginPost(
        requestBody: UserLoginParams,
    ): CancelablePromise<UserLoginResp> {
        return __request(OpenAPI, {
            method: 'POST',
            url: '/api/v1/login',
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                403: `Insufficient permission`,
                422: `Validation Error`,
            },
        });
    }
}
