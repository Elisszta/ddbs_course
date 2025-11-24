/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { SelectionBatchCreateParams } from '../models/SelectionBatchCreateParams';
import type { SelectionBatchQueryResp } from '../models/SelectionBatchQueryResp';
import type { SelectionBatchResp } from '../models/SelectionBatchResp';
import type { CancelablePromise } from '../core/CancelablePromise';
import { OpenAPI } from '../core/OpenAPI';
import { request as __request } from '../core/request';
export class SelectionBatchApiService {
    /**
     * Get Selection Batch
     * @returns SelectionBatchQueryResp Successful Response
     * @throws ApiError
     */
    public static getSelectionBatchApiV1SelectionBatchesGet(): CancelablePromise<SelectionBatchQueryResp> {
        return __request(OpenAPI, {
            method: 'GET',
            url: '/api/v1/selection-batches',
            errors: {
                403: `Insufficient permission`,
            },
        });
    }
    /**
     * Create Selection Batch
     * @param requestBody
     * @returns SelectionBatchResp Successful Response
     * @throws ApiError
     */
    public static createSelectionBatchApiV1SelectionBatchesPost(
        requestBody: SelectionBatchCreateParams,
    ): CancelablePromise<SelectionBatchResp> {
        return __request(OpenAPI, {
            method: 'POST',
            url: '/api/v1/selection-batches',
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                403: `Insufficient permission`,
                422: `Validation Error`,
            },
        });
    }
    /**
     * Delete Selection Batch
     * @param batchId
     * @returns void
     * @throws ApiError
     */
    public static deleteSelectionBatchApiV1SelectionBatchesBatchIdDelete(
        batchId: number,
    ): CancelablePromise<void> {
        return __request(OpenAPI, {
            method: 'DELETE',
            url: '/api/v1/selection-batches/{batch_id}',
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
