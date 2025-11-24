/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
export type SelectionBatchResp = {
    batch_id: number;
    name: string;
    begin_time: string;
    end_time: string;
    status: SelectionBatchResp.status;
};
export namespace SelectionBatchResp {
    export enum status {
        PAST = 'past',
        CURRENT = 'current',
        FUTURE = 'future',
    }
}

