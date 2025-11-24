/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
export type CourseCreateParams = {
    name: string;
    capacity: number;
    teacher_ids: Array<number>;
    campus: CourseCreateParams.campus;
};
export namespace CourseCreateParams {
    export enum campus {
        A = 'A',
        B = 'B',
        C = 'C',
    }
}

