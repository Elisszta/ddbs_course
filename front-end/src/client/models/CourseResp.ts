/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
export type CourseResp = {
    course_id: number;
    teachers: string;
    name: string;
    capacity: number;
    num_selected: number;
    campus: CourseResp.campus;
    is_selected?: (boolean | null);
};
export namespace CourseResp {
    export enum campus {
        A = 'A',
        B = 'B',
        C = 'C',
    }
}

