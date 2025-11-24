/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
export type StudentResp = {
    stu_id: number;
    name: string;
    sex: StudentResp.sex;
    age: number;
    current_campus: StudentResp.current_campus;
};
export namespace StudentResp {
    export enum sex {
        M = 'M',
        F = 'F',
    }
    export enum current_campus {
        A = 'A',
        B = 'B',
        C = 'C',
    }
}

