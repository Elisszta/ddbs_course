/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
export type StudentCreateParams = {
    year: number;
    name: string;
    sex: StudentCreateParams.sex;
    age: number;
    current_campus: StudentCreateParams.current_campus;
};
export namespace StudentCreateParams {
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

