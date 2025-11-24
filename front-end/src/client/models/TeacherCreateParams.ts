/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
export type TeacherCreateParams = {
    id: number;
    name: string;
    sex: TeacherCreateParams.sex;
    age: number;
};
export namespace TeacherCreateParams {
    export enum sex {
        M = 'M',
        F = 'F',
    }
}

