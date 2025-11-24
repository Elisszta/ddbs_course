/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
export type UserLoginResp = {
    token: string;
    user_id: number;
    role: UserLoginResp.role;
    username: string;
};
export namespace UserLoginResp {
    export enum role {
        TEACHER = 'teacher',
        STUDENT = 'student',
        ADMIN = 'admin',
    }
}

