// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// TODO : Import from  SDK
import { VALDIATOR_NAME } from '~/pages/validator/ValidatorDataTypes';

export function getValidatorName(rawName: string | number[]) {
    let name: string;

    if (Array.isArray(rawName)) {
        name = String.fromCharCode(...rawName);
    } else {
        name = Buffer.from(rawName, 'base64').toString();
        if (!VALDIATOR_NAME.test(name)) {
            name = rawName;
        }
    }
    return name;
}