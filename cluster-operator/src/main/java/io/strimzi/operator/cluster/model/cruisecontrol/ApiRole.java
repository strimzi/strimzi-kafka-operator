/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

public enum ApiRole {
    VIEWER,
    USER,
    ADMIN,
    NONE;

    public static ApiRole forValue(String value) {
        switch (value) {
            case "VIEWER":
                return ApiRole.VIEWER;
            case "USER":
                return ApiRole.USER;
            case "ADMIN":
                return ApiRole.ADMIN;
            default:
                return ApiRole.NONE;
        }
    }
}
