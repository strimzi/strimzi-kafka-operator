/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model.cruisecontrol;

/**
 * Enum with Cruise Control headers
 */
public enum CruiseControlHeaders {
    /**
     * User task id
     */
    USER_TASK_ID_HEADER("User-Task-ID");

    private final String name;

    /**
     * Creates the Enum from String
     *
     * @param name String with the path
     */
    CruiseControlHeaders(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
