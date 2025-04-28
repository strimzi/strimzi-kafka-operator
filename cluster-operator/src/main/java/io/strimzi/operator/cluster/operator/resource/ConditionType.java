/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.api.kafka.model.common.Condition;

/**
 * Possible Condition types for a Condition in Strimzi custom resource status
 */
public enum ConditionType {
    /**
     * Indicates a Warning condition
     */
    WARNING("Warning"),
    /**
     * Indicates an Error condition
     */
    ERROR("Error"),
    /**
     * Indicates a Ready condition
     */
    READY("Ready");

    private final String type;

    ConditionType(String type) {
        this.type = type;
    }

    /**
     * Checks if the given condition is a warning condition.
     *
     * @param condition The condition to check.
     * @return {@code true} if the condition is of type {@code WARNING}, {@code false} otherwise.
     */
    public static boolean isWarning(Condition condition) {
        return WARNING.type.equals(condition.getType());
    }
}
