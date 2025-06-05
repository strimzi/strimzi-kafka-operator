/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model.resourcerequirements;

/**
 * Enum representing Kubernetes resource types used in container resource specifications.
 */
public enum ResourceType {
    /**
     * Represents the memory resource type (e.g., "memory").
     */
    MEMORY("memory"),

    /**
     * Represents the CPU resource type (e.g., "cpu").
     */
    CPU("cpu");

    private final String value;

    ResourceType(String value) {
        this.value = value;
    }

    /**
     * Returns the string value of the resource type.
     *
     * @return the resource type name as used in Kubernetes, e.g., "cpu" or "memory"
     */
    public String value() {
        return value;
    }
}
