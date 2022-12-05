/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

/**
 * Enum for ImagePullPolicy types. Supports the 3 types supported in Kubernetes / OpenShift:
 * - Always
 * - Never
 * - IfNotPresent
 */
public enum ImagePullPolicy {
    /**
     * Always pull the container image
     */
    ALWAYS("Always"),

    /**
     * Pull the container image when it is not present
     */
    IFNOTPRESENT("IfNotPresent"),

    /**
     * Never pull the container image
     */
    NEVER("Never");

    private final String imagePullPolicy;

    ImagePullPolicy(String imagePullPolicy) {
        this.imagePullPolicy = imagePullPolicy;
    }

    @Override
    public String toString()    {
        return imagePullPolicy;
    }
}
