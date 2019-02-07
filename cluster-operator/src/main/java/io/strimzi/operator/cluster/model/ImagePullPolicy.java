/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

public enum ImagePullPolicy {
    ALWAYS("Always"),
    IFNOTPRESENT("IfNotPresent"),
    NEVER("Never");

    private final String imagePullPolicy;

    ImagePullPolicy(String imagePullPolicy) {
        this.imagePullPolicy = imagePullPolicy;
    }

    public String toString()    {
        return imagePullPolicy;
    }
}
