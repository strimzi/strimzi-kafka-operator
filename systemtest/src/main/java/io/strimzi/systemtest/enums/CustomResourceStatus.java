/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.enums;

public enum CustomResourceStatus {
    Ready("Ready", "True"),
    NotReady("NotReady", "True"),
    Warning("Warning", "True");

    private final String type;
    private final String status;

    CustomResourceStatus(String type, String status) {
        this.type = type;
        this.status = status;
    }

    public String getType() {
        return type;
    }

    public String getStatus() {
        return status;
    }
}
