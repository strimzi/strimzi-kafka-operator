/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients;

public enum EClientType {

    BASIC("BASIC"),
    INTERNAL("INTERNAL"),
    TRACING("TRACING"),
    OAUTH("OAUTH");

    private String clientType;

    EClientType(String clientType) {
        this.clientType = clientType;
    }

    public String getClientType() {
        return this.clientType;
    }
}
