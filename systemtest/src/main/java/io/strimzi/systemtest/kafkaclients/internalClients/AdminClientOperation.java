/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.internalClients;

public enum AdminClientOperation {
    CREATE_TOPICS("create"),
    DELETE_TOPICS("delete"),
    LIST_TOPICS("list"),
    ALTER_TOPICS("alter"),
    HELP("help");

    private final String operation;

    AdminClientOperation(String operation) {
        this.operation = operation;
    }

    @Override
    public String toString() {
        return operation;
    }
}