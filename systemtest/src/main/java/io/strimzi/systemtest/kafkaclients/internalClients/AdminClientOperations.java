/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.internalClients;

public enum AdminClientOperations {
    CREATE_TOPICS("create"),
    DELETE_TOPICS("delete"),
    LIST_TOPICS("list"),
    UPDATE_TOPICS("update"),
    HELP("help");

    private final String operation;

    AdminClientOperations(String operation) {
        this.operation = operation;
    }

    @Override
    public String toString() {
        return operation;
    }
}