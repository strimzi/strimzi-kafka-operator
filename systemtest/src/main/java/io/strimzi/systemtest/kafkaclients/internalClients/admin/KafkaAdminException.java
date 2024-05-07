/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.internalClients.admin;

/**
 * Represents exceptions that are specific to Kafka admin operations. e.g. describing none existing topics
 */
class KafkaAdminException extends RuntimeException {
    public KafkaAdminException(String message) {
        super(message);
    }

    public KafkaAdminException(String message, Throwable cause) {
        super(message, cause);
    }
}
