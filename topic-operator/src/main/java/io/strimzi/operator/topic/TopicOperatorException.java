/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import org.apache.kafka.common.errors.ApiException;

/**
 * Exception which maps to a condition
 */
public abstract class TopicOperatorException extends Exception {

    enum Reason {
        RESOURCE_CONFLICT("ResourceConflict"),
        NOT_SUPPORTED("NotSupported"),
        KAFKA_ERROR("KafkaError"),
        INTERNAL_ERROR("InternalError");
        /**
         * The reason as a string
         */
        public final String reason;
        private Reason(String reason) {
            this.reason = reason;
        }
    }

    /**
     * The reason
     */
    private final Reason reason;

    TopicOperatorException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }

    TopicOperatorException(ApiException cause) {
        super(cause);
        this.reason = Reason.KAFKA_ERROR;
    }

    /**
     * @return The reason for the condition
     */
    public String reason() {
        return reason.reason;
    }

    static class ResourceConflict extends TopicOperatorException {
        ResourceConflict(String message) {
            super(Reason.RESOURCE_CONFLICT, message);
        }
    }

    static class KafkaError extends TopicOperatorException {
        KafkaError(ApiException cause) {
            super(cause);
        }
    }

    static class InternalError extends TopicOperatorException {
        InternalError(Throwable message) {
            super(Reason.INTERNAL_ERROR, message.toString());
        }
    }

    static class NotSupported extends TopicOperatorException {
        NotSupported(String message) {
            super(Reason.NOT_SUPPORTED, message);
        }
    }
}
