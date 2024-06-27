/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.model;

import org.apache.kafka.common.errors.ApiException;

/**
 * Exception which maps to a condition.
 */
public abstract class TopicOperatorException extends Exception {
    /** Possible error reasons. */
    public enum Reason {
        /** Conflict while reconciling the resource. */
        RESOURCE_CONFLICT("ResourceConflict"),
        /** Operation not supported. */
        NOT_SUPPORTED("NotSupported"),
        /** Kafka error wrapper. */
        KAFKA_ERROR("KafkaError"),
        /** Internal server error. */
        INTERNAL_ERROR("InternalError");
        
        /** Reason as a string. */
        public final String value;
        
        Reason(String value) {
            this.value = value;
        }
    }

    /**
     * The error reason.
     */
    private final Reason reason;

    /**
     * @param reason Error reason.
     * @param message Error message.
     */
    public TopicOperatorException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }

    /**
     * @param cause The error cause.
     */
    public TopicOperatorException(ApiException cause) {
        super(cause);
        this.reason = Reason.KAFKA_ERROR;
    }

    /**
     * @return The error reason as string.
     */
    public String reason() {
        return reason.value;
    }

    /**
     * Resource conflict.
     */
    public static class ResourceConflict extends TopicOperatorException {
        /**
         * @param message The error message.
         */
        public ResourceConflict(String message) {
            super(Reason.RESOURCE_CONFLICT, message);
        }
    }

    /**
     * Kafka error.
     */
    public static class KafkaError extends TopicOperatorException {
        /**
         * @param cause The Kafka API exception.
         */
        public KafkaError(ApiException cause) {
            super(cause);
        }
    }

    /**
     * Internal error.
     */
    public static class InternalError extends TopicOperatorException {
        /**
         * @param throwable The throwable.
         */
        public InternalError(Throwable throwable) {
            super(Reason.INTERNAL_ERROR, throwable.toString());
        }
    }

    /**
     * Not supported.
     */
    public static class NotSupported extends TopicOperatorException {
        /**
         * @param message The error message.
         */
        public NotSupported(String message) {
            super(Reason.NOT_SUPPORTED, message);
        }
    }
}
