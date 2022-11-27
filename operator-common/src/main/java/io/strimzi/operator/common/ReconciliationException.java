/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.strimzi.api.kafka.model.status.Status;

/**
 * Custom exception which wraps the Custom Resource status together with the original exception which caused the issue.
 * This is used in situations where we need to indicate some problem but also pass the CR Status which should be set on
 * the custom resource.
 */
public class ReconciliationException extends Exception {
    /**
     * Status of the custom resource
     */
    private final Status status;

    /**
     * Creates new exception from the custom resource status and the original exception
     *
     * @param status    Status which should be set to the CR
     * @param cause     Exception which cause the reconciliation to fail
     */
    public ReconciliationException(Status status, Throwable cause) {
        super(cause);
        this.status = status;
    }

    /**
     * Returns the status accompanying the exception
     *
     * @return  Status from the exception
     */
    public Status getStatus() {
        return status;
    }
}
