/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.strimzi.api.kafka.model.status.Status;

public class ReconciliationException extends Exception {
    private final Status status;

    public ReconciliationException(Status status, Throwable cause) {
        super(cause);
        this.status = status;
    }

    public Status getStatus() {
        return status;
    }
}
