/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

/**
 * Retryable Cruise Control connection exception
 */
public class CruiseControlRetriableConnectionException extends RuntimeException {
    /**
     * Constructor
     *
     * @param cause     Cause of this error
     */
    public CruiseControlRetriableConnectionException(Throwable cause) {
        super(cause);
    }
}
