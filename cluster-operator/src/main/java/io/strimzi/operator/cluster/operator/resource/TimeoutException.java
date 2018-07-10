/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

/**
 * Thrown to indicate that timeout has been exceeded.
 */
public class TimeoutException extends RuntimeException {
    public TimeoutException() {
        super();
    }

    public TimeoutException(String message) {
        super(message);
    }
}
