/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

/**
 * Thrown to indicate that timeout has been exceeded.
 */
public class StrimziTimeoutException extends RuntimeException {
    /**
     * Constructs the TimeoutException
     */
    public StrimziTimeoutException() {
        super();
    }

    /**
     * Constructs the timeout exception
     *
     * @param message   Error message
     */
    public StrimziTimeoutException(String message) {
        super(message);
    }
}
