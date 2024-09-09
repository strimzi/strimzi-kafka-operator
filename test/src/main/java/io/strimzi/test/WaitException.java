/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test;

/**
 * Exception used to indicate that waiting for an event failed
 */
public class WaitException extends RuntimeException {
    /**
     * Constructor
     *
     * @param message   Exception message
     */
    public WaitException(String message) {
        super(message);
    }

    /**
     * Constructor
     *
     * @param cause     Cause of the exception
     */
    public WaitException(Throwable cause) {
        super(cause);
    }
}