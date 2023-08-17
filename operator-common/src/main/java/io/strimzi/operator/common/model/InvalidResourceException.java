/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

/**
 * Exception thrown when the custom resource is invalid
 */
public class InvalidResourceException extends RuntimeException {
    /**
     * Constructor
     */
    public InvalidResourceException() {
        super();
    }

    /**
     * Constructor
     *
     * @param s Message describing the issue
     */
    public InvalidResourceException(String s) {
        super(s);
    }

    /**
     * Constructor
     *
     * @param cause Cause of the issue
     */
    public InvalidResourceException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructor
     *
     * @param message   Message describing the issue
     * @param cause     Cause of the issue
     */
    public InvalidResourceException(String message, Throwable cause) {
        super(message, cause);
    }
}
