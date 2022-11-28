/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

/**
 * Represents an exception raised when invalid configuration is passed at operator startup
 */
public class InvalidConfigurationException extends RuntimeException {
    /**
     * Constructor
     *
     * @param message   Message describe the issue
     */
    public InvalidConfigurationException(String message) {
        super(message);
    }

    /**
     * Constructor
     *
     * @param cause     Cause of the issue
     */
    public InvalidConfigurationException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructor
     *
     * @param message   Message describing the issue
     * @param t         Cause of the issue
     */
    public InvalidConfigurationException(String message, Throwable t) {
        super(message, t);
    }
}
