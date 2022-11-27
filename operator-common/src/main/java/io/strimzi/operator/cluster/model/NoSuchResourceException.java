/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

/**
 * Exception indicating the a resource does not exist
 */
public class NoSuchResourceException extends RuntimeException {
    /**
     * Constructor
     *
     * @param message   Error message
     */
    public NoSuchResourceException(String message) {
        super(message);
    }
}
