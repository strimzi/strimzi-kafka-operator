/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

/**
 * Thrown to indicate that given Kafka version is not supported.
 */
public class UnsupportedVersionException extends Exception {
    /**
     * Constructor
     *
     * @param message   Error message
     */
    public UnsupportedVersionException(String message) {
        super(message);
    }
}
