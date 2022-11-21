/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.model;

/**
 * Represents an exception raised when a certificate Secret is missing
 */
public class InvalidCertificateException extends RuntimeException {
    /**
     * Constructs the Invalid certificate exception
     *
     * @param message   Message with the error
     */
    public InvalidCertificateException(String message) {
        super(message);
    }
}
