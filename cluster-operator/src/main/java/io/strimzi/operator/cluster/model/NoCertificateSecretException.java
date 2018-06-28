/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

/**
 * Represents an exception raised when a certificate Secret is missing
 */
public class NoCertificateSecretException extends RuntimeException {

    public NoCertificateSecretException(String message) {
        super(message);
    }
}
