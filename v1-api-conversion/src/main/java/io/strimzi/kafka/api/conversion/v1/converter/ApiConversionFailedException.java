/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter;

/**
 * Exception used to indicate a conversion issue
 */
public class ApiConversionFailedException extends RuntimeException {
    /**
     * Creates a new exception with the given message.
     *
     * @param s     The exception message
     */
    public ApiConversionFailedException(String s) {
        super(s);
    }
}