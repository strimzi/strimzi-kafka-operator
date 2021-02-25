/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.converter;

/**
 * Exception used to indicate an conversion issue
 */
public class ApiConversionFailedException extends RuntimeException {
    public ApiConversionFailedException(String s) {
        super(s);
    }
}