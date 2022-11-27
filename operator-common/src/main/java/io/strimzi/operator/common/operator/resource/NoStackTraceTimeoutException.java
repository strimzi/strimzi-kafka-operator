/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import java.util.concurrent.TimeoutException;

// This class should be imported from Vertx but for some reason it is not possible
// https://github.com/eclipse-vertx/vert.x/blob/4.0.3/src/main/java/io/vertx/core/http/impl/NoStackTraceTimeoutException.java

/**
 * An exception without a stack trace
 */
public class NoStackTraceTimeoutException extends TimeoutException {
    /**
     * Constructor
     *
     * @param message   Error message
     */
    NoStackTraceTimeoutException(String message) {
        super(message);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
