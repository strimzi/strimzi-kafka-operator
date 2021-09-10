/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.utils;

/**
 * Extension for RuntimeException used in active waiting @see TestUtils.waitFor(...) method.
 * Usage of this Exception should be always associated with active waiting where the condition
 * should not always be met which results in WaitException.
 */
public class WaitException extends RuntimeException {
    public WaitException(String message) {
        super(message);
    }

    public WaitException(Throwable cause) {
        super(cause);
    }
}