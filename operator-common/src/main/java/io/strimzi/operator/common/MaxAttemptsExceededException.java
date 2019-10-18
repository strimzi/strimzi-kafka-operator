/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

/**
 * Thrown to indicate a {@link BackOff} has exceeded its maximum number of attempts.
 */
public class MaxAttemptsExceededException extends RuntimeException {
}
