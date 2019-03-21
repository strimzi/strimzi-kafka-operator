/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

public class AbortRollException extends RuntimeException {
    public AbortRollException(String message) {
        super(message);
    }
}
