/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

public class InvalidResourceException extends RuntimeException {
    public InvalidResourceException() {
        super();
    }

    protected InvalidResourceException(String s) {
    }
}
