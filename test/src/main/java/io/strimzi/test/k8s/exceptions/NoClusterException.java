/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s.exceptions;

public class NoClusterException extends RuntimeException {
    public NoClusterException(String message) {
        super(message);
    }
}
