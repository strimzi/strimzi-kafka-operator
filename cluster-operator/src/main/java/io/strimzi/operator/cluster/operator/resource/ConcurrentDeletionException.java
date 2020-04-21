/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

/**
 * Thrown for exceptional circumstances when deleting already deleted (at that time) resource.
 */
public class ConcurrentDeletionException extends RuntimeException {
    public ConcurrentDeletionException() {
        super();
    }
    public ConcurrentDeletionException(String s) {
        super(s);
    }
    public ConcurrentDeletionException(Throwable cause) {
        super(cause);
    }
    public ConcurrentDeletionException(String message, Throwable cause) {
        super(message, cause);
    }
}