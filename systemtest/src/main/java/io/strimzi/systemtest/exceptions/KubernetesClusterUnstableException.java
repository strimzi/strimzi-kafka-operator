/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.exceptions;

/**
 * Provides an exception, which is used in cases where cluster may not be responding and its most likely broken.
 * This could be caused by network or out of memory problem.
 */
public class KubernetesClusterUnstableException extends RuntimeException {

    public KubernetesClusterUnstableException(String message) {
        super(message);
    }
}

