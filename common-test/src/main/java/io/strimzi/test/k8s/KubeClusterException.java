/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

public class KubeClusterException extends RuntimeException {
    public final int statusCode;

    public KubeClusterException(int statusCode, String s) {
        super(s);
        this.statusCode = statusCode;
    }

    public KubeClusterException(Throwable cause) {
        super(cause);
        this.statusCode = -1;
    }
}
