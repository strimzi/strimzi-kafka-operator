/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

import java.io.Serializable;

public class ProcessResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int sc;
    private final String out;
    private final String err;

    ProcessResult(int sc, String out, String err) {
        this.sc = sc;
        this.out = out;
        this.err = err;
    }

    public int exitStatus() {
        return sc;
    }

    public String out() {
        return out;
    }

    public String err() {
        return err;
    }
}
