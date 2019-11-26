/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.executor;

import java.io.Serializable;

public class ExecResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int returnCode;
    private final String stdOut;
    private final String stdErr;

    ExecResult(int returnCode, String stdOut, String stdErr) {
        this.returnCode = returnCode;
        this.stdOut = stdOut;
        this.stdErr = stdErr;
    }

    public boolean exitStatus() {
        return returnCode == 0;
    }

    public int returnCode() {
        return returnCode;
    }

    public String out() {
        return stdOut;
    }

    public String err() {
        return stdErr;
    }
}
