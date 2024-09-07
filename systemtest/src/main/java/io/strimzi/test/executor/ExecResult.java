/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.executor;

import java.io.Serializable;

/**
 * Result of an execution of an command
 */
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

    /**
     * @return True if the command succeeded. False otherwise.
     */
    public boolean exitStatus() {
        return returnCode == 0;
    }

    /**
     * @return  The command return code
     */
    public int returnCode() {
        return returnCode;
    }

    /**
     * @return  The standard output of the command
     */
    public String out() {
        return stdOut;
    }

    /**
     * @return  The error output of the command
     */
    public String err() {
        return stdErr;
    }
}
