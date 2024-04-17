/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance;

public class OperationTimer {

    /**
     * Executes an operation and measures its execution time.
     *
     * @param operation The operation to execute and time.
     * @return The execution time of the operation in milliseconds.
     */
    public static long measureTimeInMillis(Runnable operation) {
        long startTime = System.currentTimeMillis();
        operation.run();
        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }
}
