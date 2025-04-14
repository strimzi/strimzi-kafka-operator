/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

/**
 * Utility class for handling progress fields of KafkaRebalance custom resource
 */
public class KafkaRebalanceProgressUtils {

    private static final int SECONDS_IN_A_MINUTE = 60;

    /**
     * Estimates the number of minutes it will take an ongoing partition rebalance to complete.
     *
     * @param taskStartTime The time when the task started, in seconds since the Unix epoch.
     * @param currentTime The current time, in seconds since the Unix epoch, at the moment of the method call.
     * @param totalDataToMove The total amount of data that needs to be moved, in megabytes.
     * @param finishedDataMovement The amount of data that has already been moved, in megabytes.
     * @return The estimated time to completion in minutes.
     * @throws IllegalArgumentException if:
     *     - Any of the method argument values are negative.
     *     - The value of taskStartTime is not greater than value of the currentTime.
     * @throws ArithmeticException if:
     *     - The elapsed time between `taskStartTime` and `currentTime` is zero, making rate calculation impossible.
     *     - The data movement rate is zero, making the time to completion estimation impossible.
     */
    /* test */ static int estimateTimeToCompletionInMinutes(long taskStartTime,
                                                        long currentTime,
                                                        int totalDataToMove,
                                                        int finishedDataMovement)
            throws IllegalArgumentException, ArithmeticException {
        if (taskStartTime < 0 || currentTime < 0 || totalDataToMove < 0 || finishedDataMovement < 0) {
            throw new IllegalArgumentException(
                    String.format("Invalid negative value(s) provided for one of the following arguments: taskStartTime: %d, currentTime: %d, totalDataToMove: %d, finishedDataMovement: %d.",
                    taskStartTime, currentTime, totalDataToMove, finishedDataMovement)
            );
        }
        if (!(currentTime >= taskStartTime)) {
            throw new IllegalArgumentException(
                    String.format("Invalid value(s) provided for the arguments taskStartTime: %d, currentTime: %d. " +
                            "The value of taskStartTime must be greater than the value  currentTime", taskStartTime, currentTime)
            );
        }
        // Calculate the seconds between the start time and the current time
        long timeElapsedInSeconds = currentTime - taskStartTime;
        if (timeElapsedInSeconds == 0) {
            throw new ArithmeticException("Time elapsed less than or equal to zero, cannot calculate byte movement rate.");
        }

        double rate = (double) finishedDataMovement / timeElapsedInSeconds;
        if (rate == 0) {
            throw new ArithmeticException("Byte movement rate is zero, cannot estimate time to completion.");
        }

        int dataLeftToMove = totalDataToMove - finishedDataMovement;
        return (int) (dataLeftToMove / (rate * SECONDS_IN_A_MINUTE));
    }

    /**
     * Estimates the percentage of data movement completed of an ongoing partition rebalance given the total number of
     * megabytes to be moved and the number of megabytes already moved as part of the rebalance.
     *
     * @param totalDataToMove The total amount of data that needs to be moved, in megabytes.
     * @param finishedDataMovement The amount of data that has already been moved, in megabytes.
     * @return The percentage of data movement completed as a rounded down integer between [0 and 100].
     * @throws IllegalArgumentException if `finishedDataMovement` is greater than `totalDataToMove` or if either value is negative.
     */
    /* test */ static int estimateCompletedByteMovementPercentage(int totalDataToMove, int finishedDataMovement) {
        if (!(finishedDataMovement <= totalDataToMove) || finishedDataMovement < 0) {
            throw new IllegalArgumentException(
                    String.format("Invalid value(s) provided for the arguments totalDataToMove: %d or finishedDataMovement: %d. " +
                                  "The value of finishedDataMovement must be less than or equal to totalDataToMove and both values must be non-negative",
                                  totalDataToMove, finishedDataMovement)
            );
        }
        return (int) ((double) finishedDataMovement / totalDataToMove * 100);
    }
}
