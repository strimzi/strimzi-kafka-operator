/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import java.time.Duration;
import java.time.LocalDateTime;

/**
 * Utility class for handling progress fields of KafkaRebalance custom resource
 */
public class KafkaRebalanceProgressUtils {

    /**
     * Estimates the number of minutes it will take an ongoing partition rebalance to complete.
     *
     * @param taskStartTime The date time when the task started.
     * @param currentTime The date time at the moment of the method call.
     * @param totalDataToMoveInMB The total amount of data that needs to be moved, in megabytes.
     * @param finishedDataMovementInMB The amount of data that has already been moved, in megabytes.
     * @return The estimated time to completion in minutes.
     * @throws IllegalArgumentException if:
     *     - Any of the method argument values are negative.
     *     - The value of taskStartTime is not greater than value of the currentTime.
     * @throws ArithmeticException if:
     *     - The elapsed time between `taskStartTime` and `currentTime` is zero, making rate calculation impossible.
     *     - The data movement rate is zero, making the time to completion estimation impossible.
     */
    /* test */ static int estimateTimeToCompletionInMinutes(LocalDateTime taskStartTime,
                                                            LocalDateTime currentTime,
                                                            int totalDataToMoveInMB,
                                                            int finishedDataMovementInMB)
            throws IllegalArgumentException, ArithmeticException {
        if (totalDataToMoveInMB < 0 || finishedDataMovementInMB < 0) {
            throw new IllegalArgumentException(
                    String.format("Invalid negative value(s) provided for one of the following arguments: totalDataToMoveInMB: %d, finishedDataMovementInMB: %d.",
                            totalDataToMoveInMB, finishedDataMovementInMB)
            );
        }

        // Calculate the time elapsed between the start time and the current time
        Duration timeElapsed = Duration.between(taskStartTime, currentTime);

        if (timeElapsed.isNegative() || timeElapsed.isZero()) {
            throw new IllegalArgumentException(
                    String.format("Invalid time range: taskStartTime (%s) must be before currentTime (%s). Cannot calculate byte movement rate.",
                            taskStartTime, currentTime));
        }

        double rateMBperMinute = ((double) finishedDataMovementInMB / timeElapsed.getSeconds()) * 60;
        if (finishedDataMovementInMB == 0) {
            throw new ArithmeticException("finishedDataMovementInMB is zero, cannot estimate time to completion.");
        }

        int dataLeftToMoveMB = totalDataToMoveInMB - finishedDataMovementInMB;
        return (int) (dataLeftToMoveMB / (rateMBperMinute));
    }

    /**
     * Estimates the percentage of data movement completed of an ongoing partition rebalance given the total number of
     * megabytes to be moved and the number of megabytes already moved as part of the rebalance.
     *
     * @param totalDataToMoveInMB The total amount of data that needs to be moved, in megabytes.
     * @param finishedDataMovementInMB The amount of data that has already been moved, in megabytes.
     * @return The percentage of data movement completed as a rounded down integer between [0 and 100].
     * @throws IllegalArgumentException if `finishedDataMovementInMB` is greater than `totalDataToMoveInMB` or if either value is negative.
     */
    /* test */ static int estimateCompletedByteMovementPercentage(int totalDataToMoveInMB, int finishedDataMovementInMB) {
        if (finishedDataMovementInMB > totalDataToMoveInMB || finishedDataMovementInMB < 0) {
            throw new IllegalArgumentException(
                    String.format("Invalid value(s) provided for the arguments totalDataToMoveInMB: %d or finishedDataMovementInMB: %d. " +
                                  "The value of finishedDataMovementInMB must be less than or equal to totalDataToMoveInMB and both values must be non-negative",
                                  totalDataToMoveInMB, finishedDataMovementInMB)
            );
        }
        return (int) ((double) finishedDataMovementInMB / totalDataToMoveInMB * 100);
    }
}
