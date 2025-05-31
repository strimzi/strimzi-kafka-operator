/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceProgressUtils.estimateTimeToCompletionInMinutes;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KafkaRebalanceProgressUtilsTest {

    @Test
    public void testEstimateTimeToCompletionInMinutes() {
        Instant currentTime = Instant.now();

        /*
         * Total Data to Move:          1000 MB
         * Data Moved:                  10 MB
         * Time Since Task Start:       1 second
         * Data Rate:                   10 MB/s
         * Remaining Data:              990 MB
         * Estimated Time to Complete:  99 seconds -> 2 minutes
         */
        assertThat(estimateTimeToCompletionInMinutes(currentTime.minusSeconds(1), currentTime, 1000, 10), is(2));
        /*
         * Total Data to Move:          1000 MB
         * Data Moved:                  10 MB
         * Time Since Task Start:       60 seconds
         * Data Rate:                   0.167 MB/s
         * Remaining Data:              990 MB
         * Estimated Time to Complete:  5940 seconds -> 99 minutes
         */
        assertThat(estimateTimeToCompletionInMinutes(currentTime.minusSeconds(60), currentTime, 1000, 10), is(99));
        /*
         * Total Data to Move:          1000 MB
         * Data Moved:                  500 MB
         * Time Since Task Start:       60 seconds
         * Data Rate:                   8.33 MB/s
         * Remaining Data:              500 MB
         * Estimated Time to Complete:  60 seconds -> 1 minute
         */
        assertThat(estimateTimeToCompletionInMinutes(currentTime.minusSeconds(60), currentTime, 1000, 500), is(1));
        /*
         * Total Data to Move:          1,000,000 MB
         * Data Moved:                  100 MB
         * Time Since Task Start:       60 seconds
         * Data Rate:                   1.6666666666666667 MB/s
         * Remaining Data:              999,900 MB
         * Estimated Time to Complete:  599,940 seconds -> 9999 minutes
         */
        assertThat(estimateTimeToCompletionInMinutes(currentTime.minusSeconds(60), currentTime, 1000000, 100), is(9999));
        /*
         * Total Data to Move:          1000 MB
         * Data Moved:                  990 MB
         * Time Since Task Start:       60 seconds
         * Data Rate:                   16.5 MB/s
         * Remaining Data:              10 MB
         * Estimated Time to Complete:  ~0.6 seconds -> 0 minutes
         */
        assertThat(estimateTimeToCompletionInMinutes(currentTime.minusSeconds(60), currentTime, 1000, 990), is(0));
        /*
         * Total Data to Move:          2,147,483,647 MB (Integer.MAX_VALUE)
         * Data Moved:                  1,073,741,823.5 MB
         * Time Since Task Start:       1000 seconds
         * Data Rate:                   1073741.823 MB/s
         * Remaining Data:              1,073,741,824.5 MB
         * Estimated Time to Complete:  ~1000 seconds -> 17 minutes
         */
        assertThat(estimateTimeToCompletionInMinutes(currentTime.minusSeconds(1000), currentTime, Integer.MAX_VALUE, Integer.MAX_VALUE / 2), is(17));
        /*
         * Total Data to Move:          1000 MB
         * Data Moved:                  10 MB
         * Time Since Task Start:       1000 seconds
         * Data Rate:                   0.01 MB/s
         * Remaining Data:              990 MB
         * Estimated Time to Complete:  99000 seconds -> 1650 minutes
         */
        assertThat(estimateTimeToCompletionInMinutes(currentTime.minusSeconds(1000), currentTime, 1000, 10), is(1650));
        /*
         * Total Data to Move:          1 MB
         * Data Moved:                  1 MB
         * Time Since Task Start:       1 hour (3600 seconds)
         * Data Rate:                   0.000278 MB/s
         * Remaining Data:              0 MB
         * Estimated Time to Complete:  0 seconds -> 0 minutes
         */
        assertThat(estimateTimeToCompletionInMinutes(currentTime.minus(1, ChronoUnit.HOURS), currentTime, 1, 1), is(0));
        /*
        * Total Data to Move:          1000 MB
        * Data Moved:                  500 MB
        * Time Since Task Start:       30 days (2,592,000 seconds)
        * Data Rate:                   ~0.000193 MB/s
        * Remaining Data:              500 MB
        * Estimated Time to Complete:  2,592,000 seconds -> 43,200 minutes
        */
        assertThat(estimateTimeToCompletionInMinutes(currentTime.minus(30, ChronoUnit.DAYS), currentTime, 1000, 500), is(43200));

        assertThrows(IllegalArgumentException.class, () -> estimateTimeToCompletionInMinutes(currentTime.plusSeconds(1), currentTime, 1000, 10));
        assertThrows(IllegalArgumentException.class, () -> estimateTimeToCompletionInMinutes(currentTime.minus(1, ChronoUnit.HOURS), currentTime, -1000, 10));
        assertThrows(IllegalArgumentException.class, () -> estimateTimeToCompletionInMinutes(currentTime.minus(30, ChronoUnit.DAYS), currentTime, 1000, -1));

        assertThrows(IllegalArgumentException.class, () -> estimateTimeToCompletionInMinutes(currentTime, currentTime, 1000, 10));
        assertThrows(IllegalArgumentException.class, () -> estimateTimeToCompletionInMinutes(currentTime.minus(1, ChronoUnit.HOURS), currentTime,  0, 0));
        assertThrows(IllegalArgumentException.class, () -> estimateTimeToCompletionInMinutes(currentTime.minus(10, ChronoUnit.HOURS), currentTime, 1000, 0));
    }

    @Test
    public void testEstimateCompletedByteMovementPercentage() {
        assertThat(KafkaRebalanceProgressUtils.estimateCompletedByteMovementPercentage(100, 50), is(50));
        assertThat(KafkaRebalanceProgressUtils.estimateCompletedByteMovementPercentage(100, 0), is(0));
        assertThat(KafkaRebalanceProgressUtils.estimateCompletedByteMovementPercentage(100, 100), is(100));
        assertThat(KafkaRebalanceProgressUtils.estimateCompletedByteMovementPercentage(0, 0), is(0));

        // Ensure exception is thrown when finishedDataMovement > totalDataToMove
        assertThrows(IllegalArgumentException.class, () -> {
            KafkaRebalanceProgressUtils.estimateCompletedByteMovementPercentage(100, 150);
        });

        // Ensure exception is thrown when finishedDataMovement is negative
        assertThrows(IllegalArgumentException.class, () -> {
            KafkaRebalanceProgressUtils.estimateCompletedByteMovementPercentage(100, -1);
        });

        // Ensure exception is thrown when totalDataToMove is negative
        assertThrows(IllegalArgumentException.class, () -> {
            KafkaRebalanceProgressUtils.estimateCompletedByteMovementPercentage(-100, 50);
        });
    }
}
