/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceProgressUtils.estimateTimeToCompletionInMinutes;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KafkaRebalanceProgressUtilsTest {

    @Test
    public void testEstimateTimeToCompletionInMinutes() {
        long currentTime = Instant.now().getEpochSecond();

        assertThat(estimateTimeToCompletionInMinutes(currentTime - 1, currentTime, 1000, 10), is(1));
        assertThat(estimateTimeToCompletionInMinutes(currentTime - 60, currentTime, 1000, 10), is(99));
        assertThat(estimateTimeToCompletionInMinutes(currentTime - 60, currentTime, 1000, 500), is(0));

        assertThat(estimateTimeToCompletionInMinutes(currentTime - 60, currentTime, 1000000, 100), is(9999));
        assertThat(estimateTimeToCompletionInMinutes(currentTime - 60, currentTime, 1000, 990), is(0));
        assertThat(estimateTimeToCompletionInMinutes(currentTime - 1000, currentTime, Integer.MAX_VALUE, Integer.MAX_VALUE / 2), is(16));
        assertThat(estimateTimeToCompletionInMinutes(currentTime - 1000, currentTime, 1000, 10), is(1650));
        assertThat(estimateTimeToCompletionInMinutes(currentTime - 3600, currentTime, 1, 1), is(0));
        assertThat(estimateTimeToCompletionInMinutes(currentTime - (3600 * 24 * 30), currentTime, 1000, 500), is(43200));

        assertThrows(IllegalArgumentException.class, () -> estimateTimeToCompletionInMinutes(currentTime + 1, currentTime, 1000, 10));
        assertThrows(IllegalArgumentException.class, () -> estimateTimeToCompletionInMinutes(currentTime - 3600, currentTime, -1000, 10));
        assertThrows(IllegalArgumentException.class, () -> estimateTimeToCompletionInMinutes(currentTime - (3600 * 24 * 30), currentTime, 1000, -1));

        assertThrows(ArithmeticException.class, () -> estimateTimeToCompletionInMinutes(currentTime, currentTime, 1000, 10));
        assertThrows(ArithmeticException.class, () -> estimateTimeToCompletionInMinutes(currentTime - 3600, currentTime,  0, 0));
        assertThrows(ArithmeticException.class, () -> estimateTimeToCompletionInMinutes(currentTime - 3600 * 10, currentTime, 1000, 0));
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
