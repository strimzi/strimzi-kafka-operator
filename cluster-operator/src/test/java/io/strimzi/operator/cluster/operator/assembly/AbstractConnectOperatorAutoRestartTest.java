/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.connector.AutoRestartStatusBuilder;
import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * This class tests the shared methods related to connector auto-restarting that are used in the same way by both
 * Kafka Connect and its connectors and Kafka Mirror Maker 2.
 */
public class AbstractConnectOperatorAutoRestartTest {
    @Test
    public void testShouldAutoRestartConnector() {
        // Should restart after minute 2 when auto restart count is 1
        var autoRestartStatus =  new AutoRestartStatusBuilder()
            .withCount(1)
            .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(3).format(DateTimeFormatter.ISO_INSTANT))
            .build();
        assertThat(AbstractConnectOperator.shouldAutoRestart(autoRestartStatus, null), is(true));

        // Should not restart before minute 2 when auto restart count is 1
        autoRestartStatus =  new AutoRestartStatusBuilder()
            .withCount(1)
            .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(1).format(DateTimeFormatter.ISO_INSTANT))
            .build();
        assertThat(AbstractConnectOperator.shouldAutoRestart(autoRestartStatus, null), is(false));

        // Should restart after minute 12 when auto restart count is 3
        autoRestartStatus =  new AutoRestartStatusBuilder()
            .withCount(3)
            .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(13).format(DateTimeFormatter.ISO_INSTANT))
            .build();
        assertThat(AbstractConnectOperator.shouldAutoRestart(autoRestartStatus, null), is(true));

        // Should not restart before minute 12 when auto restart count is 3
        autoRestartStatus =  new AutoRestartStatusBuilder()
            .withCount(3)
            .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(10).format(DateTimeFormatter.ISO_INSTANT))
            .build();
        assertThat(AbstractConnectOperator.shouldAutoRestart(autoRestartStatus, null), is(false));

        // Should restart after minute 61 when auto restart count is 25
        autoRestartStatus =  new AutoRestartStatusBuilder()
                .withCount(25)
                .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(61).format(DateTimeFormatter.ISO_INSTANT))
                .build();
        assertThat(AbstractConnectOperator.shouldAutoRestart(autoRestartStatus, null), is(true));

        // Should not restart after 59 minutes when auto restart count is 25
        autoRestartStatus =  new AutoRestartStatusBuilder()
                .withCount(25)
                .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(59).format(DateTimeFormatter.ISO_INSTANT))
                .build();
        assertThat(AbstractConnectOperator.shouldAutoRestart(autoRestartStatus, null), is(false));

        // Should not restart after 6 attempts
        autoRestartStatus =  new AutoRestartStatusBuilder()
            .withCount(7)
            .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusDays(1).format(DateTimeFormatter.ISO_INSTANT))
            .build();
        assertThat(AbstractConnectOperator.shouldAutoRestart(autoRestartStatus, 7), is(false));

        // Should restart after 6 attempts when maxRestarts set to higher number
        autoRestartStatus =  new AutoRestartStatusBuilder()
                .withCount(7)
                .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusDays(1).format(DateTimeFormatter.ISO_INSTANT))
                .build();
        assertThat(AbstractConnectOperator.shouldAutoRestart(autoRestartStatus, 8), is(true));
    }

    @Test
    public void testShouldResetAutoRestartStatus() {
        // Should reset after minute 2 when auto restart count is 1
        var autoRestartStatus =  new AutoRestartStatusBuilder()
                .withCount(1)
                .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(3).format(DateTimeFormatter.ISO_INSTANT))
                .build();
        assertThat(AbstractConnectOperator.shouldResetAutoRestartStatus(autoRestartStatus), is(true));

        // Should not reset before minute 2 when auto restart count is 1
        autoRestartStatus =  new AutoRestartStatusBuilder()
                .withCount(1)
                .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(1).format(DateTimeFormatter.ISO_INSTANT))
                .build();
        assertThat(AbstractConnectOperator.shouldResetAutoRestartStatus(autoRestartStatus), is(false));

        // Should reset after minute 12 when auto restart count is 3
        autoRestartStatus =  new AutoRestartStatusBuilder()
                .withCount(3)
                .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(13).format(DateTimeFormatter.ISO_INSTANT))
                .build();
        assertThat(AbstractConnectOperator.shouldResetAutoRestartStatus(autoRestartStatus), is(true));

        // Should not reset before minute 12 when auto restart count is 3
        autoRestartStatus =  new AutoRestartStatusBuilder()
                .withCount(3)
                .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(10).format(DateTimeFormatter.ISO_INSTANT))
                .build();
        assertThat(AbstractConnectOperator.shouldResetAutoRestartStatus(autoRestartStatus), is(false));

        // Should reset after 60 minutes when auto restart count is 25
        autoRestartStatus =  new AutoRestartStatusBuilder()
                .withCount(25)
                .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(61).format(DateTimeFormatter.ISO_INSTANT))
                .build();
        assertThat(AbstractConnectOperator.shouldResetAutoRestartStatus(autoRestartStatus), is(true));

        // Should not reset after 59 minutes when auto restart count is 25
        autoRestartStatus =  new AutoRestartStatusBuilder()
                .withCount(25)
                .withLastRestartTimestamp(ZonedDateTime.now(ZoneOffset.UTC).minusMinutes(59).format(DateTimeFormatter.ISO_INSTANT))
                .build();
        assertThat(AbstractConnectOperator.shouldResetAutoRestartStatus(autoRestartStatus), is(false));
    }
}
