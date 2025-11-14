/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance;

import java.time.LocalDateTime;
import java.time.temporal.TemporalAccessor;

/**
 * Shared timestamp for all performance tests to ensure unified logging across systemtest/target/performance/*
 * The timestamp is reset at the start of each test run (including maven reruns) so each run gets a unique timestamp,
 * but all tests within that run share the same timestamp.
 */
public final class TimeHolder {
    private TimeHolder() {}

    private static TemporalAccessor actualTime = LocalDateTime.now();

    public static TemporalAccessor getActualTime() {
        return actualTime;
    }

    /**
     * Resets the timestamp to the current time. Called by the test execution listener at the start of each test run.
     */
    public static void resetTimestamp() {
        actualTime = LocalDateTime.now();
    }
}