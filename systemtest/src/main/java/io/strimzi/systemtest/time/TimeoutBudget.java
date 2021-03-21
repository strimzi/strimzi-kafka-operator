/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.time;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class TimeoutBudget {
    private long startTime;
    private long endTime;

    public TimeoutBudget(long timeout, TimeUnit timeUnit) {
        reset(timeout, timeUnit);
    }

    public static TimeoutBudget ofDuration(final Duration duration) {
        final long ms = duration.toMillis();
        if (ms < 0) {
            return new TimeoutBudget(duration.toNanos(), TimeUnit.NANOSECONDS);
        } else {
            return new TimeoutBudget(ms, TimeUnit.MILLISECONDS);
        }
    }

    public void reset(long timeout, TimeUnit timeUnit) {
        this.startTime = System.currentTimeMillis();
        this.endTime = startTime + timeUnit.toMillis(timeout);
    }

    public long timeLeft() {
        long diff = endTime - System.currentTimeMillis();
        if (diff >= 0) {
            return diff;
        } else {
            return -1;
        }
    }

    /**
     * Get the remaining time of the budget.
     * @return The remaining time.
     */
    public Duration remaining() {
        return Duration.ofMillis(timeLeft());
    }

    public boolean timeoutExpired() {
        return timeLeft() < 0;
    }

    public long timeSpent() {
        return System.currentTimeMillis() - startTime;
    }
}