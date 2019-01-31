/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

/**
 * Encapsulates computing delays for an exponential back-off.
 */
public class BackOff {

    private static final long DEFAULT_SCALE_MS = 200L;
    private static final int DEFAULT_BASE = 2;
    private static final int DEFAULT_MAX_ATTEMPTS = 6;

    private final long scaleMs;
    private final int base;
    private final int maxAttempts;
    private int attempt = 0;

    public BackOff() {
        this(DEFAULT_SCALE_MS, DEFAULT_BASE, DEFAULT_MAX_ATTEMPTS);
    }

    public BackOff(int maxAttempts) {
        this(DEFAULT_SCALE_MS, DEFAULT_BASE, maxAttempts);
    }

    public BackOff(long scaleMs, int base, int maxAttempts) {
        if (scaleMs <= 0) {
            throw new IllegalArgumentException();
        }
        if (base <= 0) {
            throw new IllegalArgumentException();
        }
        if (maxAttempts <= 0) {
            throw new IllegalArgumentException();
        }
        this.scaleMs = scaleMs;
        this.base = base;
        this.maxAttempts = maxAttempts;
    }

    /**
     * The maximum number of attempts
     */
    public int maxAttempts() {
        return maxAttempts;
    }

    /**
     * The maximum number of attempts
     */
    public int maxNumDelays() {
        return maxAttempts - 1;
    }

    /**
     * Return the next delay to use, in milliseconds.
     * The first delay is always zero, the 2nd delay is scaleMs, and the delay increases exponentially from there.
     * @throws MaxAttemptsExceededException if the next attempt would exceed the configured number of attempts.
     */
    public long delayMs() {
        if (attempt == maxAttempts) {
            throw new MaxAttemptsExceededException();
        }
        return delay(attempt++);
    }

    /**
     * Returns whether the next call to {@link #delayMs()} will throw MaxAttemptsExceededException.
     */
    public boolean done() {
        return attempt >= maxAttempts;
    }

    /**
     * How much delay for attempt n?
     */
    private long delay(int n) {
        if (n == 0) {
            return 0L;
        }
        int pow = 1;
        while (n-- > 1) {
            pow *= base;
        }
        return scaleMs * pow;
    }

    private long cumulativeDelayAttemptMs(int numDelays) {
        if (numDelays > maxAttempts) {
            throw new MaxAttemptsExceededException();
        }
        long total = 0;
        for (int i = 0; i < numDelays; i++) {
            total += delay(i);
        }
        return total;
    }

    /**
     * The cumulative delay issued by {@link #delayMs()} so far.
     */
    public long cumulativeDelayMs() {
        return cumulativeDelayAttemptMs(attempt);
    }

    /**
     * The total possible delay for this BackOff.
     * This does not depend on how much delay has already been issued.
     */
    public long totalDelayMs() {
        return cumulativeDelayAttemptMs(maxAttempts);
    }
}
