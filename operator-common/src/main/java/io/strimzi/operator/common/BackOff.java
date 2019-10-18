/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

/**
 * <p>Encapsulates computing delays for an exponential back-off, when an operation has to be retried.
 * The {@link #delayMs()} method will return an increasing delay to be used between operation attempts.</p>
 * <pre>{@literal
 *     |<-attempt->    |<-attempt->        |<-attempt->|*fail*
 *     |<-- delayMs -->|<---- delayMs ---->|           |}</pre>
 * <p>The delays after the 0th attempt is always 0ms and the remaining delays are computed by the formula:</p>
 * <pre>  delayMs(n) = scaleMs * base ^ attempt</pre>
 * <p>Thus the delay after the 1st attempt is {@code scaleMs}, and after the 2nd attempt is {@code scaleMs * base}.</p>
 */
public class BackOff {

    private static final long DEFAULT_SCALE_MS = 200L;
    private static final int DEFAULT_BASE = 2;
    private static final int DEFAULT_MAX_ATTEMPTS = 6;

    private final long scaleMs;
    private final int base;
    private final int maxAttempts;
    private int attempt = 0;

    /**
     * Computes delays according to {@code 200 * 2^attempt} with a maximum of 6 attempts.
     */
    public BackOff() {
        this(DEFAULT_SCALE_MS, DEFAULT_BASE, DEFAULT_MAX_ATTEMPTS);
    }

    /**
     * Computes delays according to {@code 200 * 2^attempt} with the given maximum number of attempts.
     * @param maxAttempts The maximum number of attempts.
     */
    public BackOff(int maxAttempts) {
        this(DEFAULT_SCALE_MS, DEFAULT_BASE, maxAttempts);
    }

    /**
     * Computes delays according to {@code scaleMs * base^attempt} with the given maximum number of attempts.
     * @param scaleMs The scale.
     * @param base The base.
     * @param maxAttempts The maximum number of attempts to make before {@code MaxAttemptsExceededException} is thrown.
     */
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
     * @return The maximum number of attempts.
     */
    public int maxAttempts() {
        return maxAttempts;
    }

    /**
     * @return The maximum number of delays.
     */
    public int maxNumDelays() {
        return maxAttempts - 1;
    }

    /**
     * Return the next delay to use, in milliseconds.
     * The first delay is always zero, the 2nd delay is scaleMs, and the delay increases exponentially from there.
     * @return Return the next delay to use, in milliseconds.
     * @throws MaxAttemptsExceededException if the next attempt would exceed the configured number of attempts.
     */
    public long delayMs() {
        if (attempt == maxAttempts) {
            throw new MaxAttemptsExceededException();
        }
        return delay(attempt++);
    }

    /**
     * @return Whether the next call to {@link #delayMs()} will throw MaxAttemptsExceededException.
     */
    public boolean done() {
        return attempt >= maxAttempts;
    }

    /**
     * @return How much delay for attempt n?
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
     * @return The cumulative delay issued by {@link #delayMs()} so far.
     */
    public long cumulativeDelayMs() {
        return cumulativeDelayAttemptMs(attempt);
    }

    /**
     * The total possible delay for this BackOff.
     * This does not depend on how much delay has already been issued.
     * @return The total possible delay for this BackOff.
     */
    public long totalDelayMs() {
        return cumulativeDelayAttemptMs(maxAttempts);
    }
}
