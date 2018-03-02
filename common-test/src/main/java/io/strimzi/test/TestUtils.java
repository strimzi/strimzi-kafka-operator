/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;

public final class TestUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestUtils.class);

    private TestUtils() {
        // All static methods
    }

    /** Returns a Map of the given sequence of key, value pairs. */
    public static <T> Map<T, T> map(T... pairs) {
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException();
        }
        Map<T, T> result = new HashMap<>(pairs.length / 2);
        for (int i = 0; i < pairs.length; i += 2) {
            result.put(pairs[i], pairs[i + 1]);
        }
        return result;
    }

    /**
     * Poll the given {@code ready} function every {@code pollIntervalMs} milliseconds until it returns true,
     * or throw a TimeoutException if it doesn't returns true within {@code timeoutMs} milliseconds.
     * @return The remaining time left until timeout occurs
     * (helpful if you have several calls which need to share a common timeout),
     * */
    public static long waitFor(String description, long pollIntervalMs, long timeoutMs, BooleanSupplier ready) {
        LOGGER.debug("Waiting for {}", description);
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (true) {
            boolean result = ready.getAsBoolean();
            long timeLeft = deadline - System.currentTimeMillis();
            if (result) {
                return timeLeft;
            }
            if (timeLeft <= 0) {
                throw new TimeoutException("Timeout after " + timeoutMs + " ms waiting for " + description + " to be ready");
            }
            long sleepTime = Math.min(pollIntervalMs, timeLeft);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("{} not ready, will try again in {} ms ({}ms till timeout)", description, sleepTime, timeLeft);
            }
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                return deadline - System.currentTimeMillis();
            }
        }
    }
}
