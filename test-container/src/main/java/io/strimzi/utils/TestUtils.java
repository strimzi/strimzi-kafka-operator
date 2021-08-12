/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.function.BooleanSupplier;

public class TestUtils {

    private static final Logger LOGGER = LogManager.getLogger(TestUtils.class);

    /**
     * Poll the given {@code ready} function every {@code pollIntervalMs} milliseconds until it returns true,
     * or throw a WaitException if it doesn't returns true within {@code timeoutMs} milliseconds.
     * @return The remaining time left until timeout occurs
     * (helpful if you have several calls which need to share a common timeout),
     * */
    public static long waitFor(String description, long pollIntervalMs, long timeoutMs, BooleanSupplier ready) {
        return waitFor(description, pollIntervalMs, timeoutMs, ready, () -> { });
    }

    public static long waitFor(String description, long pollIntervalMs, long timeoutMs, BooleanSupplier ready, Runnable onTimeout) {
        LOGGER.debug("Waiting for {}", description);
        long deadline = System.currentTimeMillis() + timeoutMs;
        String exceptionMessage = null;
        int exceptionCount = 0;
        StringWriter stackTraceError = new StringWriter();

        while (true) {
            boolean result;
            try {
                result = ready.getAsBoolean();
            } catch (Exception e) {
                exceptionMessage = e.getMessage();
                if (++exceptionCount == 1 && exceptionMessage != null) {
                    // Log the first exception as soon as it occurs
                    LOGGER.error("Exception waiting for {}, {}", description, exceptionMessage);
                    // log the stacktrace
                    e.printStackTrace(new PrintWriter(stackTraceError));
                }
                result = false;
            }
            long timeLeft = deadline - System.currentTimeMillis();
            if (result) {
                return timeLeft;
            }
            if (timeLeft <= 0) {
                if (exceptionCount > 1) {
                    LOGGER.error("Exception waiting for {}, {}", description, exceptionMessage);

                    if (!stackTraceError.toString().isEmpty()) {
                        // printing handled stacktrace
                        LOGGER.error(stackTraceError.toString());
                    }
                }
                onTimeout.run();
                WaitException waitException = new WaitException("Timeout after " + timeoutMs + " ms waiting for " + description);
                waitException.printStackTrace();
                throw waitException;
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
