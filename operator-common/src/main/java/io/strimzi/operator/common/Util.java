/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.strimzi.operator.common.operator.resource.TimeoutException;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

public class Util {

    private static final Logger LOGGER = LogManager.getLogger(Util.class);

    public static <T> Future<T> async(Vertx vertx, Supplier<T> supplier) {
        Future<T> result = Future.future();
        vertx.executeBlocking(
            future -> {
                try {
                    future.complete(supplier.get());
                } catch (Throwable t) {
                    future.fail(t);
                }
            }, result
        );
        return result;
    }

    /**
     * @param vertx The vertx instance.
     * @param logContext A string used for context in logging.
     * @param pollIntervalMs The poll interval in milliseconds.
     * @param timeoutMs The timeout, in milliseconds.
     * @param ready Determines when the wait is complete by returning true.
     * @return A future that completes when the given {@code ready} indicates readiness.
     */
    public static Future<Void> waitFor(Vertx vertx, String logContext, long pollIntervalMs, long timeoutMs, BooleanSupplier ready) {
        Future<Void> fut = Future.future();
        LOGGER.debug("Waiting for {} to get ready", logContext);
        long deadline = System.currentTimeMillis() + timeoutMs;
        Handler<Long> handler = new Handler<Long>() {
            @Override
            public void handle(Long timerId) {
                vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                    future -> {
                        try {
                            if (ready.getAsBoolean())   {
                                future.complete();
                            } else {
                                LOGGER.trace("{} is not ready", logContext);
                                future.fail("Not ready yet");
                            }
                        } catch (Throwable e) {
                            LOGGER.warn("Caught exception while waiting for {} to get ready", logContext, e);
                            future.fail(e);
                        }
                    },
                    true,
                    res -> {
                        if (res.succeeded()) {
                            LOGGER.debug("{} is ready", logContext);
                            fut.complete();
                        } else {
                            long timeLeft = deadline - System.currentTimeMillis();
                            if (timeLeft <= 0) {
                                String exceptionMessage = String.format("Exceeded timeout of %dms while waiting for %s to be ready", timeoutMs, logContext);
                                LOGGER.error(exceptionMessage);
                                fut.fail(new TimeoutException(exceptionMessage));
                            } else {
                                // Schedule ourselves to run again
                                vertx.setTimer(Math.min(pollIntervalMs, timeLeft), this);
                            }
                        }
                    }
                );
            }
        };

        // Call the handler ourselves the first time
        handler.handle(null);

        return fut;
    }

    /**
     * Parse a map from String.
     * For example a map of images {@code 2.0.0=strimzi/kafka:latest-kafka-2.0.0, 2.1.0=strimzi/kafka:latest-kafka-2.1.0}
     * or a map with labels / annotations {@code key1=value1 key2=value2}.
     *
     * @param str The string to parse.
     *
     * @return The parsed map.
     */
    public static Map<String, String> parseMap(String str) {
        if (str != null) {
            StringTokenizer tok = new StringTokenizer(str, ", \t\n\r");
            HashMap<String, String> map = new HashMap<>();
            while (tok.hasMoreTokens()) {
                String record = tok.nextToken();
                int endIndex = record.indexOf('=');

                if (endIndex == -1)  {
                    throw new RuntimeException("Failed to parse Map from String");
                }

                String key = record.substring(0, endIndex);
                String value = record.substring(endIndex + 1);
                map.put(key.trim(), value.trim());
            }
            return Collections.unmodifiableMap(map);
        } else {
            return Collections.emptyMap();
        }
    }
}
