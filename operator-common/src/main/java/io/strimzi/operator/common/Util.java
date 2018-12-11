/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.strimzi.operator.common.operator.resource.TimeoutException;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.function.BooleanSupplier;

public class Util {

    private static final Logger LOGGER = LogManager.getLogger(Util.class);

    /**
     * Returns a future that completes when the given {@code ready} indicates readiness.
     *
     * @param vertx The vertx instance
     * @param logContext A string used for context in logging
     * @param pollIntervalMs The poll interval in milliseconds.
     * @param timeoutMs The timeout, in milliseconds.
     * @param ready Determines when the wait is complete by returning true.
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
}
