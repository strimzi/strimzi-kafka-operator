/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.vertx.core.Vertx;
import static java.util.Objects.requireNonNull;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Shutdown hook that retrieves and stops all deployed verticles without invoking Vertx.close.
 * <br><br>
 * System.exit is a blocking call that waits all registered shutdown-hook threads to complete before stopping the VM.
 * Vertx.close stops all deployed verticles and waits for the event-loop threads to complete their current iteration.
 * We have a deadlock when invoking System.exit from an event-loop thread which triggers a shutdown hook invoking Vertx.close.
 * There is no issue when invoking System.exit from a non event-loop thread.
 */
public class ShutdownHook implements Runnable {
    private static final Logger LOGGER = LogManager.getLogger(ShutdownHook.class);

    private Vertx vertx;
    private long timeoutMs;

    public ShutdownHook(Vertx vertx) {
        this(vertx, 120_000);
    }

    public ShutdownHook(Vertx vertx, long timeoutMs) {
        this.vertx = requireNonNull(vertx, "Vertx can't be null");
        this.timeoutMs = timeoutMs;
    }

    @Override
    public void run() {
        LOGGER.info("Shutdown started");
        Set<String> ids = vertx.deploymentIDs();
        if (ids.size() > 0) {
            ExecutorService executor = Executors.newFixedThreadPool(ids.size());
            ids.forEach(id -> {
                try {
                    executor.submit(undeploy(id));
                } catch (Throwable t) {
                    LOGGER.error("Error while shutting down", t);
                }
            });
            stopExecutor(executor, timeoutMs);
        }
        LOGGER.info("Shutdown completed");
    }

    private Runnable undeploy(String id) {
        return () -> {
            CountDownLatch latch = new CountDownLatch(1);
            vertx.undeploy(id).onComplete(ar -> latch.countDown());
            try {
                latch.await();
            } catch (InterruptedException e) {
            }
        };
    }

    private void stopExecutor(ExecutorService executor, long timeoutMs) {
        try {
            executor.shutdown();
            executor.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("Error while shutting down", e);
        } finally {
            if (!executor.isTerminated()) {
                executor.shutdownNow();
            }
        }
    }
}
