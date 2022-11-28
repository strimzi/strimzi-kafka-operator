/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.vertx.core.Vertx;
import static java.util.Objects.requireNonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * This shutdown hook ensure that {@code Vertx.close()} is called on a clean shutdown,
 * which in turn calls the stop method of all running Verticles.
 * <p>
 * We add a fixed timeout because Vertx has none for stopping running Verticles.
 */
@SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
public class ShutdownHook implements Runnable {
    private static final Logger LOGGER = LogManager.getLogger(ShutdownHook.class);

    private Vertx vertx;
    private long timeoutMs;

    /**
     * Constructs the shutdown hook
     *
     * @param vertx     Vert.x instance
     */
    public ShutdownHook(Vertx vertx) {
        this(vertx, 120_000);
    }

    /**
     * Constructs the shutdown hook
     *
     * @param vertx         Vert.x instance
     * @param timeoutMs     Timeout in milliseconds
     */
    public ShutdownHook(Vertx vertx, long timeoutMs) {
        this.vertx = requireNonNull(vertx, "Vertx can't be null");
        this.timeoutMs = timeoutMs;
    }

    @Override
    public void run() {
        LOGGER.info("Shutdown started");
        if (vertx.deploymentIDs().size() > 0) {
            CountDownLatch latch = new CountDownLatch(1);
            vertx.close(ar -> {
                if (!ar.succeeded()) {
                    LOGGER.error("Vertx close failed", ar.cause());
                }
                latch.countDown();
            });
            try {
                if (!latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
                    LOGGER.error("Timed out while waiting for Vertx close");
                }
            } catch (InterruptedException e) {
                LOGGER.error("Interrupted while waiting for Vertx close");
            }
        }
        LOGGER.info("Shutdown completed");
    }
}
