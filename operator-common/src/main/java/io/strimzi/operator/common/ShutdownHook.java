/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * This shutdown hook ensure that {@code Vertx.close()} is called on a clean shutdown,
 * which in turn calls the stop method of all running Verticles.
 *
 * We add a fixed timeout because Vertx has none for stopping running Verticles.
 */
@SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
public class ShutdownHook implements Runnable {
    private static final Logger LOGGER = LogManager.getLogger(ShutdownHook.class);
    
    private Vertx vertx;
    private long timeoutMs;

    public ShutdownHook(Vertx vertx) {
        this.vertx = vertx;
        this.timeoutMs = 120_000;
    }
    
    public ShutdownHook(Vertx vertx, long timeoutMs) {
        this.vertx = vertx;
        this.timeoutMs = timeoutMs;
    }
    
    @Override
    public void run() {
        LOGGER.info("Shutting down");
        CountDownLatch latch = new CountDownLatch(1);
        if (vertx != null) {
            vertx.close(ar -> {
                if (!ar.succeeded()) {
                    LOGGER.error("Vertx close failed", ar.cause());
                }
                latch.countDown();
            });
            try {
                if (!latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
                    LOGGER.error("Timed out waiting for shutdown to complete");
                }
            } catch (InterruptedException e) {
                LOGGER.error("Shutdown thread interrupted");
            }
        }
        LOGGER.info("Shutdown completed");
    }
}
