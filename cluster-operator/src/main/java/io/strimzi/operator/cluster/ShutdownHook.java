/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Stack;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * This shutdown hook executes the shutdown procedures. It allows shutdown procedures to be registered and executes
 * them serially in the first-in last-out order. This allows us to build a hierarchy of steps executed in desired
 * order (e.g. first stop the operators before releasing the leader election lock etc.). This is different from just
 * registering multiple Java shutdown hooks because they would be executed in parallel / nondeterministic order.
 */
public class ShutdownHook implements Runnable {
    private static final Logger LOGGER = LogManager.getLogger(ShutdownHook.class);

    private final Stack<Runnable> shutdownStack;

    /**
     * Constructs the shutdown hook
     */
    public ShutdownHook() {
        this.shutdownStack = new Stack<>();
    }

    @Override
    public void run() {
        LOGGER.info("Shutdown hook started");

        while (!shutdownStack.isEmpty()) {
            shutdownStack.pop().run();
        }

        LOGGER.info("Shutdown hook completed");
    }

    /**
     * Registers a lambda which should be called during the shutdown
     *
     * @param shutdownFunction  Function which should be called when shutting down
     */
    public void register(Runnable shutdownFunction)    {
        shutdownStack.push(shutdownFunction);
    }

    /**
     * Utility method which can be used to shut down Vert.x
     *
     * @param vertx         Vert.x instance
     * @param timeoutMs     Timeout for the shutdown
     */
    public static void shutdownVertx(Vertx vertx, long timeoutMs) {
        LOGGER.info("Shutting down Vert.x");

        CountDownLatch latch = new CountDownLatch(1);

        vertx.close(ar -> {
            if (!ar.succeeded()) {
                LOGGER.error("Vert.x close failed", ar.cause());
            }
            latch.countDown();
        });

        try {
            if (!latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
                LOGGER.error("Timed out while waiting for Vert.x close");
            }
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted while waiting for Vert.x close");
        }

        LOGGER.info("Shutdown of Vert.x is complete");
    }

    /**
     * Utility method which can be used to undeploy verticle at shutdown
     *
     * @param vertx         Vert.x instance
     * @param verticleId    ID of the verticle which should be undeployed
     * @param timeoutMs     Timeout for the shutdown
     */
    public static void undeployVertxVerticle(Vertx vertx, String verticleId, long timeoutMs) {
        LOGGER.info("Shutting down Vert.x verticle {}", verticleId);
        CountDownLatch latch = new CountDownLatch(1);
        vertx.undeploy(verticleId, ar -> {
            if (!ar.succeeded()) {
                LOGGER.error("Vert.x verticle failed to undeploy", ar.cause());
            }

            latch.countDown();
        });

        try {
            if (!latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
                LOGGER.error("Timed out while waiting for Vert.x verticle to undeploy");
            }
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted while waiting for Vert.x verticle to undeploy");
        }

        LOGGER.info("Shutdown of Vert.x verticle {} is complete", verticleId);
    }
}
