/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.vertx.core.Promise;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.impl.Deployment;
import io.vertx.core.impl.VertxImpl;
import static java.util.Objects.requireNonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Shutdown hook that retrieves and stops all deployed verticles without invoking Vertx.close.
 * <br><br>
 * System.exit is a blocking call that waits all registered shutdown-hook threads to complete before stopping the VM.
 * Vertx.close stops all deployed verticles and waits for the event-loop threads to complete their current iteration.
 * We have a deadlock when invoking System.exit from an event-loop thread which triggers a shutdown hook invoking Vertx.close.
 * There is no issue when invoking System.exit from a non event-loop thread.
 */
@SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
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
        vertx.deploymentIDs()
                .stream()
                .map(id -> ((VertxImpl) vertx).getDeployment(id))
                .forEach(deployment -> stopVerticles(deployment));
        LOGGER.info("Shutdown completed");
    }

    private void stopVerticles(Deployment deployment) {
        Set<Verticle> verticles = deployment.getVerticles();
        ExecutorService executor = Executors.newFixedThreadPool(verticles.size());
        verticles.forEach(verticle -> {
            try {
                executor.submit(stopVerticle(verticle));
            } catch (Throwable t) {
                LOGGER.error("Error while waiting for verticle stop");
            }
        });
        stopExecutor(executor, timeoutMs);
    }

    private Runnable stopVerticle(Verticle verticle) {
        return () -> {
            try {
                verticle.stop(Promise.promise());
            } catch (Throwable t) {
                LOGGER.error("Failed to stop verticle", t);
                t.printStackTrace();
            }
        };
    }

    private static boolean stopExecutor(ExecutorService executor, long timeoutMs) {
        try {
            executor.shutdown();
            executor.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS);
        } finally {
            if (executor.isTerminated()) {
                return true;
            } else {
                executor.shutdownNow();
                return false;
            }
        }
    }
}
