/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
public class ShutdownHook implements Runnable {
    private static final Logger LOGGER = LogManager.getLogger(ShutdownHook.class.getName());
    
    private Vertx vertx;
    
    public ShutdownHook(Vertx vertx) {
        this.vertx = vertx;
    }
    
    @Override
    public void run() {
        LOGGER.info("Shutting down");
        CountDownLatch latch = new CountDownLatch(1);
        if (vertx != null) {
            vertx.close(ar -> {
                if (!ar.succeeded()) {
                    LOGGER.error("Failure in stopping Vertx", ar.cause());
                }
                latch.countDown();
            });
            try {
                if (!latch.await(2, TimeUnit.MINUTES)) {
                    LOGGER.error("Timed out waiting to undeploy all Verticles");
                }
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }

        LOGGER.info("Shutdown complete");
    }
}
