/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import static org.hamcrest.Matchers.greaterThan;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class ShutdownHookTest {
    @Test
    public void testTerminationTimeout() throws InterruptedException {
        Vertx vertx = Vertx.vertx();
        CountDownLatch latch = new CountDownLatch(1);
        vertx.deployVerticle(new MyVerticle(5_000)).onComplete(result -> {
            latch.countDown();
        });
        latch.await(20, TimeUnit.SECONDS);
        assertThat("Verticle was not deployed", vertx.deploymentIDs(), hasSize(1));

        long timeoutMs = 2_000;
        ShutdownHook hook = new ShutdownHook(vertx, timeoutMs);

        long start = System.nanoTime();
        hook.run();
        long execTimeMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(String.format(
                "Shutdown took %d ms, which exceeds the configured timeout of %d ms", execTimeMs, timeoutMs),
                (long) (timeoutMs * 1.1), greaterThan(execTimeMs));
    }

    @Test
    public void testVerticlesStop() throws InterruptedException {
        int nVerticles = 10;
        Vertx vertx = Vertx.vertx();
        CountDownLatch latch = new CountDownLatch(nVerticles);
        List<MyVerticle> verticles = new ArrayList<>(nVerticles);
        while (verticles.size() < nVerticles) {
            MyVerticle myVerticle = new MyVerticle();
            verticles.add(myVerticle);
            vertx.deployVerticle(myVerticle).onComplete(result -> {
                latch.countDown();
            });
        }

        latch.await(20, TimeUnit.SECONDS);
        assertThat("Verticles were not deployed", vertx.deploymentIDs(), hasSize(nVerticles));

        ShutdownHook hook = new ShutdownHook(vertx);
        hook.run();

        assertThat("Verticles were not stopped", vertx.deploymentIDs(), empty());
        for (MyVerticle verticle : verticles) {
            assertThat("Verticle stop was not executed", verticle.getCounter(), is(1));
        }
    }

    static class MyVerticle extends AbstractVerticle {
        private int counter = 0;
        private long delayMs = 0;

        public MyVerticle() {
        }

        public MyVerticle(long delayMs) {
            this.delayMs = delayMs;
        }

        @Override
        public void stop() {
            try {
                TimeUnit.MILLISECONDS.sleep(delayMs);
            } catch (InterruptedException e) {
            }
            counter++;
        }

        public int getCounter() {
            return counter;
        }
    }
}
