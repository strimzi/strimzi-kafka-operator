/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class ShutdownHookTest {
    @Test
    public void testOrdering() throws InterruptedException {
        ShutdownHook hook = new ShutdownHook();

        CountDownLatch latchOne = new CountDownLatch(1);
        CountDownLatch latchTwo = new CountDownLatch(1);
        CountDownLatch latchThree = new CountDownLatch(1);

        hook.register(() -> {
            try {
                latchTwo.await();
                latchThree.countDown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        hook.register(() -> {
            try {
                latchOne.await();
                latchTwo.countDown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        hook.register(latchOne::countDown);

        CompletableFuture.runAsync(hook);

        boolean completed = latchThree.await(5_000L, TimeUnit.MILLISECONDS);
        assertThat("Last latch did not complete", completed, is(true));
    }

    @Test
    public void testVertxTerminationTimeout() throws InterruptedException {
        Vertx vertx = Vertx.vertx();
        CountDownLatch latch = new CountDownLatch(1);
        vertx.deployVerticle(new MyVerticle(5_000)).onComplete(result -> latch.countDown());
        latch.await(20, TimeUnit.SECONDS);
        assertThat("Verticle was not deployed", vertx.deploymentIDs(), hasSize(1));

        long timeoutMs = 2_000;
        ShutdownHook hook = new ShutdownHook();
        hook.register(() -> ShutdownHook.shutdownVertx(vertx, timeoutMs));

        long start = System.nanoTime();
        hook.run();
        long execTimeMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(String.format(
                "Shutdown took %d ms, which exceeds the configured timeout of %d ms", execTimeMs, timeoutMs),
                (long) (timeoutMs * 1.1), greaterThan(execTimeMs));
    }

    @Test
    public void testVertxStop() throws InterruptedException {
        int nVerticles = 10;
        Vertx vertx = Vertx.vertx();
        CountDownLatch latch = new CountDownLatch(nVerticles);
        List<MyVerticle> verticles = new ArrayList<>(nVerticles);
        while (verticles.size() < nVerticles) {
            MyVerticle myVerticle = new MyVerticle();
            verticles.add(myVerticle);
            vertx.deployVerticle(myVerticle).onComplete(result -> latch.countDown());
        }

        latch.await(20, TimeUnit.SECONDS);
        assertThat("Verticles were not deployed", vertx.deploymentIDs(), hasSize(nVerticles));

        ShutdownHook hook = new ShutdownHook();
        hook.register(() -> ShutdownHook.shutdownVertx(vertx, 10_000L));
        hook.run();

        assertThat("Verticles were not stopped", vertx.deploymentIDs(), empty());
        for (MyVerticle verticle : verticles) {
            assertThat("Verticle stop was not executed", verticle.getCounter(), is(1));
        }
    }

    @Test
    public void testVerticlesUndeploy() throws InterruptedException {
        Vertx vertx = Vertx.vertx();
        CountDownLatch latch = new CountDownLatch(1);
        MyVerticle myVerticle = new MyVerticle();
        vertx.deployVerticle(myVerticle).onComplete(result -> latch.countDown());

        latch.await(20, TimeUnit.SECONDS);
        assertThat("Verticles were not deployed", vertx.deploymentIDs(), is(Set.of(myVerticle.deploymentID())));

        ShutdownHook hook = new ShutdownHook();
        hook.register(() -> ShutdownHook.undeployVertxVerticle(vertx, myVerticle.deploymentID(), 10_000L));
        hook.run();

        assertThat("Verticles were not stopped", vertx.deploymentIDs(), empty());
        assertThat("Verticle stop was not executed", myVerticle.getCounter(), is(1));
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
                // Nothing to do
            }
            counter++;
        }

        public int getCounter() {
            return counter;
        }
    }
}
