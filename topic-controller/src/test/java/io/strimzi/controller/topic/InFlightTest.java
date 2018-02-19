/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.topic;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(VertxUnitRunner.class)
public class InFlightTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(InFlightTest.class);

    private final Vertx vertx = Vertx.vertx();

    @Test
    public void testSingleTask(TestContext context) {
        Async async = context.async();
        InFlight<String> inflight = new InFlight(vertx);

        inflight.enqueue("test", ignored -> async.complete(), fut -> fut.complete());
    }

    @Test
    public void testTwoTasks(TestContext context) {
        Async bothEnqueued = context.async();
        Async firstCompleted = context.async();
        Async secondCompleted = context.async();
        InFlight<String> inflight = new InFlight(vertx);
        inflight.enqueue("test", v -> {
            LOGGER.debug("completing firstCompleted");
            firstCompleted.complete();
        }, fut -> {
                LOGGER.debug("1st task waiting for both to enqueue");
                bothEnqueued.await();
                LOGGER.debug("1st task completing");
                fut.complete();
            });

        inflight.enqueue("test", v -> {
            LOGGER.debug("completing secondCompleted");
            secondCompleted.complete();
        }, fut -> {
                LOGGER.debug("2nd task waiting for both to enqueue");
                bothEnqueued.await();
                LOGGER.debug("2nd task completing");
                fut.complete();
            });
        LOGGER.debug("completing bothEnqueued");
        bothEnqueued.complete();
        secondCompleted.await();
        LOGGER.debug("Waiting for inflight to empty");
        Async empty = context.async();
        vertx.setPeriodic(100, timerId -> {
            int size = inflight.size();
            LOGGER.debug("inflight size {}", size);
            if (size == 0) {
                vertx.cancelTimer(timerId);
                LOGGER.debug("completing empty");
                empty.complete();
            }
        });
        // TODO Need to test the case where the action doesn't complete immediately, but runs something on the context
        // (e.g. via timer) which will complete the fut at a later time.
    }

    @Test
    public void testTwoTasksFirstThrows(TestContext context) {
        Async bothEnqueued = context.async();
        Async firstCompleted = context.async();
        Async secondCompleted = context.async();
        InFlight<String> inflight = new InFlight(vertx);
        inflight.enqueue("test", v -> {
            LOGGER.debug("completing firstCompleted");
            firstCompleted.complete();
            context.assertTrue(v.failed());
            context.assertEquals("Oops!", v.cause().getMessage());
        }, fut -> {
                LOGGER.debug("1st task waiting for both to enqueue");
                bothEnqueued.await();
                LOGGER.debug("1st task failing");
                fut.fail("Oops!");
            });

        inflight.enqueue("test", v -> {
            LOGGER.debug("completing secondCompleted");
            secondCompleted.complete();
        }, fut -> {
                LOGGER.debug("2nd task waiting for both to enqueue");
                bothEnqueued.await();
                LOGGER.debug("2nd task completing");
                fut.complete();
            });
        LOGGER.debug("completing bothEnqueued");
        bothEnqueued.complete();
        secondCompleted.await();
        LOGGER.debug("Waiting for inflight to empty");
        Async empty = context.async();
        vertx.setPeriodic(100, timerId -> {
            int size = inflight.size();
            LOGGER.debug("inflight size {}", size);
            if (size == 0) {
                vertx.cancelTimer(timerId);
                LOGGER.debug("completing empty");
                empty.complete();
            }
        });
    }

    @Test
    public void test0(TestContext context) {
        testSingleTask(context);
        testTwoTasks(context);
        testTwoTasks(context);
    }
}
