/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;



@RunWith(VertxUnitRunner.class)
public class InFlightTest {

    private static final Logger LOGGER = LogManager.getLogger(InFlightTest.class);

    private final Vertx vertx = Vertx.vertx();

    @Test
    public void testSingleTask(TestContext context) {
        Async async = context.async();
        InFlight<String> inflight = new InFlight(vertx);

        inflight.enqueue("test", fut -> fut.complete(), ignored -> async.complete());
    }

    @Test
    public void testTwoTasks(TestContext context) {
        Async bothEnqueued = context.async();
        Async firstCompleted = context.async();
        Async secondCompleted = context.async();
        InFlight<String> inflight = new InFlight(vertx);
        inflight.enqueue("test", fut -> {
            LOGGER.debug("1st task waiting for both to enqueue");
            bothEnqueued.await();
            LOGGER.debug("1st task completing");
            fut.complete();
        }, v -> {
                LOGGER.debug("completing firstCompleted");
                firstCompleted.complete();
            });

        inflight.enqueue("test", fut -> {
            LOGGER.debug("2nd task waiting for both to enqueue");
            bothEnqueued.await();
            LOGGER.debug("2nd task completing");
            fut.complete();
        }, v -> {
                LOGGER.debug("completing secondCompleted");
                secondCompleted.complete();
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
        inflight.enqueue("test", fut -> {
            LOGGER.debug("1st task waiting for both to enqueue");
            bothEnqueued.await();
            LOGGER.debug("1st task failing");
            fut.fail("Oops!");
        }, v -> {
                LOGGER.debug("completing firstCompleted");
                firstCompleted.complete();
                context.assertTrue(v.failed());
                context.assertEquals("Oops!", v.cause().getMessage());
            });

        inflight.enqueue("test", fut -> {
            LOGGER.debug("2nd task waiting for both to enqueue");
            bothEnqueued.await();
            LOGGER.debug("2nd task completing");
            fut.complete();
        }, v -> {
                LOGGER.debug("completing secondCompleted");
                secondCompleted.complete();
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
