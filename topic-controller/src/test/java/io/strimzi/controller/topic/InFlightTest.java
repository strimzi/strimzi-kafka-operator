/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.controller.topic;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(VertxUnitRunner.class)
public class InFlightTest {

    private static final Logger logger = LoggerFactory.getLogger(InFlightTest.class);

    private final Vertx vertx = Vertx.vertx();

    @Test
    public void testSingleTask(TestContext context) {
        Async async = context.async();
        InFlight<String> inflight = new InFlight(vertx);

        inflight.enqueue("test", ignored->async.complete(), fut->fut.complete());
    }

    @Test
    public void testTwoTasks(TestContext context) {
        Async bothEnqueued = context.async();
        Async firstCompleted = context.async();
        Async secondCompleted = context.async();
        InFlight<String> inflight = new InFlight(vertx);
        inflight.enqueue("test", v -> {
            logger.debug("completing firstCompleted");
            firstCompleted.complete();
        }, fut -> {
            logger.debug("1st task waiting for both to enqueue");
            bothEnqueued.await();
            logger.debug("1st task completing");
            fut.complete();
        });

        inflight.enqueue("test", v->{
            logger.debug("completing secondCompleted");
            secondCompleted.complete();
        }, fut -> {
            logger.debug("2nd task waiting for both to enqueue");
            bothEnqueued.await();
            logger.debug("2nd task completing");
            fut.complete();
        });
        logger.debug("completing bothEnqueued");
        bothEnqueued.complete();
        secondCompleted.await();
        logger.debug("Waiting for inflight to empty");
        Async empty = context.async();
        vertx.setPeriodic(100, timerId -> {
            int size = inflight.size();
            logger.debug("inflight size {}", size);
            if (size == 0) {
                vertx.cancelTimer(timerId);
                logger.debug("completing empty");
                empty.complete();
            }
        });

    }

    @Test
    public void testTwoTasksFirstThrows(TestContext context) {
        Async bothEnqueued = context.async();
        Async firstCompleted = context.async();
        Async secondCompleted = context.async();
        InFlight<String> inflight = new InFlight(vertx);
        inflight.enqueue("test", v -> {
            logger.debug("completing firstCompleted");
            firstCompleted.complete();
            context.assertTrue(v.failed());
            context.assertEquals("Oops!", v.cause().getMessage());
        }, fut -> {
            logger.debug("1st task waiting for both to enqueue");
            bothEnqueued.await();
            logger.debug("1st task failing");
            fut.fail("Oops!");
        });

        inflight.enqueue("test", v -> {
            logger.debug("completing secondCompleted");
            secondCompleted.complete();
        }, fut -> {
            logger.debug("2nd task waiting for both to enqueue");
            bothEnqueued.await();
            logger.debug("2nd task completing");
            fut.complete();
        });
        logger.debug("completing bothEnqueued");
        bothEnqueued.complete();
        secondCompleted.await();
        logger.debug("Waiting for inflight to empty");
        Async empty = context.async();
        vertx.setPeriodic(100, timerId -> {
            int size = inflight.size();
            logger.debug("inflight size {}", size);
            if (size == 0) {
                vertx.cancelTimer(timerId);
                logger.debug("completing empty");
                empty.complete();
            }
        });
    }
}
