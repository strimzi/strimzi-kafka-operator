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
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class InFlightTest {

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
            bothEnqueued.await();
            firstCompleted.complete();
        }, fut -> fut.complete());
        inflight.enqueue("test", v->secondCompleted.complete(), fut -> fut.complete());
        bothEnqueued.complete();
        secondCompleted.await();
        Async empty = context.async();
        vertx.setPeriodic(100, timerId -> {
            if (inflight.size() == 0) {
                vertx.cancelTimer(timerId);
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
        inflight.enqueue("test", v->{
            bothEnqueued.await();
            firstCompleted.complete();
            context.assertTrue(v.failed());
            context.assertEquals("Oops!", v.cause().getMessage());
            //throw new RuntimeException();
        }, fut -> fut.fail("Oops!"));
        inflight.enqueue("test", v->secondCompleted.complete(), fut -> fut.complete());
        bothEnqueued.complete();
        secondCompleted.await();
        Async empty = context.async();
        vertx.setPeriodic(100, timerId -> {
            if (inflight.size() == 0) {
                vertx.cancelTimer(timerId);
                empty.complete();
            }
        });
    }
}
