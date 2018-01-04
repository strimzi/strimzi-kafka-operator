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

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Inflight tracks the current reconciliation jobs being done, and prevents
 * us from trying to handling a change that we caused while we're still
 * handling the original change.
 *
 * For example, consider this linearization:
 * 1. User creates a topic in Kafka
 * 2. Controller notified of topic creation via ZooKeeper.
 * 3. Controller creates ConfigMap due to event 1.
 * 4. Controller notified of ConfigMap creation via Kubernetes
 *
 * Without Inflight the processing for event 1 is not complete (we've not created the
 * topic in the private topic store), so the linearization can proceed like this:
 *
 * 5. Controller creates private topic in topic store due to event 1.
 * 6. Controller creates private topic in topic sture doe to event 4.
 *    Exception because that private topic already exists.
 *
 * With Inflight the processing for the ConfigMap creation is deferred until all the processing
 * due to event 1 is complete. The reconciliation algorithm is smart
 * enough realize, when reconciling the ConfigMap creation that the Kafka
 * and TopicStore state is already correct, and so the reconciliation is a noop.
 */
class InFlight<T> {

    private final Vertx vertx;

    private ConcurrentHashMap<T, Future<Void>> map = new ConcurrentHashMap<>();

    public InFlight(Vertx vertx) {
        this.vertx = vertx;
    }

    private Future<Void> futureWithHandler(Handler<AsyncResult<Void>> handler) {
        Future<Void> fut = Future.future();
        fut.setHandler(handler);
        return fut;
    }

    /**
     * Run the given {@code action} on the context thread,
     * immediately if there are currently no other actions with the given {@code key},
     * or when the other actions with the given {@code key} have completed.
     * When the given {@code action} is complete it must complete its argument future,
     * which will complete the given {@code resultHandler}.
     */
    public void enqueue(T key, Handler<AsyncResult<Void>> resultHandler, Handler<Future<Void>> action) {
        Future<Void> fut = futureWithHandler(resultHandler);
        map.compute(key, (k, current) -> {
            if (current == null) {
                vertx.runOnContext(ignored->action.handle(fut));
                return fut;
            } else {
                current.setHandler(ar -> {
                    vertx.runOnContext(ar2 -> {
                        try {
                            action.handle(fut);
                        } finally {
                            // remove from map if fut is the current key
                            map.compute(key, (k2, v)-> {
                                if (v == fut) {
                                    return null;
                                } else {
                                    return v;
                                }
                            });
                        }
                    });
                });
                return fut;
            }
        });
    }

    /**
     * The number of keys with inflight actions.
     */
    public int size() {
        return map.size();
    }
}
