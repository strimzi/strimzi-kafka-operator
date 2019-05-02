/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

/**
 * Inflight tracks the current reconciliation jobs being done, and prevents
 * us from trying to handling a change that we caused while we're still
 * handling the original change.
 *
 * For example, consider this linearization:
 * 1. User creates a topic in Kafka
 * 2. Operator notified of topic creation via ZooKeeper.
 * 3. Operator creates KafkaTopic due to event 1.
 * 4. Operator notified of KafkaTopic creation via Kubernetes
 *
 * Without Inflight the processing for event 1 is not complete (we've not created the
 * topic in the private topic store), so the linearization can proceed like this:
 *
 * 5. Operator creates private topic in topic store due to event 1.
 * 6. Operator creates private topic in topic sture doe to event 4.
 *    Exception because that private topic already exists.
 *
 * With Inflight the processing for the KafkaTopic creation is deferred until all the processing
 * due to event 1 is complete. The reconciliation algorithm is smart
 * enough realize, when reconciling the KafkaTopic creation that the Kafka
 * and TopicStore state is already correct, and so the reconciliation is a noop.
 */
class InFlight {

    private final static Logger LOGGER = LogManager.getLogger(InFlight.class);

    private final Vertx vertx;

    private final ConcurrentHashMap<TopicName, Integer> map = new ConcurrentHashMap<>();

    public InFlight(Vertx vertx) {
        this.vertx = vertx;
    }

    public String toString() {
        return this.map.toString();
    }

    /**
     * Run the given {@code action} on the context thread,
     * immediately if there are currently no other actions with the given {@code key},
     * or when the other actions with the given {@code key} have completed.
     * When the given {@code action} is complete it must complete its argument future,
     * which will complete the given {@code resultHandler}.
     */
    public void enqueue(TopicName key, Handler<Future<Void>> action, Handler<AsyncResult<Void>> resultHandler) {
        String lockName = key.toString();
        int timeoutMs = 30 * 1_000;
        BiFunction<TopicName, Integer, Integer> decrement = (topicName, waiters) -> {
            if (waiters != null) {
                return waiters == 1 ? null : waiters - 1;
            } else {
                LOGGER.error("Assertion failure. topic {}, action {}", lockName, action);
                return null;
            }
        };
        LOGGER.debug("Queuing action {} on topic {}", action, lockName);
        map.compute(key, (topicName, waiters) -> waiters == null ? 1 : waiters + 1);
        vertx.sharedData().getLockWithTimeout(lockName, timeoutMs, ar -> {
            if (ar.succeeded()) {
                Future<Void> f = Future.future();
                f.setHandler(ar2 -> {
                    LOGGER.debug("Executing handler for action {} on topic {}", action, lockName);
                    try {
                        resultHandler.handle(ar2);
                    } finally {
                        ar.result().release();
                        map.compute(key, decrement);
                    }
                });
                LOGGER.debug("Executing action {} on topic {}", action, lockName);
                action.handle(f);
            } else {
                try {
                    resultHandler.handle(Future.failedFuture("Failed to acquire lock for topic " + lockName + " after " + timeoutMs + "ms. Not executing action " + action));
                } finally {
                    map.compute(key, decrement);
                }
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
