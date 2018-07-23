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
class InFlight<T> {

    private final static Logger LOGGER = LogManager.getLogger(InFlight.class);

    private final Vertx vertx;

    private final ConcurrentHashMap<T, InflightHandler> map = new ConcurrentHashMap<>();

    class InflightHandler implements Handler<AsyncResult<Void>> {

        private final Handler<AsyncResult<Void>> h1;
        private final Handler<AsyncResult<Void>> h2;
        private final String fur;
        private Handler<AsyncResult<Void>> h3;
        private final Future<Void> fut;

        public InflightHandler(T key, String fur, Handler<AsyncResult<Void>> h1) {
            this.fur = fur;
            this.h1 = h1;
            this.h2 = x -> {
                // remove from map if fut is the current key
                map.compute(key, (k2, v) -> {
                    if (v == this) {
                        LOGGER.debug("Removing finished action {}", this);
                        return null;
                    } else {
                        return v;
                    }
                });
            };
            Future<Void> fut = Future.future();
            this.fut = fut;
            fut.setHandler(this);
        }

        @Override
        public void handle(AsyncResult<Void> event) {
            h1.handle(event);
            h2.handle(event);
            if (h3 != null) {
                h3.handle(event);
            }
        }

        public void setHandler(Handler<AsyncResult<Void>> h3) {
            this.h3 = h3;
        }

        public String toString() {
            return fur;
        }
    }

    public InFlight(Vertx vertx) {
        this.vertx = vertx;
    }


    /**
     * Run the given {@code action} on the context thread,
     * immediately if there are currently no other actions with the given {@code key},
     * or when the other actions with the given {@code key} have completed.
     * When the given {@code action} is complete it must complete its argument future,
     * which will complete the given {@code resultHandler}.
     */
    public void enqueue(T key, Handler<Future<Void>> action, Handler<AsyncResult<Void>> resultHandler) {
        InflightHandler fut = new InflightHandler(key, action.toString(), resultHandler);
        LOGGER.debug("resultHandler:{}, action:{}, fut:{}", resultHandler, action, fut);
        map.compute(key, (k, current) -> {
            if (current == null) {
                LOGGER.debug("Queueing {} for immediate execution", action);
                vertx.runOnContext(ignored -> action.handle(fut.fut));
                return fut;
            } else {
                LOGGER.debug("Queueing {} for deferred execution after {}", action, current);
                current.setHandler(ar -> {
                    LOGGER.debug("Queueing {} after deferred execution", action);
                    vertx.runOnContext(ar2 ->
                            action.handle(fut.fut));
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
