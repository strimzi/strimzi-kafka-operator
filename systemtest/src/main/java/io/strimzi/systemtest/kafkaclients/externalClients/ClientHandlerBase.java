/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.externalClients;

import io.vertx.core.AbstractVerticle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.function.IntPredicate;

public abstract class ClientHandlerBase<T> extends AbstractVerticle {

    private static final Logger LOGGER = LogManager.getLogger(ClientHandlerBase.class);
    final CompletableFuture<T> resultPromise;
    final IntPredicate msgCntPredicate;

    public ClientHandlerBase(CompletableFuture<T> resultPromise, IntPredicate msgCntPredicate) {
        this.resultPromise = resultPromise;
        this.msgCntPredicate = msgCntPredicate;
    }

    @Override
    public void start() {
        handleClient();
    }

    protected abstract void handleClient();

    public CompletableFuture<T> getResultPromise() {
        return resultPromise;
    }
}