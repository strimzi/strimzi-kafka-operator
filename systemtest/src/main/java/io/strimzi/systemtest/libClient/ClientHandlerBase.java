/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.libClient;

import io.vertx.core.AbstractVerticle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

public abstract class ClientHandlerBase<T> extends AbstractVerticle {

    private static final Logger LOGGER = LogManager.getLogger(Producer.class);
    private Properties properties;
    private final String containerId;
    protected final CompletableFuture<T> resultPromise;
    private int messageCount;
    private Predicate predicate;
    private final AtomicInteger numSent = new AtomicInteger(0);

    public ClientHandlerBase(String containerId, CompletableFuture<T> resultPromise) {
        this.resultPromise = resultPromise;
        this.containerId = containerId;
    }

    @Override
    public void start() {
        produceMessages();
    }

    protected abstract void produceMessages();

}