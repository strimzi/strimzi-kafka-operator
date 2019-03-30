/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;

/**
 * Creates minimal vertx instances
 */
public class VertxFactory {
    public static Vertx create() {
        VertxOptions options = new VertxOptions()
                .setWorkerPoolSize(1)
                .setInternalBlockingPoolSize(1)
                .setEventLoopPoolSize(1);
        return Vertx.vertx(options);
    }
}
