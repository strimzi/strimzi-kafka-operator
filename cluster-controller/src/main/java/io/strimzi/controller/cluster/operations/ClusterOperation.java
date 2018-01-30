package io.strimzi.controller.cluster.operations;

import io.strimzi.controller.cluster.K8SUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

public abstract class ClusterOperation {
    protected final String namespace;
    protected final String name;

    protected final int LOCK_TIMEOUT = 60000;
    protected final Vertx vertx;

    protected ClusterOperation(Vertx vertx, String namespace, String name) {
        this.vertx = vertx;
        this.namespace = namespace;
        this.name = name;
    }

    protected abstract String getLockName();

}
