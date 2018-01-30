package io.strimzi.controller.cluster.operations;

import io.strimzi.controller.cluster.K8SUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

public abstract class ClusterOperation {

    protected final int LOCK_TIMEOUT = 60000;
    protected final Vertx vertx;

    protected ClusterOperation(Vertx vertx) {
        this.vertx = vertx;
    }

    protected abstract String getLockName(String namespace, String name);

}
