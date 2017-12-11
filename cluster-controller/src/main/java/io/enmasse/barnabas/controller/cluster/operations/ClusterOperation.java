package io.enmasse.barnabas.controller.cluster.operations;

import io.enmasse.barnabas.controller.cluster.K8SUtils;
import io.vertx.core.Vertx;

public abstract class ClusterOperation implements Operation {
    protected final K8SUtils k8s;
    protected final Vertx vertx;
    protected final String namespace;
    protected final String name;

    protected final int LOCK_TIMEOUT = 60000;

    protected ClusterOperation(Vertx vertx, K8SUtils k8s, String namespace, String name) {
        this.vertx = vertx;
        this.k8s = k8s;
        this.namespace = namespace;
        this.name = name;
    }

    protected abstract String getLockName();
}
