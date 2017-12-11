package io.enmasse.barnabas.controller.cluster.operations;

import io.enmasse.barnabas.controller.cluster.K8SUtils;
import io.vertx.core.Vertx;

public abstract class K8sOperation implements Operation {
    protected final K8SUtils k8s;
    protected final Vertx vertx;

    protected K8sOperation(Vertx vertx, K8SUtils k8s) {
        this.vertx = vertx;
        this.k8s = k8s;
    }
}
