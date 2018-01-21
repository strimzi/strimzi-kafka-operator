package io.strimzi.controller.cluster.operations.openshift;

import io.strimzi.controller.cluster.K8SUtils;
import io.strimzi.controller.cluster.OpenShiftUtils;
import io.strimzi.controller.cluster.operations.Operation;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

public abstract class OpenShiftOperation implements Operation {
    protected OpenShiftOperation() {
    }

    public void execute(Vertx vertx, K8SUtils k8s, Handler<AsyncResult<Void>> handler) {
        execute(vertx, k8s.getOpenShiftUtils(), handler);
    }

    abstract void execute(Vertx vertx, OpenShiftUtils os, Handler<AsyncResult<Void>> handler);
}
