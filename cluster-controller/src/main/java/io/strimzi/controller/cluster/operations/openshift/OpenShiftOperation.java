package io.strimzi.controller.cluster.operations.openshift;

import io.strimzi.controller.cluster.K8SUtils;
import io.strimzi.controller.cluster.OpenShiftUtils;
import io.strimzi.controller.cluster.operations.Operation;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

/**
 * Base OpenShift operation
 */
public abstract class OpenShiftOperation implements Operation {
    protected OpenShiftOperation() {
    }

    /**
     * This method is called from the default executor with K8SUtils object.
     * It converts it to OpenShiftUtils object and calls the operation
     *
     * @param vertx     Vert.x instance
     * @param k8s       K8SUtils instance
     * @param handler   Result handler
     */
    public void execute(Vertx vertx, K8SUtils k8s, Handler<AsyncResult<Void>> handler) {
        execute(vertx, k8s.getOpenShiftUtils(), handler);
    }

    /**
     * Executes OpenShift operations
     *
     * @param vertx     Vert.x instance
     * @param os        OpenShiftUtils instance
     * @param handler   Result handler
     */
    abstract void execute(Vertx vertx, OpenShiftUtils os, Handler<AsyncResult<Void>> handler);
}
