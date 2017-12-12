package io.enmasse.barnabas.controller.cluster.operations;

import io.enmasse.barnabas.controller.cluster.K8SUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

public class OperationExecutor {
    private static OperationExecutor instance = null;

    private Vertx vertx;
    private K8SUtils k8s;

    public static OperationExecutor getInstance(Vertx vertx, K8SUtils k8s) {
        if(instance == null) {
            instance = new OperationExecutor(vertx, k8s);
        }
        else {
            instance.setVertx(vertx);
            instance.setK8s(k8s);
        }

        return instance;
    }

    public static OperationExecutor getInstance() {
        return instance;
    }

    private OperationExecutor(Vertx vertx, K8SUtils k8s) {
        this.vertx = vertx;
        this.k8s = k8s;
    }

    public void execute(Operation op, Handler<AsyncResult<Void>> handler) {
        op.execute(vertx, k8s, handler);
    }

    private Vertx getVertx() {
        return vertx;
    }

    private void setVertx(Vertx vertx) {
        this.vertx = vertx;
    }

    private K8SUtils getK8s() {
        return k8s;
    }

    private void setK8s(K8SUtils k8s) {
        this.k8s = k8s;
    }
}
