package io.strimzi.controller.cluster.operations;

import io.strimzi.controller.cluster.K8SUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

public interface Operation {
    void execute(Vertx vertx, K8SUtils k8s, Handler<AsyncResult<Void>> handler);
}
