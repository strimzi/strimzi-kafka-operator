package io.strimzi.controller.cluster.operations;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

public interface Operation<U> {
    void execute(Vertx vertx, U utils, Handler<AsyncResult<Void>> handler);
}
