package io.enmasse.barnabas.controller.cluster.operations;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

public interface Operation {
    void execute(Handler<AsyncResult<Void>> handler);
}
