package io.enmasse.barnabas.controller.cluster.resources;

import io.fabric8.kubernetes.api.model.DoneableService;
import io.fabric8.kubernetes.api.model.ServiceFluentImpl;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

public interface Resource {
    void create(Handler<AsyncResult<Void>> handler);
    void delete(Handler<AsyncResult<Void>> handler);
    boolean exists();
}
