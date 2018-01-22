package io.strimzi.controller.cluster.operations.openshift;

import io.fabric8.openshift.api.model.ImageStream;
import io.strimzi.controller.cluster.OpenShiftUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteImageStreamOperation extends OpenShiftOperation {
    private static final Logger log = LoggerFactory.getLogger(DeleteImageStreamOperation.class.getName());
    private final String namespace;
    private final String name;

    public DeleteImageStreamOperation(String namespace, String name) {
        this.namespace = namespace;
        this.name = name;
    }

    @Override
    public void execute(Vertx vertx, OpenShiftUtils os, Handler<AsyncResult<Void>> handler) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    try {
                        if (os.exists(namespace, name, ImageStream.class)) {
                            log.info("Deleting ImageStream {}", name);
                            os.delete(namespace, name, ImageStream.class);
                            future.complete();
                        } else {
                            log.warn("ImageStream {} doesn't exists", name);
                            future.complete();
                        }
                    } catch (Exception e) {
                            log.error("Caught exception while deleting ImageStream", e);
                            future.fail(e);
                    }
                },
                false,
                res -> {
                    if (res.succeeded()) {
                        log.info("ImageStream {} has been deleted", name);
                        handler.handle(Future.succeededFuture());
                    }
                    else {
                        log.error("ImageStream deletion failed: {}", res.result());
                        handler.handle(Future.failedFuture((Exception)res.result()));
                    }
                }
        );
    }
}
