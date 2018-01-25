package io.strimzi.controller.cluster.operations.openshift;

import io.fabric8.openshift.api.model.ImageStream;
import io.strimzi.controller.cluster.OpenShiftUtils;
import io.strimzi.controller.cluster.operations.Operation;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deletes ImageStream resource
 */
public class DeleteImageStreamOperation implements Operation<OpenShiftUtils> {
    private static final Logger log = LoggerFactory.getLogger(DeleteImageStreamOperation.class.getName());
    private final String namespace;
    private final String name;

    /**
     * Constructor
     *
     * @param namespace     Namespace of the ImageStream
     * @param name          Name of the ImageStream
     */
    public DeleteImageStreamOperation(String namespace, String name) {
        this.namespace = namespace;
        this.name = name;
    }

    /**
     * Deletes ImageStream
     *
     * @param vertx   Vert.x instance
     * @param os      OpenShiftUtils instance
     * @param handler Result handler
     */
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
                        log.error("ImageStream deletion failed: {}", res.cause().toString());
                        handler.handle(Future.failedFuture(res.cause()));
                    }
                }
        );
    }
}
