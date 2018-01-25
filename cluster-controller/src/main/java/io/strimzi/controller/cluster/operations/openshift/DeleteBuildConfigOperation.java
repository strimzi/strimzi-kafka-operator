package io.strimzi.controller.cluster.operations.openshift;

import io.fabric8.openshift.api.model.BuildConfig;
import io.strimzi.controller.cluster.OpenShiftUtils;
import io.strimzi.controller.cluster.operations.Operation;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deletes BuildConfig resource
 */
public class DeleteBuildConfigOperation implements Operation<OpenShiftUtils> {
    private static final Logger log = LoggerFactory.getLogger(DeleteBuildConfigOperation.class.getName());
    private final String namespace;
    private final String name;

    /**
     * Constructor
     *
     * @param namespace     Namespace of the BuildConfig
     * @param name          Name of the BuildConfig
     */
    public DeleteBuildConfigOperation(String namespace, String name) {
        this.namespace = namespace;
        this.name = name;
    }

    /**
     * Deletes BuildConfig
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
                        if (os.exists(namespace, name, BuildConfig.class)) {
                            log.info("Deleting BuildConfig {}", name);
                            os.delete(namespace, name, BuildConfig.class);
                            future.complete();
                        }
                        else {
                            log.warn("BuildConfig {} doesn't exists", name);
                            future.complete();
                        }
                    } catch (Exception e) {
                        log.error("Caught exception while deleting BuildConfig", e);
                        future.fail(e);
                    }
                },
                false,
                res -> {
                    if (res.succeeded()) {
                        log.info("BuildConfig {} has been deleted", name);
                        handler.handle(Future.succeededFuture());
                    }
                    else {
                        log.error("BuildConfig deletion failed: {}", res.cause().toString());
                        handler.handle(Future.failedFuture(res.cause()));
                    }
                }
        );
    }
}
