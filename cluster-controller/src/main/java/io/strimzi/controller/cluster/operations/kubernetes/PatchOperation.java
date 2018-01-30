package io.strimzi.controller.cluster.operations.kubernetes;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.client.dsl.Patchable;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PatchOperation {
    private static final Logger log = LoggerFactory.getLogger(PatchOperation.class.getName());
    private final Vertx vertx;

    public PatchOperation(Vertx vertx) {
        this.vertx = vertx;
    }

    public <T extends HasMetadata, R extends KubernetesResource<T>, P extends Patchable<R, T>> void patch(P patchable, R patch, Handler<AsyncResult<Void>> handler) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    try {
                        log.info("Patching resource with {}", patch);
                        patchable.patch(patch);
                        future.complete();
                    }
                    catch (Exception e) {
                        log.error("Caught exception while patching", e);
                        future.fail(e);
                    }
                },
                false,
                res -> {
                    if (res.succeeded()) {
                        log.info("Resource has been patched", patch);
                        handler.handle(Future.succeededFuture());
                    }
                    else {
                        log.error("Failed to patch resource: {}", res.cause().toString());
                        handler.handle(Future.failedFuture(res.cause()));
                    }
                }
        );
    }
}
