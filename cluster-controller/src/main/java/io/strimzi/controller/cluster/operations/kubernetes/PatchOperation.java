package io.strimzi.controller.cluster.operations.kubernetes;

import io.strimzi.controller.cluster.K8SUtils;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.client.dsl.Patchable;
import io.strimzi.controller.cluster.operations.Operation;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PatchOperation<U> implements Operation<U> {
    private static final Logger log = LoggerFactory.getLogger(PatchOperation.class.getName());
    private final Patchable patchable;
    private final KubernetesResource patch;

    public PatchOperation(Patchable patchable, KubernetesResource patch) {
        this.patchable = patchable;
        this.patch = patch;
    }

    @Override
    public void execute(Vertx vertx, U k8s, Handler<AsyncResult<Void>> handler) {
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
