package io.strimzi.controller.cluster.operations.openshift;

import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.client.dsl.Patchable;
import io.strimzi.controller.cluster.K8SUtils;
import io.strimzi.controller.cluster.OpenShiftUtils;
import io.strimzi.controller.cluster.operations.kubernetes.K8sOperation;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Patches OpenShift resource. Should work with all Patchable resources
 */
public class PatchOperation extends OpenShiftOperation {
    private static final Logger log = LoggerFactory.getLogger(PatchOperation.class.getName());
    private final Patchable patchable;
    private final KubernetesResource patch;

    /**
     * @param patchable     Resource which should be patched
     * @param patch         Patch
     */
    public PatchOperation(Patchable patchable, KubernetesResource patch) {
        this.patchable = patchable;
        this.patch = patch;
    }

    /**
     * @param vertx   Vert.x instance
     * @param os      OpenShiftUtils instance
     * @param handler Result handler
     */
    @Override
    public void execute(Vertx vertx, OpenShiftUtils os, Handler<AsyncResult<Void>> handler) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    try {
                        log.info("Patching resource with {}", patch);
                        os.patch(patchable, patch);
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
                        log.error("Failed to patch resource: {}", res.result());
                        handler.handle(Future.failedFuture((Exception)res.result()));
                    }
                }
        );
    }
}
