package io.strimzi.controller.cluster.operations.kubernetes;

import io.strimzi.controller.cluster.K8SUtils;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import io.strimzi.controller.cluster.operations.Operation;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScaleDownOperation implements Operation<K8SUtils> {
    private static final Logger log = LoggerFactory.getLogger(ScaleDownOperation.class.getName());
    private final ScalableResource res;
    private final int scaleTo;

    public ScaleDownOperation(ScalableResource res, int scaleTo) {
        this.res = res;
        this.scaleTo = scaleTo;
    }

    @Override
    public void execute(Vertx vertx, K8SUtils k8s, Handler<AsyncResult<Void>> handler) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    try {
                        Object gettable = res.get();
                        int nextReplicas;

                        if (gettable instanceof StatefulSet) {
                            nextReplicas = ((StatefulSet) res.get()).getSpec().getReplicas();
                        } else if (gettable instanceof Deployment) {
                            nextReplicas = ((Deployment) res.get()).getSpec().getReplicas();
                        } else {
                            future.fail("Unknown resource type: " + gettable.getClass().getCanonicalName());
                            return;
                        }

                        while (nextReplicas > scaleTo) {
                            nextReplicas--;
                            log.info("Scaling down from {} to {}", nextReplicas+1, nextReplicas);
                            k8s.scale(res, nextReplicas, true);
                        }

                        future.complete();
                    }
                    catch (Exception e) {
                        log.error("Caught exception while scaling down", e);
                        future.fail(e);
                    }
                },
                false,
                res -> {
                    if (res.succeeded()) {
                        log.info("Scaling down to {} replicas has been completed", scaleTo);
                        handler.handle(Future.succeededFuture());
                    }
                    else {
                        log.error("Scaling down has failed: {}", res.cause().toString());
                        handler.handle(Future.failedFuture(res.cause()));
                    }
                }
        );
    }
}
