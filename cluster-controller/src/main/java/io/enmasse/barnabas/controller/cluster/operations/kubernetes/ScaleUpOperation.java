package io.enmasse.barnabas.controller.cluster.operations.kubernetes;

import io.enmasse.barnabas.controller.cluster.K8SUtils;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScaleUpOperation extends K8sOperation {
    private static final Logger log = LoggerFactory.getLogger(ScaleUpOperation.class.getName());
    private final ScalableResource res;
    private final int scaleTo;

    public ScaleUpOperation(ScalableResource res, int scaleTo) {
        this.res = res;
        this.scaleTo = scaleTo;
    }

    @Override
    public void execute(Vertx vertx, K8SUtils k8s, Handler<AsyncResult<Void>> handler) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    try {
                        log.info("Scaling up to {} replicas", scaleTo);
                        k8s.scale(res, scaleTo, true);
                        future.complete();
                    }
                    catch (Exception e) {
                        log.error("Caught exception while scaling up", e);
                        future.fail(e);
                    }
                },
                false,
                res -> {
                    if (res.succeeded()) {
                        log.info("Scaling up to {} replicas has been completed", scaleTo);
                        handler.handle(Future.succeededFuture());
                    }
                    else {
                        log.error("Scaling up has failed: {}", res.result());
                        handler.handle(Future.failedFuture((Exception)res.result()));
                    }
                }
        );
    }
}
