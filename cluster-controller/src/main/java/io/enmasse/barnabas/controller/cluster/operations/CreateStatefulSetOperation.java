package io.enmasse.barnabas.controller.cluster.operations;

import io.enmasse.barnabas.controller.cluster.K8SUtils;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateStatefulSetOperation extends K8sOperation {
    private static final Logger log = LoggerFactory.getLogger(CreateStatefulSetOperation.class.getName());
    private final StatefulSet sfs;

    public CreateStatefulSetOperation(Vertx vertx, K8SUtils k8s, StatefulSet sfs) {
        super(vertx, k8s);
        this.sfs = sfs;
    }

    @Override
    public void execute(Handler<AsyncResult<Void>> handler) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    if (!k8s.statefulSetExists(sfs.getMetadata().getNamespace(), sfs.getMetadata().getName())) {
                        try {
                            log.info("Creating stateful set {}", sfs);
                            k8s.createStatefulSet(sfs);
                            future.complete();
                        } catch (Exception e) {
                            log.error("Caught exception while creating stateful set", e);
                            future.fail(e);
                        }
                    }
                    else {
                        log.warn("Stateful set {} already exists", sfs);
                        future.complete();
                    }
                },
                false,
                res -> {
                    if (res.succeeded()) {
                        log.info("Stateful set {} has been created", sfs);
                        handler.handle(Future.succeededFuture());
                    }
                    else {
                        log.error("Stateful set creation failed: {}", res.result());
                        handler.handle(Future.failedFuture((Exception)res.result()));
                    }
                }
        );
    }
}
