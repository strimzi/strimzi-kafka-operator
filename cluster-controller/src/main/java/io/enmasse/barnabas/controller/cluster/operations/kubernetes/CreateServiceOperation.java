package io.enmasse.barnabas.controller.cluster.operations.kubernetes;

import io.enmasse.barnabas.controller.cluster.K8SUtils;
import io.fabric8.kubernetes.api.model.Service;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateServiceOperation extends K8sOperation {
    private static final Logger log = LoggerFactory.getLogger(CreateServiceOperation.class.getName());
    private final Service svc;

    public CreateServiceOperation(Service svc) {
        this.svc = svc;
    }

    @Override
    public void execute(Vertx vertx, K8SUtils k8s, Handler<AsyncResult<Void>> handler) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    if (!k8s.serviceExists(svc.getMetadata().getNamespace(), svc.getMetadata().getName())) {
                        try {
                            log.info("Creating service {}", svc);
                            k8s.createService(svc);
                            future.complete();
                        } catch (Exception e) {
                            log.error("Caught exception while creating service", e);
                            future.fail(e);
                        }
                    }
                    else {
                        log.warn("Service {} already exists", svc);
                        future.complete();
                    }
                },
                false,
                res -> {
                    if (res.succeeded()) {
                        log.info("Service {} has been created", svc);
                        handler.handle(Future.succeededFuture());
                    }
                    else {
                        log.error("Service creation failed: {}", res.result());
                        handler.handle(Future.failedFuture((Exception)res.result()));
                    }
                }
        );
    }
}
