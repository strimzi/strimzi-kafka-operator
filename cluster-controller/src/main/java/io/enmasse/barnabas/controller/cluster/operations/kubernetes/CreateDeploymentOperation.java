package io.enmasse.barnabas.controller.cluster.operations.kubernetes;

import io.enmasse.barnabas.controller.cluster.K8SUtils;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateDeploymentOperation extends K8sOperation {
    private static final Logger log = LoggerFactory.getLogger(CreateDeploymentOperation.class.getName());
    private final Deployment dep;

    public CreateDeploymentOperation(Deployment dep) {
        this.dep = dep;
    }

    @Override
    public void execute(Vertx vertx, K8SUtils k8s, Handler<AsyncResult<Void>> handler) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    if (!k8s.deploymentExists(dep.getMetadata().getNamespace(), dep.getMetadata().getName())) {
                        try {
                            log.info("Creating deployment {}", dep);
                            k8s.createDeployment(dep);
                            future.complete();
                        } catch (Exception e) {
                            log.error("Caught exception while creating deployment", e);
                            future.fail(e);
                        }
                    }
                    else {
                        log.warn("Deployment {} already exists", dep);
                        future.complete();
                    }
                },
                false,
                res -> {
                    if (res.succeeded()) {
                        log.info("Deployment {} has been created", dep);
                        handler.handle(Future.succeededFuture());
                    }
                    else {
                        log.error("Deployment creation failed: {}", res.result());
                        handler.handle(Future.failedFuture((Exception)res.result()));
                    }
                }
        );
    }
}
