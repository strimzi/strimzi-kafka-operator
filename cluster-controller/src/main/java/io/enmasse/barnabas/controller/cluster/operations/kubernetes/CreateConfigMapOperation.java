package io.enmasse.barnabas.controller.cluster.operations.kubernetes;

import io.enmasse.barnabas.controller.cluster.K8SUtils;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateConfigMapOperation extends K8sOperation {

    private static final Logger log = LoggerFactory.getLogger(CreateConfigMapOperation.class.getName());
    private final ConfigMap cm;

    public CreateConfigMapOperation(ConfigMap cm) {
        this.cm = cm;
    }

    @Override
    public void execute(Vertx vertx, K8SUtils k8s, Handler<AsyncResult<Void>> handler) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    if (!k8s.configMapExists(cm.getMetadata().getNamespace(), cm.getMetadata().getName())) {
                        try {
                            log.info("Creating configmap {}", cm);
                            k8s.createConfigMap(cm);
                            future.complete();
                        } catch (Exception e) {
                            log.error("Caught exception while creating configmap", e);
                            future.fail(e);
                        }
                    }
                    else {
                        log.warn("Configmap {} already exists", cm);
                        future.complete();
                    }
                }, false,
                res -> {
                    if (res.succeeded()) {
                        log.info("Configmap {} has been created", cm);
                        handler.handle(Future.succeededFuture());
                    }
                    else {
                        log.error("Configmap creation failed: {}", res.result());
                        handler.handle(Future.failedFuture((Exception)res.result()));
                    }
                }
        );
    }
}
