package io.strimzi.controller.cluster.operations.kubernetes;

import io.strimzi.controller.cluster.K8SUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteStatefulSetOperation extends K8sOperation {
    private static final Logger log = LoggerFactory.getLogger(DeleteStatefulSetOperation.class.getName());
    private final String namespace;
    private final String name;

    public DeleteStatefulSetOperation(String namespace, String name) {
        this.namespace = namespace;
        this.name = name;
    }

    @Override
    public void execute(Vertx vertx, K8SUtils k8s, Handler<AsyncResult<Void>> handler) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    if (k8s.statefulSetExists(namespace, name)) {
                        try {
                            log.info("Deleting stateful set {} in namespace {}", name, namespace);
                            k8s.deleteStatefulSet(namespace, name);
                            future.complete();
                        } catch (Exception e) {
                            log.error("Caught exception while deleting stateful set", e);
                            future.fail(e);
                        }
                    }
                    else {
                        log.warn("Stateful set {} in namespace {} doesn't exists", name, namespace);
                        future.complete();
                    }
                },
                false,
                res -> {
                    if (res.succeeded()) {
                        log.info("Stateful set {} in namespace {} has been deleted", name, namespace);
                        handler.handle(Future.succeededFuture());
                    }
                    else {
                        log.error("Stateful set deletion failed: {}", res.cause().toString());
                        handler.handle(Future.failedFuture(res.cause()));
                    }
                }
        );
    }
}
