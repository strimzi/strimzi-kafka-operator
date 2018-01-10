package io.strimzi.controller.cluster.operations;

import io.strimzi.controller.cluster.K8SUtils;
import io.strimzi.controller.cluster.operations.kubernetes.DeleteDeploymentOperation;
import io.strimzi.controller.cluster.operations.kubernetes.DeleteServiceOperation;
import io.vertx.core.*;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteKafkaConnectClusterOperation extends KafkaConnectClusterOperation {
    private static final Logger log = LoggerFactory.getLogger(DeleteKafkaConnectClusterOperation.class.getName());

    public DeleteKafkaConnectClusterOperation(String namespace, String name) {
        super(namespace, name);
    }

    @Override
    public void execute(Vertx vertx, K8SUtils k8s, Handler<AsyncResult<Void>> handler) {
        vertx.sharedData().getLockWithTimeout(getLockName(), LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                Lock lock = res.result();

                log.info("Deleting Kafka Connect cluster {} from namespace {}", name, namespace);

                Future<Void> futureService = Future.future();
                OperationExecutor.getInstance().execute(new DeleteServiceOperation(namespace, name), futureService.completer());

                Future<Void> futureDeployment = Future.future();
                OperationExecutor.getInstance().execute(new DeleteDeploymentOperation(namespace, name), futureDeployment.completer());

                CompositeFuture.join(futureService, futureDeployment).setHandler(ar -> {
                    if (ar.succeeded()) {
                        log.info("Kafka Connect cluster {} successfully deleted from namespace {}", name, namespace);
                        handler.handle(Future.succeededFuture());
                        lock.release();
                    } else {
                        log.error("Kafka Connect cluster {} failed to delete from namespace {}", name, namespace);
                        handler.handle(Future.failedFuture("Failed to delete Kafka Connect cluster"));
                        lock.release();
                    }
                });
            } else {
                log.error("Failed to acquire lock to delete Kafka Connect cluster {}", getLockName());
                handler.handle(Future.failedFuture("Failed to acquire lock to delete Kafka Connect cluster"));
            }
        });
    }
}
