package io.strimzi.controller.cluster.operations;

import io.strimzi.controller.cluster.K8SUtils;
import io.strimzi.controller.cluster.operations.kubernetes.CreateDeploymentOperation;
import io.strimzi.controller.cluster.operations.kubernetes.CreateServiceOperation;
import io.strimzi.controller.cluster.resources.KafkaConnectCluster;
import io.vertx.core.*;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateKafkaConnectClusterOperation extends KafkaConnectClusterOperation {
    private static final Logger log = LoggerFactory.getLogger(CreateKafkaConnectClusterOperation.class.getName());

    public CreateKafkaConnectClusterOperation(String namespace, String name) {
        super(namespace, name);
    }

    @Override
    public void execute(Vertx vertx, K8SUtils k8s, Handler<AsyncResult<Void>> handler) {
        vertx.sharedData().getLockWithTimeout(getLockName(), LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                Lock lock = res.result();

                log.info("Creating Kafka Connect cluster {} in namespace {}", name, namespace);

                KafkaConnectCluster connect = KafkaConnectCluster.fromConfigMap(k8s.getConfigmap(namespace, name));

                Future<Void> futureService = Future.future();
                OperationExecutor.getInstance().execute(new CreateServiceOperation(connect.generateService()), futureService.completer());

                Future<Void> futureDeployment = Future.future();
                OperationExecutor.getInstance().execute(new CreateDeploymentOperation(connect.generateDeployment()), futureDeployment.completer());

                CompositeFuture.join(futureService, futureDeployment).setHandler(ar -> {
                    if (ar.succeeded()) {
                        log.info("Kafka Connect cluster {} successfully created in namespace {}", name, namespace);
                        handler.handle(Future.succeededFuture());
                        lock.release();
                    } else {
                        log.error("Kafka Connect cluster {} failed to create in namespace {}", name, namespace);
                        handler.handle(Future.failedFuture("Failed to create Kafka Connect cluster"));
                        lock.release();
                    }
                });
            } else {
                log.error("Failed to acquire lock to create Kafka Connect cluster {}", getLockName());
                handler.handle(Future.failedFuture("Failed to acquire lock to create Kafka Connect cluster"));
            }
        });
    }
}
