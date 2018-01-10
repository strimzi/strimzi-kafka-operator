package io.strimzi.controller.cluster.operations;

import io.strimzi.controller.cluster.K8SUtils;
import io.strimzi.controller.cluster.operations.kubernetes.CreateConfigMapOperation;
import io.strimzi.controller.cluster.operations.kubernetes.CreateServiceOperation;
import io.strimzi.controller.cluster.operations.kubernetes.CreateStatefulSetOperation;
import io.strimzi.controller.cluster.resources.KafkaCluster;
import io.vertx.core.*;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateKafkaClusterOperation extends KafkaClusterOperation {
    private static final Logger log = LoggerFactory.getLogger(CreateKafkaClusterOperation.class.getName());

    public CreateKafkaClusterOperation(String namespace, String name) {
        super(namespace, name);
    }

    @Override
    public void execute(Vertx vertx, K8SUtils k8s, Handler<AsyncResult<Void>> handler) {
        vertx.sharedData().getLockWithTimeout(getLockName(), LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                Lock lock = res.result();

                log.info("Creating Kafka cluster {} in namespace {}", name, namespace);

                KafkaCluster kafka = KafkaCluster.fromConfigMap(k8s.getConfigmap(namespace, name));

                // start creating configMap operation only if metrics are enabled,
                // otherwise the future is already complete (for the "join")
                Future<Void> futureConfigMap = Future.future();
                if (kafka.isMetricsEnabled()) {
                    OperationExecutor.getInstance().execute(new CreateConfigMapOperation(kafka.generateMetricsConfigMap()), futureConfigMap.completer());
                } else {
                    futureConfigMap.complete();
                }

                Future<Void> futureService = Future.future();
                OperationExecutor.getInstance().execute(new CreateServiceOperation(kafka.generateService()), futureService.completer());

                Future<Void> futureHeadlessService = Future.future();
                OperationExecutor.getInstance().execute(new CreateServiceOperation(kafka.generateHeadlessService()), futureHeadlessService.completer());

                Future<Void> futureStatefulSet = Future.future();
                OperationExecutor.getInstance().execute(new CreateStatefulSetOperation(kafka.generateStatefulSet(k8s.isOpenShift())), futureStatefulSet.completer());

                CompositeFuture.join(futureConfigMap, futureService, futureHeadlessService, futureStatefulSet).setHandler(ar -> {
                    if (ar.succeeded()) {
                        log.info("Kafka cluster {} successfully created in namespace {}", name, namespace);
                        handler.handle(Future.succeededFuture());
                        lock.release();
                    } else {
                        log.error("Kafka cluster {} failed to create in namespace {}", name, namespace);
                        handler.handle(Future.failedFuture("Failed to create Kafka cluster"));
                        lock.release();
                    }
                });
            } else {
                log.error("Failed to acquire lock to create Kafka cluster {}", getLockName());
                handler.handle(Future.failedFuture("Failed to acquire lock to create Kafka cluster"));
            }
        });
    }
}
