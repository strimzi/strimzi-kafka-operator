package io.enmasse.barnabas.controller.cluster.operations.cluster;

import io.enmasse.barnabas.controller.cluster.K8SUtils;
import io.enmasse.barnabas.controller.cluster.operations.OperationExecutor;
import io.enmasse.barnabas.controller.cluster.operations.kubernetes.DeleteServiceOperation;
import io.enmasse.barnabas.controller.cluster.operations.kubernetes.DeleteStatefulSetOperation;
import io.enmasse.barnabas.controller.cluster.resources.KafkaResource;
import io.vertx.core.*;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteKafkaClusterOperation extends KafkaClusterOperation {
    private static final Logger log = LoggerFactory.getLogger(DeleteKafkaClusterOperation.class.getName());

    public DeleteKafkaClusterOperation(String namespace, String name) {
        super(namespace, name);
    }

    @Override
    public void execute(Vertx vertx, K8SUtils k8s, Handler<AsyncResult<Void>> handler) {
        vertx.sharedData().getLockWithTimeout(getLockName(), LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                Lock lock = res.result();

                log.info("Deleting Kafka cluster {} from namespace {}", name, namespace);

                KafkaResource kafka = KafkaResource.fromStatefulSet(k8s.getStatefulSet(namespace, name));

                Future<Void> futureService = Future.future();
                OperationExecutor.getInstance().execute(new DeleteServiceOperation(namespace, name), futureService.completer());

                Future<Void> futureHeadlessService = Future.future();
                OperationExecutor.getInstance().execute(new DeleteServiceOperation(namespace, kafka.getHeadlessName()), futureHeadlessService.completer());

                Future<Void> futureStatefulSet = Future.future();
                OperationExecutor.getInstance().execute(new DeleteStatefulSetOperation(namespace, name), futureStatefulSet.completer());

                CompositeFuture.join(futureService, futureHeadlessService, futureStatefulSet).setHandler(ar -> {
                    if (ar.succeeded()) {
                        log.info("Kafka cluster {} successfully deleted from namespace {}", name, namespace);
                        handler.handle(Future.succeededFuture());
                        lock.release();
                    } else {
                        log.error("Kafka cluster {} failed to delete from namespace {}", name, namespace);
                        handler.handle(Future.failedFuture("Failed to delete Zookeeper cluster"));
                        lock.release();
                    }
                });
            } else {
                log.error("Failed to acquire lock to delete Kafka cluster {}", getLockName());
                handler.handle(Future.failedFuture("Failed to acquire lock to delete Kafka cluster"));
            }
        });
    }
}
