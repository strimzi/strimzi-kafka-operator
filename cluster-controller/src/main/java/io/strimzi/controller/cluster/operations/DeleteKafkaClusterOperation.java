package io.strimzi.controller.cluster.operations;

import io.strimzi.controller.cluster.K8SUtils;
import io.strimzi.controller.cluster.operations.kubernetes.DeletePersistentVolumeClaimOperation;
import io.strimzi.controller.cluster.resources.KafkaCluster;
import io.strimzi.controller.cluster.resources.Storage;
import io.vertx.core.*;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

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

                KafkaCluster kafka = KafkaCluster.fromStatefulSet(k8s, namespace, name);

                log.info("Deleting Kafka cluster {} from namespace {}", kafka.getName(), namespace);

                // start deleting configMap operation only if metrics are enabled,
                // otherwise the future is already complete (for the "join")
                Future<Void> futureConfigMap = Future.future();
                if (kafka.isMetricsEnabled()) {
                    OperationExecutor.getInstance().executeK8s(DeleteOperation.deleteConfigMap(namespace, kafka.getMetricsConfigName()), futureConfigMap.completer());
                } else {
                    futureConfigMap.complete();
                }

                Future<Void> futureService = Future.future();
                OperationExecutor.getInstance().executeFabric8(DeleteOperation.deleteService(namespace, kafka.getName()), futureService.completer());

                Future<Void> futureHeadlessService = Future.future();
                OperationExecutor.getInstance().executeFabric8(DeleteOperation.deleteService(namespace, kafka.getHeadlessName()), futureHeadlessService.completer());

                Future<Void> futureStatefulSet = Future.future();
                OperationExecutor.getInstance().executeK8s(DeleteOperation.deleteStatefulSet(namespace, kafka.getName()), futureStatefulSet.completer());

                Future<Void> futurePersistentVolumeClaim = Future.future();
                if ((kafka.getStorage().type() == Storage.StorageType.PERSISTENT_CLAIM) && kafka.getStorage().isDeleteClaim()) {

                    List<Future> futurePersistentVolumeClaims = new ArrayList<>();
                    for (int i = 0; i < kafka.getReplicas(); i++) {
                        Future<Void> f = Future.future();
                        futurePersistentVolumeClaims.add(f);
                        OperationExecutor.getInstance().executeK8s(new DeletePersistentVolumeClaimOperation(namespace, kafka.getVolumeName() + "-" + kafka.getName() + "-" + i), f.completer());
                    }
                    CompositeFuture.join(futurePersistentVolumeClaims).setHandler(ar -> {
                        if (ar.succeeded()) {
                            handler.handle(Future.succeededFuture());
                        } else {
                            handler.handle(Future.failedFuture("Failed to delete persistent volume claims"));
                        }
                    });
                } else {
                    futurePersistentVolumeClaim.complete();
                }

                CompositeFuture.join(futureConfigMap, futureService, futureHeadlessService, futureStatefulSet, futurePersistentVolumeClaim).setHandler(ar -> {
                    if (ar.succeeded()) {
                        log.info("Kafka cluster {} successfully deleted from namespace {}", kafka.getName(), namespace);
                        handler.handle(Future.succeededFuture());
                        lock.release();
                    } else {
                        log.error("Kafka cluster {} failed to delete from namespace {}", kafka.getName(), namespace);
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
