package io.strimzi.controller.cluster.operations;

import io.strimzi.controller.cluster.K8SUtils;
import io.strimzi.controller.cluster.operations.kubernetes.DeleteConfigMapOperation;
import io.strimzi.controller.cluster.operations.kubernetes.DeletePersistentVolumeClaimOperation;
import io.strimzi.controller.cluster.operations.kubernetes.DeleteServiceOperation;
import io.strimzi.controller.cluster.operations.kubernetes.DeleteStatefulSetOperation;
import io.strimzi.controller.cluster.resources.Storage;
import io.strimzi.controller.cluster.resources.ZookeeperCluster;
import io.vertx.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.shareddata.Lock;

import java.util.ArrayList;
import java.util.List;

public class DeleteZookeeperClusterOperation extends ZookeeperClusterOperation {
    private static final Logger log = LoggerFactory.getLogger(DeleteZookeeperClusterOperation.class.getName());

    public DeleteZookeeperClusterOperation(String namespace, String name) {
        super(namespace, name);
    }

    @Override
    public void execute(Vertx vertx, K8SUtils k8s, Handler<AsyncResult<Void>> handler) {
        vertx.sharedData().getLockWithTimeout(getLockName(), LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                Lock lock = res.result();

                ZookeeperCluster zk = ZookeeperCluster.fromStatefulSet(k8s, namespace, name);

                log.info("Deleting Zookeeper cluster {} from namespace {}", zk.getName(), namespace);

                // start deleting configMap operation only if metrics are enabled,
                // otherwise the future is already complete (for the "join")
                Future<Void> futureConfigMap = Future.future();
                if (zk.isMetricsEnabled()) {
                    OperationExecutor.getInstance().execute(new DeleteConfigMapOperation(namespace, zk.getMetricsConfigName()), futureConfigMap.completer());
                } else {
                    futureConfigMap.complete();
                }

                Future<Void> futureService = Future.future();
                OperationExecutor.getInstance().execute(new DeleteServiceOperation(namespace, zk.getName()), futureService.completer());

                Future<Void> futureHeadlessService = Future.future();
                OperationExecutor.getInstance().execute(new DeleteServiceOperation(namespace, zk.getHeadlessName()), futureHeadlessService.completer());

                Future<Void> futureStatefulSet = Future.future();
                OperationExecutor.getInstance().execute(new DeleteStatefulSetOperation(namespace, zk.getName()), futureStatefulSet.completer());

                Future<Void> futurePersistentVolumeClaim = Future.future();
                if ((zk.getStorage().type() == Storage.StorageType.PERSISTENT_CLAIM) && zk.getStorage().isDeleteClaim()) {

                    List<Future> futurePersistentVolumeClaims = new ArrayList<>();
                    for (int i = 0; i < zk.getReplicas(); i++) {
                        Future<Void> f = Future.future();
                        futurePersistentVolumeClaims.add(f);
                        OperationExecutor.getInstance().execute(new DeletePersistentVolumeClaimOperation(namespace, zk.getVolumeName() + "-" + zk.getName() + "-" + i), f.completer());
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
                        log.info("Zookeeper cluster {} successfully deleted from namespace {}", zk.getName(), namespace);
                        handler.handle(Future.succeededFuture());
                        lock.release();
                    } else {
                        log.error("Zookeeper cluster {} failed to delete from namespace {}", zk.getName(), namespace);
                        handler.handle(Future.failedFuture("Failed to delete Zookeeper cluster"));
                        lock.release();
                    }
                });
            } else {
                log.error("Failed to acquire lock to delete Zookeeper cluster {}", getLockName());
                handler.handle(Future.failedFuture("Failed to acquire lock to delete Zookeeper cluster"));
            }
        });
    }
}
