package io.strimzi.controller.cluster.operations;

import io.strimzi.controller.cluster.K8SUtils;
import io.strimzi.controller.cluster.operations.kubernetes.DeleteConfigMapOperation;
import io.strimzi.controller.cluster.operations.kubernetes.DeleteServiceOperation;
import io.strimzi.controller.cluster.operations.kubernetes.DeleteStatefulSetOperation;
import io.strimzi.controller.cluster.resources.ZookeeperCluster;
import io.vertx.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.shareddata.Lock;

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

                log.info("Deleting Zookeeper cluster {} from namespace {}", name + "-zookeeper", namespace);

                ZookeeperCluster zk = ZookeeperCluster.fromStatefulSet(k8s.getStatefulSet(namespace, name + "-zookeeper"));

                // start deleting configMap operation only if metrics are enabled,
                // otherwise the future is already complete (for the "join")
                Future<Void> futureConfigMap = Future.future();
                if (zk.isMetricsEnabled()) {
                    OperationExecutor.getInstance().execute(new DeleteConfigMapOperation(namespace, zk.getMetricsConfigName()), futureConfigMap.completer());
                } else {
                    futureConfigMap.complete();
                }

                Future<Void> futureService = Future.future();
                OperationExecutor.getInstance().execute(new DeleteServiceOperation(namespace, name + "-zookeeper"), futureService.completer());

                Future<Void> futureHeadlessService = Future.future();
                OperationExecutor.getInstance().execute(new DeleteServiceOperation(namespace, zk.getHeadlessName()), futureHeadlessService.completer());

                Future<Void> futureStatefulSet = Future.future();
                OperationExecutor.getInstance().execute(new DeleteStatefulSetOperation(namespace, name + "-zookeeper"), futureStatefulSet.completer());

                CompositeFuture.join(futureConfigMap, futureService, futureHeadlessService, futureStatefulSet).setHandler(ar -> {
                    if (ar.succeeded()) {
                        log.info("Zookeeper cluster {} successfully deleted from namespace {}", name + "-zookeeper", namespace);
                        handler.handle(Future.succeededFuture());
                        lock.release();
                    } else {
                        log.error("Zookeeper cluster {} failed to delete from namespace {}", name + "-zookeeper", namespace);
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
