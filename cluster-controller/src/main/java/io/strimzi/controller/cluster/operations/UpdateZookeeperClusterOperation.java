package io.strimzi.controller.cluster.operations;

import io.strimzi.controller.cluster.K8SUtils;
import io.strimzi.controller.cluster.operations.kubernetes.ManualRollingUpdateOperation;
import io.strimzi.controller.cluster.operations.kubernetes.PatchOperation;
import io.strimzi.controller.cluster.operations.kubernetes.ScaleDownOperation;
import io.strimzi.controller.cluster.operations.kubernetes.ScaleUpOperation;
import io.strimzi.controller.cluster.resources.ClusterDiffResult;
import io.strimzi.controller.cluster.resources.ZookeeperCluster;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.vertx.core.*;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateZookeeperClusterOperation extends ZookeeperClusterOperation {
    private static final Logger log = LoggerFactory.getLogger(UpdateZookeeperClusterOperation.class.getName());

    private K8SUtils k8s;

    public UpdateZookeeperClusterOperation(String namespace, String name) {
        super(namespace, name);
    }

    @Override
    public void execute(Vertx vertx, K8SUtils k8s, Handler<AsyncResult<Void>> handler) {
        this.k8s = k8s;

        vertx.sharedData().getLockWithTimeout(getLockName(), LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                Lock lock = res.result();

                log.info("Updating Zookeeper cluster {} in namespace {}", name + "-zookeeper", namespace);

                ClusterDiffResult diff;
                ZookeeperCluster zk;
                ConfigMap zkConfigMap = k8s.getConfigmap(namespace, name);

                if (zkConfigMap != null)    {

                    try {
                        zk = ZookeeperCluster.fromConfigMap(zkConfigMap);
                        diff = zk.diff(k8s.getStatefulSet(namespace, name + "-zookeeper"), k8s.getConfigmap(namespace, name + "-zookeeper-metrics-config"));
                    } catch (Exception ex) {
                        log.error("Error while parsing cluster ConfigMap", ex);
                        handler.handle(Future.failedFuture("ConfigMap parsing error"));
                        lock.release();
                        return;
                    }

                } else {
                    log.error("ConfigMap {} doesn't exist anymore in namespace {}", name, namespace);
                    handler.handle(Future.failedFuture("ConfigMap doesn't exist anymore"));
                    lock.release();
                    return;
                }

                Future<Void> chainFuture = Future.future();

                scaleDown(zk, diff)
                        .compose(i -> patchService(zk, diff))
                        .compose(i -> patchHeadlessService(zk, diff))
                        .compose(i -> patchStatefulSet(zk, diff))
                        .compose(i -> patchMetricsConfigMap(zk, diff))
                        .compose(i -> rollingUpdate(diff))
                        .compose(i -> scaleUp(zk, diff))
                        .compose(chainFuture::complete, chainFuture);

                chainFuture.setHandler(ar -> {
                    if (ar.succeeded()) {
                        log.info("Zookeeper cluster {} successfully updated in namespace {}", name + "-zookeeper", namespace);
                        handler.handle(Future.succeededFuture());
                        lock.release();
                    } else {
                        log.error("Zookeeper cluster {} failed to update in namespace {}", name + "-zookeeper", namespace);
                        handler.handle(Future.failedFuture("Failed to update Zookeeper cluster"));
                        lock.release();
                    }
                });
            } else {
                log.error("Failed to acquire lock to create Zookeeper cluster {}", getLockName());
                handler.handle(Future.failedFuture("Failed to acquire lock to create Zookeeper cluster"));
            }
        });
    }

    private Future<Void> scaleDown(ZookeeperCluster zk, ClusterDiffResult diff) {
        Future<Void> scaleDown = Future.future();

        if (diff.getScaleDown())    {
            log.info("Scaling down stateful set {} in namespace {}", name + "-zookeeper", namespace);
            OperationExecutor.getInstance().execute(new ScaleDownOperation(k8s.getStatefulSetResource(namespace, name + "-zookeeper"), zk.getReplicas()), scaleDown.completer());
        }
        else {
            scaleDown.complete();
        }

        return scaleDown;
    }

    private Future<Void> patchService(ZookeeperCluster zk, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            Future<Void> patchService = Future.future();
            OperationExecutor.getInstance().execute(new PatchOperation(k8s.getServiceResource(namespace, name + "-zookeeper"), zk.patchService(k8s.getService(namespace, name + "-zookeeper"))), patchService.completer());
            return patchService;
        }
            else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchHeadlessService(ZookeeperCluster zk, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            Future<Void> patchService = Future.future();
            OperationExecutor.getInstance().execute(new PatchOperation(k8s.getServiceResource(namespace, zk.getHeadlessName()), zk.patchHeadlessService(k8s.getService(namespace, zk.getHeadlessName()))), patchService.completer());
            return patchService;
        }
            else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchStatefulSet(ZookeeperCluster zk, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            Future<Void> patchStatefulSet = Future.future();
            OperationExecutor.getInstance().execute(new PatchOperation(k8s.getStatefulSetResource(namespace, name + "-zookeeper").cascading(false), zk.patchStatefulSet(k8s.getStatefulSet(namespace, name + "-zookeeper"))), patchStatefulSet.completer());
            return patchStatefulSet;
        }
        else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchMetricsConfigMap(ZookeeperCluster zk, ClusterDiffResult diff) {
        if (diff.isMetricsChanged()) {
            Future<Void> patchConfigMap = Future.future();
            OperationExecutor.getInstance().execute(new PatchOperation(k8s.getConfigmapResource(namespace, zk.getMetricsConfigName()), zk.patchMetricsConfigMap(k8s.getConfigmap(namespace, name + "-zookeeper-metrics-config"))), patchConfigMap.completer());
            return patchConfigMap;
        } else {
            return Future.succeededFuture();
        }
    }

    private Future<Void> rollingUpdate(ClusterDiffResult diff) {
        Future<Void> rollingUpdate = Future.future();

        if (diff.getRollingUpdate()) {
            OperationExecutor.getInstance().execute(new ManualRollingUpdateOperation(namespace, name + "-zookeeper", k8s.getStatefulSet(namespace, name + "-zookeeper").getSpec().getReplicas()), rollingUpdate.completer());
        }
        else {
            rollingUpdate.complete();
        }

        return rollingUpdate;
    }

    private Future<Void> scaleUp(ZookeeperCluster zk, ClusterDiffResult diff) {
        Future<Void> scaleUp = Future.future();

        if (diff.getScaleUp()) {
            OperationExecutor.getInstance().execute(new ScaleUpOperation(k8s.getStatefulSetResource(namespace, name + "-zookeeper"), zk.getReplicas()), scaleUp.completer());
        }
        else {
            scaleUp.complete();
        }

        return scaleUp;
    }
}
