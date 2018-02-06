package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.strimzi.controller.cluster.operations.resource.ConfigMapOperations;
import io.strimzi.controller.cluster.operations.resource.PvcOperations;
import io.strimzi.controller.cluster.operations.resource.ServiceOperations;
import io.strimzi.controller.cluster.operations.resource.StatefulSetOperations;
import io.strimzi.controller.cluster.resources.ClusterDiffResult;
import io.strimzi.controller.cluster.resources.Storage;
import io.strimzi.controller.cluster.resources.ZookeeperCluster;
import io.vertx.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * CRUD-style operations on a Zookeeper cluster
 */
public class ZookeeperClusterOperations extends AbstractClusterOperations<ZookeeperCluster> {

    private static final Logger log = LoggerFactory.getLogger(ZookeeperClusterOperations.class.getName());
    private final ServiceOperations serviceOperations;
    private final StatefulSetOperations statefulSetOperations;
    private final ConfigMapOperations configMapOperations;
    private final PvcOperations pvcOperations;

    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param isOpenShift Whether we're running with OpenShift
     * @param configMapOperations For operating on ConfigMaps
     * @param serviceOperations For operating on Services
     * @param statefulSetOperations For operating on StatefulSets
     * @param pvcOperations For operating on PersistentVolumeClaims
     */
    public ZookeeperClusterOperations(Vertx vertx, boolean isOpenShift,
                                      ConfigMapOperations configMapOperations,
                                      ServiceOperations serviceOperations,
                                      StatefulSetOperations statefulSetOperations,
                                      PvcOperations pvcOperations) {
        super(vertx, isOpenShift, "zookeeper", "create");
        this.serviceOperations = serviceOperations;
        this.statefulSetOperations = statefulSetOperations;
        this.configMapOperations = configMapOperations;
        this.pvcOperations = pvcOperations;
    }

    private final CompositeOperation<ZookeeperCluster> create = new CompositeOperation<ZookeeperCluster>() {

        @Override
        public ClusterOperation<ZookeeperCluster> getCluster(String namespace, String name) {
            return new ClusterOperation<>(ZookeeperCluster.fromConfigMap(configMapOperations.get(namespace, name)), null);
        }

        @Override
        public Future<?> composite(String namespace, ClusterOperation<ZookeeperCluster> clusterOp) {
            ZookeeperCluster zk = clusterOp.cluster();
            List<Future> result = new ArrayList<>(4);

            if (zk.isMetricsEnabled()) {
                result.add(configMapOperations.create(zk.generateMetricsConfigMap()));
            }

            result.add(serviceOperations.create(zk.generateService()));

            result.add(serviceOperations.create(zk.generateHeadlessService()));

            result.add(statefulSetOperations.create(zk.generateStatefulSet(isOpenShift)));

            return CompositeFuture.join(result);
        }
    };

    @Override
    protected CompositeOperation<ZookeeperCluster> createOp() {
        return create;
    }

    private final CompositeOperation<ZookeeperCluster> delete = new CompositeOperation<ZookeeperCluster>() {
        @Override
        public Future<?> composite(String namespace, ClusterOperation<ZookeeperCluster> clusterOp) {
            ZookeeperCluster zk = clusterOp.cluster();
            boolean deleteClaims = zk.getStorage().type() == Storage.StorageType.PERSISTENT_CLAIM
                    && zk.getStorage().isDeleteClaim();
            List<Future> result = new ArrayList<>(4 + (deleteClaims ? zk.getReplicas() : 0));

            // start deleting configMap operation only if metrics are enabled,
            // otherwise the future is already complete (for the "join")
            if (zk.isMetricsEnabled()) {
                result.add(configMapOperations.delete(namespace, zk.getMetricsConfigName()));
            }

            result.add(serviceOperations.delete(namespace, zk.getName()));

            result.add(serviceOperations.delete(namespace, zk.getHeadlessName()));

            result.add(statefulSetOperations.delete(namespace, zk.getName()));


            if (deleteClaims) {
                for (int i = 0; i < zk.getReplicas(); i++) {
                    result.add(pvcOperations.delete(namespace, zk.getVolumeName() + "-" + zk.getName() + "-" + i));
                }
            }

            return CompositeFuture.join(result);
        }

        @Override
        public ClusterOperation<ZookeeperCluster> getCluster(String namespace, String name) {
            StatefulSet ss = statefulSetOperations.get(namespace, ZookeeperCluster.zookeeperClusterName(name));
            return new ClusterOperation<ZookeeperCluster>(ZookeeperCluster.fromStatefulSet(ss, namespace, name), null);
        }
    };


    @Override
    protected CompositeOperation<ZookeeperCluster> deleteOp() {
        return delete;
    }

    @Override
    protected CompositeOperation<ZookeeperCluster> updateOp() {
        return update;
    }

    private final CompositeOperation<ZookeeperCluster> update = new CompositeOperation<ZookeeperCluster>() {
        @Override
        public Future<?> composite(String namespace, ClusterOperation<ZookeeperCluster> operation) {
            ZookeeperCluster zk = operation.cluster();
            ClusterDiffResult diff = operation.diff();
            Future<Void> chainFuture = Future.future();

            scaleDown(zk, namespace, diff)
                    .compose(i -> patchService(zk, namespace, diff))
                    .compose(i -> patchHeadlessService(zk, namespace, diff))
                    .compose(i -> patchStatefulSet(zk, namespace, diff))
                    .compose(i -> patchMetricsConfigMap(zk, namespace, diff))
                    .compose(i -> rollingUpdate(zk, namespace, diff))
                    .compose(i -> scaleUp(zk, namespace, diff))
                    .compose(chainFuture::complete, chainFuture);

            return chainFuture;
        }

        @Override
        public ClusterOperation<ZookeeperCluster> getCluster(String namespace, String name) {
            ClusterDiffResult diff;
            ZookeeperCluster zk;
            ConfigMap zkConfigMap = configMapOperations.get(namespace, name);

            if (zkConfigMap != null)    {
                zk = ZookeeperCluster.fromConfigMap(zkConfigMap);
                log.info("Updating Zookeeper cluster {} in namespace {}", zk.getName(), namespace);
                diff = zk.diff(configMapOperations, statefulSetOperations, namespace);
            } else {
                throw new IllegalStateException("ConfigMap " + name + " doesn't exist anymore in namespace " + namespace);
            }

            return new ClusterOperation<>(zk, diff);
        }
    };

    private Future<Void> scaleDown(ZookeeperCluster zk, String namespace, ClusterDiffResult diff) {
        Future<Void> scaleDown = Future.future();

        if (diff.isScaleDown())    {
            log.info("Scaling down stateful set {} in namespace {}", zk.getName(), namespace);
            statefulSetOperations.scaleDown(namespace, zk.getName(), zk.getReplicas(), scaleDown.completer());
        }
        else {
            scaleDown.complete();
        }

        return scaleDown;
    }

    private Future<Void> patchService(ZookeeperCluster zk, String namespace, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            return serviceOperations.patch(namespace, zk.getName(),
                    zk.patchService(serviceOperations.get(namespace, zk.getName())));
        }
        else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchHeadlessService(ZookeeperCluster zk, String namespace, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            return serviceOperations.patch(namespace, zk.getHeadlessName(),
                    zk.patchHeadlessService(serviceOperations.get(namespace, zk.getHeadlessName())));
        }
        else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchStatefulSet(ZookeeperCluster zk, String namespace, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            return statefulSetOperations.patch(namespace, zk.getName(), false,
                    zk.patchStatefulSet(statefulSetOperations.get(namespace, zk.getName())));
        }
        else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchMetricsConfigMap(ZookeeperCluster zk, String namespace, ClusterDiffResult diff) {
        if (diff.isMetricsChanged()) {
            return configMapOperations.patch(namespace, zk.getMetricsConfigName(),
                    zk.patchMetricsConfigMap(configMapOperations.get(namespace, zk.getMetricsConfigName())));
        } else {
            return Future.succeededFuture();
        }
    }

    private Future<Void> rollingUpdate(ZookeeperCluster zk, String namespace, ClusterDiffResult diff) {
        Future<Void> rollingUpdate = Future.future();

        if (diff.isRollingUpdate()) {
            statefulSetOperations.rollingUpdate(namespace, zk.getName(),
                    rollingUpdate.completer());
        }
        else {
            rollingUpdate.complete();
        }

        return rollingUpdate;
    }

    private Future<Void> scaleUp(ZookeeperCluster zk, String namespace, ClusterDiffResult diff) {
        Future<Void> scaleUp = Future.future();

        if (diff.isScaleUp()) {
            statefulSetOperations.scaleUp(namespace, zk.getName(), zk.getReplicas(), scaleUp.completer());
        }
        else {
            scaleUp.complete();
        }

        return scaleUp;
    }
}
