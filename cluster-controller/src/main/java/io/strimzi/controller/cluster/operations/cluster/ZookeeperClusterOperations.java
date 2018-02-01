package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.client.OpenShiftClient;
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
import io.vertx.core.shareddata.Lock;

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
     * @param client The kubernetes client
     * @param configMapOperations For operating on ConfigMaps
     * @param serviceOperations For operating on Services
     * @param statefulSetOperations For operating on StatefulSets
     * @param pvcOperations For operating on PersistentVolumeClaims
     */
    public ZookeeperClusterOperations(Vertx vertx, KubernetesClient client,
                                      ConfigMapOperations configMapOperations,
                                      ServiceOperations serviceOperations,
                                      StatefulSetOperations statefulSetOperations,
                                      PvcOperations pvcOperations) {
        super(vertx, client, "zookeeper", "create");
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
                Future<Void> futureConfigMap = Future.future();
                configMapOperations.create(zk.generateMetricsConfigMap(), futureConfigMap.completer());
                result.add(futureConfigMap);
            }

            Future<Void> futureService = Future.future();
            serviceOperations.create(zk.generateService(), futureService.completer());
            result.add(futureService);

            Future<Void> futureHeadlessService = Future.future();
            serviceOperations.create(zk.generateHeadlessService(), futureHeadlessService.completer());
            result.add(futureHeadlessService);

            Future<Void> futureStatefulSet = Future.future();
            statefulSetOperations.create(zk.generateStatefulSet(client.isAdaptable(OpenShiftClient.class)), futureStatefulSet.completer());
            result.add(futureStatefulSet);

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
                Future<Void> futureConfigMap = Future.future();
                configMapOperations.delete(namespace, zk.getMetricsConfigName(), futureConfigMap.completer());
                result.add(futureConfigMap);
            }

            Future<Void> futureService = Future.future();
            serviceOperations.delete(namespace, zk.getName(), futureService.completer());
            result.add(futureService);

            Future<Void> futureHeadlessService = Future.future();
            serviceOperations.delete(namespace, zk.getHeadlessName(), futureHeadlessService.completer());
            result.add(futureHeadlessService);

            Future<Void> futureStatefulSet = Future.future();
            statefulSetOperations.delete(namespace, zk.getName(), futureStatefulSet.completer());
            result.add(futureStatefulSet);


            if (deleteClaims) {
                for (int i = 0; i < zk.getReplicas(); i++) {
                    Future<Void> f = Future.future();
                    pvcOperations.delete(namespace, zk.getVolumeName() + "-" + zk.getName() + "-" + i, f.completer());
                    result.add(f);
                }
            }

            return CompositeFuture.join(result);
        }

        @Override
        public ClusterOperation<ZookeeperCluster> getCluster(String namespace, String name) {
            return new ClusterOperation<ZookeeperCluster>(ZookeeperCluster.fromStatefulSet(statefulSetOperations, namespace, name), null);
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

        if (diff.getScaleDown())    {
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
            Future<Void> patchService = Future.future();
            serviceOperations.patch(namespace, zk.getName(),
                    zk.patchService(serviceOperations.get(namespace, zk.getName())), patchService.completer());
            return patchService;
        }
        else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchHeadlessService(ZookeeperCluster zk, String namespace, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            Future<Void> patchService = Future.future();
            serviceOperations.patch(namespace, zk.getHeadlessName(),
                    zk.patchHeadlessService(serviceOperations.get(namespace, zk.getHeadlessName())), patchService.completer());
            return patchService;
        }
        else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchStatefulSet(ZookeeperCluster zk, String namespace, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            Future<Void> patchStatefulSet = Future.future();
            statefulSetOperations.patch(namespace, zk.getName(), false,
                    zk.patchStatefulSet(statefulSetOperations.get(namespace, zk.getName())), patchStatefulSet.completer());
            return patchStatefulSet;
        }
        else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchMetricsConfigMap(ZookeeperCluster zk, String namespace, ClusterDiffResult diff) {
        if (diff.isMetricsChanged()) {
            Future<Void> patchConfigMap = Future.future();
            configMapOperations.patch(namespace, zk.getMetricsConfigName(),
                    zk.patchMetricsConfigMap(configMapOperations.get(namespace, zk.getMetricsConfigName())),
                    patchConfigMap.completer());
            return patchConfigMap;
        } else {
            return Future.succeededFuture();
        }
    }

    private Future<Void> rollingUpdate(ZookeeperCluster zk, String namespace, ClusterDiffResult diff) {
        Future<Void> rollingUpdate = Future.future();

        if (diff.getRollingUpdate()) {
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

        if (diff.getScaleUp()) {
            statefulSetOperations.scaleUp(namespace, zk.getName(), zk.getReplicas(), scaleUp.completer());
        }
        else {
            scaleUp.complete();
        }

        return scaleUp;
    }
}
