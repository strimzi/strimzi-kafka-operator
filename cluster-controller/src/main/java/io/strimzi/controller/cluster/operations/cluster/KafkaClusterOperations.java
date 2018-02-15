/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.strimzi.controller.cluster.ClusterController;
import io.strimzi.controller.cluster.operations.resource.ConfigMapOperations;
import io.strimzi.controller.cluster.operations.resource.PvcOperations;
import io.strimzi.controller.cluster.operations.resource.ServiceOperations;
import io.strimzi.controller.cluster.operations.resource.StatefulSetOperations;
import io.strimzi.controller.cluster.resources.ClusterDiffResult;
import io.strimzi.controller.cluster.resources.KafkaCluster;
import io.strimzi.controller.cluster.resources.Storage;
import io.strimzi.controller.cluster.resources.ZookeeperCluster;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * CRUD-style operations on a Kafka cluster
 */
public class KafkaClusterOperations extends AbstractClusterOperations<KafkaCluster> {
    private static final Logger log = LoggerFactory.getLogger(KafkaClusterOperations.class.getName());
    private static final String CLUSTER_TYPE_ZOOKEEPER = "zookeeper";
    private static final String CLUSTER_TYPE_KAFKA = "kafka";
    private final ConfigMapOperations configMapOperations;
    private final StatefulSetOperations statefulSetOperations;
    private final ServiceOperations serviceOperations;
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
    public KafkaClusterOperations(Vertx vertx, boolean isOpenShift,
                                  ConfigMapOperations configMapOperations,
                                  ServiceOperations serviceOperations,
                                  StatefulSetOperations statefulSetOperations,
                                  PvcOperations pvcOperations) {
        super(vertx, isOpenShift, "Kafka");
        this.configMapOperations = configMapOperations;
        this.statefulSetOperations = statefulSetOperations;
        this.serviceOperations = serviceOperations;
        this.pvcOperations = pvcOperations;
    }

    public void create(String namespace, String name, Handler<AsyncResult<Void>> handler) {
        execute(CLUSTER_TYPE_ZOOKEEPER, OP_CREATE, namespace, name, createZk, ar -> {
            if (ar.failed()) {
                handler.handle(ar);
            } else {
                execute(CLUSTER_TYPE_KAFKA, OP_CREATE, namespace, name, createKafka, handler);
            }
        });

    }

    private final CompositeOperation<KafkaCluster> createKafka = new CompositeOperation<KafkaCluster>() {

        @Override
        public ClusterOperation<KafkaCluster> getCluster(String namespace, String name){
            return new ClusterOperation<>(KafkaCluster.fromConfigMap(configMapOperations.get(namespace, name)), null);
        }

        @Override
        public Future<?> composite(String namespace, ClusterOperation<KafkaCluster> clusterOp){
            KafkaCluster kafka = clusterOp.cluster();
            List<Future> result = new ArrayList<>(4);
            // start creating configMap operation only if metrics are enabled,
            // otherwise the future is already complete (for the "join")
            if (kafka.isMetricsEnabled()) {
                result.add(configMapOperations.create(kafka.generateMetricsConfigMap()));
            }

            result.add(serviceOperations.create(kafka.generateService()));

            result.add(serviceOperations.create(kafka.generateHeadlessService()));

            result.add(statefulSetOperations.create(kafka.generateStatefulSet(isOpenShift)));

            return CompositeFuture.join(result);
        }
    };


    private final CompositeOperation<ZookeeperCluster> createZk = new CompositeOperation<ZookeeperCluster>() {

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

    private final CompositeOperation<KafkaCluster> deleteKafka = new CompositeOperation<KafkaCluster>() {
        @Override
        public Future<?> composite(String namespace, ClusterOperation<KafkaCluster> clusterOp) {
            KafkaCluster kafka = clusterOp.cluster();
            boolean deleteClaims = kafka.getStorage().type() == Storage.StorageType.PERSISTENT_CLAIM
                    && kafka.getStorage().isDeleteClaim();
            List<Future> result = new ArrayList<>(4 + (deleteClaims ? kafka.getReplicas() : 0));

            if (kafka.isMetricsEnabled()) {
                result.add(configMapOperations.delete(namespace, kafka.getMetricsConfigName()));
            }

            result.add(serviceOperations.delete(namespace, kafka.getName()));

            result.add(serviceOperations.delete(namespace, kafka.getHeadlessName()));

            result.add(statefulSetOperations.delete(namespace, kafka.getName()));

            if (deleteClaims) {
                for (int i = 0; i < kafka.getReplicas(); i++) {
                    result.add(pvcOperations.delete(namespace, kafka.getVolumeName() + "-" + kafka.getName() + "-" + i));
                }
            }

            return CompositeFuture.join(result);
        }

        @Override
        public ClusterOperation<KafkaCluster> getCluster(String namespace, String name) {
            StatefulSet ss = statefulSetOperations.get(namespace, KafkaCluster.kafkaClusterName(name));
            return new ClusterOperation<>(KafkaCluster.fromStatefulSet(ss, namespace, name), null);
        }
    };


    private final CompositeOperation<ZookeeperCluster> deleteZk = new CompositeOperation<ZookeeperCluster>() {
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
    protected void delete(String namespace, String name, Handler<AsyncResult<Void>> handler) {
        execute(CLUSTER_TYPE_KAFKA, OP_DELETE, namespace, name, deleteKafka, ar -> {
            if (ar.failed()) {
                handler.handle(ar);
            } else {
                execute(CLUSTER_TYPE_ZOOKEEPER, OP_DELETE, namespace, name, deleteZk, handler);
            }
        });

    }

    private final CompositeOperation<KafkaCluster> updateKafka = new CompositeOperation<KafkaCluster>() {
        @Override
        public Future<?> composite(String namespace, ClusterOperation<KafkaCluster> clusterOp) {
            KafkaCluster kafka = clusterOp.cluster();
            ClusterDiffResult diff = clusterOp.diff();

            Future<Void> chainFuture = Future.future();
            scaleDown(kafka, namespace, diff)
                    .compose(i -> patchService(kafka, namespace, diff))
                    .compose(i -> patchHeadlessService(kafka, namespace, diff))
                    .compose(i -> patchStatefulSet(kafka, namespace, diff))
                    .compose(i -> patchMetricsConfigMap(kafka, namespace, diff))
                    .compose(i -> rollingUpdate(kafka, namespace, diff))
                    .compose(i -> scaleUp(kafka, namespace, diff))
                    .compose(chainFuture::complete, chainFuture);

            return chainFuture;
        }

        @Override
        public ClusterOperation<KafkaCluster> getCluster(String namespace, String name) {
            ClusterDiffResult diff;
            KafkaCluster kafka;
            ConfigMap kafkaConfigMap = configMapOperations.get(namespace, name);

            if (kafkaConfigMap != null)    {
                kafka = KafkaCluster.fromConfigMap(kafkaConfigMap);
                log.info("Updating Kafka cluster {} in namespace {}", kafka.getName(), namespace);
                StatefulSet ss = statefulSetOperations.get(namespace, kafka.getName());
                ConfigMap metricsConfigMap = configMapOperations.get(namespace, kafka.getMetricsConfigName());
                diff = kafka.diff(metricsConfigMap, ss);
            } else {
                throw new IllegalStateException("ConfigMap " + name + " doesn't exist anymore in namespace " + namespace);
            }
            return new ClusterOperation<>(kafka, diff);
        }


        private Future<Void> scaleDown(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
            if (diff.isScaleDown())    {
                log.info("Scaling down stateful set {} in namespace {}", kafka.getName(), namespace);
                return statefulSetOperations.scaleDown(namespace, kafka.getName(), kafka.getReplicas());
            }
            else {
                return Future.succeededFuture();
            }
        }

        private Future<Void> patchService(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
            if (diff.isDifferent()) {
                return serviceOperations.patch(namespace, kafka.getName(), kafka.patchService(serviceOperations.get(namespace, kafka.getName())));
            }
            else
            {
                return Future.succeededFuture();
            }
        }

        private Future<Void> patchHeadlessService(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
            if (diff.isDifferent()) {
                return serviceOperations.patch(namespace, kafka.getHeadlessName(),
                        kafka.patchHeadlessService(serviceOperations.get(namespace, kafka.getHeadlessName())));
            }
            else
            {
                return Future.succeededFuture();
            }
        }

        private Future<Void> patchStatefulSet(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
            if (diff.isDifferent()) {
                return statefulSetOperations.patch(namespace, kafka.getName(), false,
                        kafka.patchStatefulSet(statefulSetOperations.get(namespace, kafka.getName())));
            } else {
                return Future.succeededFuture();
            }
        }

        private Future<Void> patchMetricsConfigMap(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
            if (diff.isMetricsChanged()) {
                return configMapOperations.patch(namespace, kafka.getMetricsConfigName(),
                        kafka.patchMetricsConfigMap(configMapOperations.get(namespace, kafka.getMetricsConfigName())));
            } else {
                return Future.succeededFuture();
            }
        }

        private Future<Void> rollingUpdate(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
            Future<Void> rollingUpdate = Future.future();

            if (diff.isRollingUpdate()) {
                statefulSetOperations.rollingUpdate(namespace, kafka.getName(),
                        rollingUpdate.completer());
            }
            else {
                rollingUpdate.complete();
            }

            return rollingUpdate;
        }

        private Future<Void> scaleUp(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
            if (diff.isScaleUp()) {
                return statefulSetOperations.scaleUp(namespace, kafka.getName(), kafka.getReplicas());
            }
            else {
                return Future.succeededFuture();
            }
        }
    };

    private final CompositeOperation<ZookeeperCluster> updateZk = new CompositeOperation<ZookeeperCluster>() {
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

        private Future<Void> scaleDown(ZookeeperCluster zk, String namespace, ClusterDiffResult diff) {
            if (diff.isScaleDown())    {
                log.info("Scaling down stateful set {} in namespace {}", zk.getName(), namespace);
                return statefulSetOperations.scaleDown(namespace, zk.getName(), zk.getReplicas());
            }
            else {
                return Future.succeededFuture();
            }
        }

        private Future<Void> patchService(ZookeeperCluster zk, String namespace, ClusterDiffResult diff) {
            if (diff.isDifferent()) {
                return serviceOperations.patch(namespace, zk.getName(),
                        zk.patchService(serviceOperations.get(namespace, zk.getName())));
            }
            else
            {
                return Future.succeededFuture();
            }
        }

        private Future<Void> patchHeadlessService(ZookeeperCluster zk, String namespace, ClusterDiffResult diff) {
            if (diff.isDifferent()) {
                return serviceOperations.patch(namespace, zk.getHeadlessName(),
                        zk.patchHeadlessService(serviceOperations.get(namespace, zk.getHeadlessName())));
            }
            else
            {
                return Future.succeededFuture();
            }
        }

        private Future<Void> patchStatefulSet(ZookeeperCluster zk, String namespace, ClusterDiffResult diff) {
            if (diff.isDifferent()) {
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
            if (diff.isScaleUp()) {
                return statefulSetOperations.scaleUp(namespace, zk.getName(), zk.getReplicas());
            }
            else {
                return Future.succeededFuture();
            }
        }

        @Override
        public ClusterOperation<ZookeeperCluster> getCluster(String namespace, String name) {
            ClusterDiffResult diff;
            ZookeeperCluster zk;
            ConfigMap zkConfigMap = configMapOperations.get(namespace, name);

            if (zkConfigMap != null)    {
                zk = ZookeeperCluster.fromConfigMap(zkConfigMap);
                log.info("Updating Zookeeper cluster {} in namespace {}", zk.getName(), namespace);
                StatefulSet ss = statefulSetOperations.get(namespace, zk.getName());
                ConfigMap metricsConfigMap = configMapOperations.get(namespace, zk.getMetricsConfigName());
                diff = zk.diff(metricsConfigMap, ss);
            } else {
                throw new IllegalStateException("ConfigMap " + name + " doesn't exist anymore in namespace " + namespace);
            }

            return new ClusterOperation<>(zk, diff);
        }
    };

    @Override
    public void update(String namespace, String name, Handler<AsyncResult<Void>> handler) {
        execute(CLUSTER_TYPE_ZOOKEEPER, OP_UPDATE, namespace, name, updateZk, ar -> {
            if (ar.failed()) {
                handler.handle(ar);
            } else {
                execute(CLUSTER_TYPE_KAFKA, OP_UPDATE, namespace, name, updateKafka, handler);
            }
        });
    }

    @Override
    public void reconcile(String namespace, Map<String, String> labels) {
        log.info("Reconciling Kafka clusters ...");

        Map<String, String> kafkaLabels = new HashMap(labels);
        kafkaLabels.put(ClusterController.STRIMZI_TYPE_LABEL, KafkaCluster.TYPE);

        List<ConfigMap> cms = configMapOperations.list(namespace, kafkaLabels);
        List<StatefulSet> sss = statefulSetOperations.list(namespace, kafkaLabels);

        Set<String> cmsNames = cms.stream().map(cm -> cm.getMetadata().getName()).collect(Collectors.toSet());
        Set<String> sssNames = sss.stream().map(cm -> cm.getMetadata().getLabels().get(ClusterController.STRIMZI_CLUSTER_LABEL)).collect(Collectors.toSet());

        List<ConfigMap> addList = cms.stream().filter(cm -> !sssNames.contains(cm.getMetadata().getName())).collect(Collectors.toList());
        List<ConfigMap> updateList = cms.stream().filter(cm -> sssNames.contains(cm.getMetadata().getName())).collect(Collectors.toList());
        List<StatefulSet> deletionList = sss.stream().filter(ss -> !cmsNames.contains(ss.getMetadata().getLabels().get(ClusterController.STRIMZI_CLUSTER_LABEL))).collect(Collectors.toList());

        add(namespace, addList);
        delete(namespace, deletionList);
        update(namespace, updateList);
    }

}
