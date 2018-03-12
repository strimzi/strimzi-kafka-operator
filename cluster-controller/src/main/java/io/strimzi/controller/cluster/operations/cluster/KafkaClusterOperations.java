/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.strimzi.controller.cluster.operations.resource.ConfigMapOperations;
import io.strimzi.controller.cluster.operations.resource.DeploymentOperations;
import io.strimzi.controller.cluster.operations.resource.EndpointOperations;
import io.strimzi.controller.cluster.operations.resource.PodOperations;
import io.strimzi.controller.cluster.operations.resource.PvcOperations;
import io.strimzi.controller.cluster.operations.resource.ServiceOperations;
import io.strimzi.controller.cluster.operations.resource.StatefulSetOperations;
import io.strimzi.controller.cluster.resources.ClusterDiffResult;
import io.strimzi.controller.cluster.resources.KafkaCluster;
import io.strimzi.controller.cluster.resources.Labels;
import io.strimzi.controller.cluster.resources.Storage;
import io.strimzi.controller.cluster.resources.TopicController;
import io.strimzi.controller.cluster.resources.ZookeeperCluster;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>Cluster operations for a "Kafka" cluster. A KafkaClusterOperations is
 * an AbstractClusterOperations that really manages two clusters,
 * for of Kafka nodes and another of ZooKeeper nodes.</p>
 */
public class KafkaClusterOperations extends AbstractClusterOperations<KafkaCluster, StatefulSet> {
    private static final Logger log = LoggerFactory.getLogger(KafkaClusterOperations.class.getName());
    private static final String CLUSTER_TYPE_ZOOKEEPER = "zookeeper";
    private static final String CLUSTER_TYPE_KAFKA = "kafka";
    private static final String CLUSTER_TYPE_TOPIC_CONTROLLER = "topic-controller";

    private final long operationTimeoutMs;

    private final StatefulSetOperations statefulSetOperations;
    private final ServiceOperations serviceOperations;
    private final PvcOperations pvcOperations;
    private final PodOperations podOperations;
    private final EndpointOperations endpointOperations;
    private final DeploymentOperations deploymentOperations;

    /**
     * @param vertx The Vertx instance
     * @param isOpenShift Whether we're running with OpenShift
     * @param configMapOperations For operating on ConfigMaps
     * @param serviceOperations For operating on Services
     * @param statefulSetOperations For operating on StatefulSets
     * @param pvcOperations For operating on PersistentVolumeClaims
     * @param podOperations For operating on Pods
     * @param deploymentOperations For operating on Deployments
     */
    public KafkaClusterOperations(Vertx vertx, boolean isOpenShift,
                                  long operationTimeoutMs,
                                  ConfigMapOperations configMapOperations,
                                  ServiceOperations serviceOperations,
                                  StatefulSetOperations statefulSetOperations,
                                  PvcOperations pvcOperations,
                                  PodOperations podOperations,
                                  EndpointOperations endpointOperations,
                                  DeploymentOperations deploymentOperations) {
        super(vertx, isOpenShift, "Kafka", configMapOperations);
        this.operationTimeoutMs = operationTimeoutMs;
        this.statefulSetOperations = statefulSetOperations;
        this.serviceOperations = serviceOperations;
        this.pvcOperations = pvcOperations;
        this.podOperations = podOperations;
        this.endpointOperations = endpointOperations;
        this.deploymentOperations = deploymentOperations;
    }

    @Override
    public void create(String namespace, String name, Handler<AsyncResult<Void>> handler) {
        execute(namespace, name, createZk, zookeeperResult -> {
            if (zookeeperResult.failed()) {
                handler.handle(zookeeperResult);
            } else {
                execute(namespace, name, createKafka, kafkaResult -> {
                    if (kafkaResult.failed()) {
                        handler.handle(kafkaResult);
                    } else {
                        ClusterOperation<TopicController> clusterOp = createTopicController.getCluster(namespace, name);
                        if (clusterOp.cluster() != null) {
                            execute(namespace, name, createTopicController, handler);
                        } else {
                            handler.handle(kafkaResult);
                        }
                    }
                });
            }
        });
    }

    private final CompositeOperation<KafkaCluster> createKafka = new CompositeOperation<KafkaCluster>() {

        @Override
        public String operationType() {
            return OP_CREATE;
        }

        @Override
        public String clusterType() {
            return CLUSTER_TYPE_KAFKA;
        }

        @Override
        public ClusterOperation<KafkaCluster> getCluster(String namespace, String name) {
            return new ClusterOperation<>(KafkaCluster.fromConfigMap(configMapOperations.get(namespace, name)), null);
        }

        @Override
        public Future<?> composite(String namespace, ClusterOperation<KafkaCluster> clusterOp) {
            Future<Void> fut = Future.future();
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

            CompositeFuture
                .join(result)
                .compose(res -> statefulSetOperations.readiness(namespace, kafka.getName(), 1_000, operationTimeoutMs))
                .compose(res -> {
                    List<Future> waitPodResult = new ArrayList<>(kafka.getReplicas());

                    for (int i = 0; i < kafka.getReplicas(); i++) {
                        String podName = kafka.getPodName(i);
                        waitPodResult.add(podOperations.readiness(namespace, podName, 1_000, operationTimeoutMs));
                    }

                    return CompositeFuture.join(waitPodResult);
                })
                .compose(res -> {
                    List<Future> waitEndpointResult = new ArrayList<>(2);
                    waitEndpointResult.add(endpointOperations.readiness(namespace, kafka.getName(), 1_000, operationTimeoutMs));
                    waitEndpointResult.add(endpointOperations.readiness(namespace, kafka.getHeadlessName(), 1_000, operationTimeoutMs));
                    return CompositeFuture.join(waitEndpointResult);
                })
                .compose(res -> {
                    fut.complete();
                }, fut);

            return fut;
        }
    };


    private final CompositeOperation<ZookeeperCluster> createZk = new CompositeOperation<ZookeeperCluster>() {

        @Override
        public String operationType() {
            return OP_CREATE;
        }

        @Override
        public String clusterType() {
            return CLUSTER_TYPE_ZOOKEEPER;
        }

        @Override
        public ClusterOperation<ZookeeperCluster> getCluster(String namespace, String name) {
            return new ClusterOperation<>(ZookeeperCluster.fromConfigMap(configMapOperations.get(namespace, name)), null);
        }

        @Override
        public Future<?> composite(String namespace, ClusterOperation<ZookeeperCluster> clusterOp) {
            Future<Void> fut = Future.future();
            ZookeeperCluster zk = clusterOp.cluster();
            List<Future> createResult = new ArrayList<>(4);

            if (zk.isMetricsEnabled()) {
                createResult.add(configMapOperations.create(zk.generateMetricsConfigMap()));
            }

            createResult.add(serviceOperations.create(zk.generateService()));

            createResult.add(serviceOperations.create(zk.generateHeadlessService()));

            createResult.add(statefulSetOperations.create(zk.generateStatefulSet(isOpenShift)));

            CompositeFuture
                .join(createResult)
                .compose(res -> statefulSetOperations.readiness(namespace, zk.getName(), 1_000, operationTimeoutMs))
                .compose(res -> {
                    List<Future> waitPodResult = new ArrayList<>(zk.getReplicas());

                    for (int i = 0; i < zk.getReplicas(); i++) {
                        String podName = zk.getPodName(i);
                        waitPodResult.add(podOperations.readiness(namespace, podName, 1_000, operationTimeoutMs));
                    }

                    return CompositeFuture.join(waitPodResult);
                })
                .compose(res -> {
                    List<Future> waitEndpointResult = new ArrayList<>(2);
                    waitEndpointResult.add(endpointOperations.readiness(namespace, zk.getName(), 1_000, operationTimeoutMs));
                    waitEndpointResult.add(endpointOperations.readiness(namespace, zk.getHeadlessName(), 1_000, operationTimeoutMs));
                    return CompositeFuture.join(waitEndpointResult);
                })
                .compose(res -> {
                    fut.complete();
                }, fut);

            return fut;
        }
    };

    private final CompositeOperation<TopicController> createTopicController = new CompositeOperation<TopicController>() {

        @Override
        public String operationType() {
            return OP_CREATE;
        }

        @Override
        public String clusterType() {
            return CLUSTER_TYPE_TOPIC_CONTROLLER;
        }

        @Override
        public ClusterOperation<TopicController> getCluster(String namespace, String name) {
            return new ClusterOperation<>(TopicController.fromConfigMap(configMapOperations.get(namespace, name)), null);
        }

        @Override
        public Future<?> composite(String namespace, ClusterOperation<TopicController> clusterOp) {

            TopicController topicController = clusterOp.cluster();
            return deploymentOperations.create(topicController.generateDeployment());
        }
    };

    private final CompositeOperation<KafkaCluster> deleteKafka = new CompositeOperation<KafkaCluster>() {
        @Override
        public String operationType() {
            return OP_DELETE;
        }

        @Override
        public String clusterType() {
            return CLUSTER_TYPE_KAFKA;
        }

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
                    result.add(pvcOperations.delete(namespace, kafka.getPersistentVolumeClaimName(i)));
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
        public String operationType() {
            return OP_DELETE;
        }

        @Override
        public String clusterType() {
            return CLUSTER_TYPE_ZOOKEEPER;
        }

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
                    result.add(pvcOperations.delete(namespace, zk.getPersistentVolumeClaimName(i)));
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

    private final CompositeOperation<TopicController> deleteTopicController = new CompositeOperation<TopicController>() {

        @Override
        public String operationType() {
            return OP_DELETE;
        }

        @Override
        public String clusterType() {
            return CLUSTER_TYPE_TOPIC_CONTROLLER;
        }

        @Override
        public Future<?> composite(String namespace, ClusterOperation<TopicController> clusterOp) {
            TopicController topicController = clusterOp.cluster();
            return deploymentOperations.delete(namespace, topicController.getName());
        }

        @Override
        public ClusterOperation<TopicController> getCluster(String namespace, String name) {
            Deployment dep = deploymentOperations.get(namespace, TopicController.topicControllerName(name));
            return new ClusterOperation<>(TopicController.fromDeployment(namespace, name, dep), null);
        }
    };

    @Override
    protected void delete(String namespace, String name, Handler<AsyncResult<Void>> handler) {

        // first check if the topic controller was really deployed
        ClusterOperation<TopicController> clusterOp = deleteTopicController.getCluster(namespace, name);
        if (clusterOp.cluster() != null) {
            execute(namespace, name, deleteTopicController, topicControllerResult -> {
                if (topicControllerResult.failed()) {
                    handler.handle(topicControllerResult);
                } else {
                    deleteKafkaAndZookeeper(namespace, name, handler);
                }
            });
        } else {
            deleteKafkaAndZookeeper(namespace, name, handler);
        }
    }

    private void deleteKafkaAndZookeeper(String namespace, String name, Handler<AsyncResult<Void>> handler) {

        execute(namespace, name, deleteKafka, kafkaResult -> {
            if (kafkaResult.failed()) {
                handler.handle(kafkaResult);
            } else {
                execute(namespace, name, deleteZk, handler);
            }
        });
    }

    private final CompositeOperation<KafkaCluster> updateKafka = new CompositeOperation<KafkaCluster>() {
        @Override
        public String operationType() {
            return OP_UPDATE;
        }

        @Override
        public String clusterType() {
            return CLUSTER_TYPE_KAFKA;
        }

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
            if (diff.isScaleDown()) {
                log.info("Scaling down stateful set {} in namespace {}", kafka.getName(), namespace);
                return statefulSetOperations.scaleDown(namespace, kafka.getName(), kafka.getReplicas());
            } else {
                return Future.succeededFuture();
            }
        }

        private Future<Void> patchService(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
            if (diff.isDifferent()) {
                return serviceOperations.patch(namespace, kafka.getName(), kafka.patchService(serviceOperations.get(namespace, kafka.getName())));
            } else {
                return Future.succeededFuture();
            }
        }

        private Future<Void> patchHeadlessService(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
            if (diff.isDifferent()) {
                return serviceOperations.patch(namespace, kafka.getHeadlessName(),
                        kafka.patchHeadlessService(serviceOperations.get(namespace, kafka.getHeadlessName())));
            } else {
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
            } else {
                rollingUpdate.complete();
            }

            return rollingUpdate;
        }

        private Future<Void> scaleUp(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
            if (diff.isScaleUp()) {
                return statefulSetOperations.scaleUp(namespace, kafka.getName(), kafka.getReplicas());
            } else {
                return Future.succeededFuture();
            }
        }
    };

    private final CompositeOperation<ZookeeperCluster> updateZk = new CompositeOperation<ZookeeperCluster>() {
        @Override
        public String operationType() {
            return OP_UPDATE;
        }

        @Override
        public String clusterType() {
            return CLUSTER_TYPE_ZOOKEEPER;
        }
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
            if (diff.isScaleDown()) {
                log.info("Scaling down stateful set {} in namespace {}", zk.getName(), namespace);
                return statefulSetOperations.scaleDown(namespace, zk.getName(), zk.getReplicas());
            } else {
                return Future.succeededFuture();
            }
        }

        private Future<Void> patchService(ZookeeperCluster zk, String namespace, ClusterDiffResult diff) {
            if (diff.isDifferent()) {
                return serviceOperations.patch(namespace, zk.getName(),
                        zk.patchService(serviceOperations.get(namespace, zk.getName())));
            } else {
                return Future.succeededFuture();
            }
        }

        private Future<Void> patchHeadlessService(ZookeeperCluster zk, String namespace, ClusterDiffResult diff) {
            if (diff.isDifferent()) {
                return serviceOperations.patch(namespace, zk.getHeadlessName(),
                        zk.patchHeadlessService(serviceOperations.get(namespace, zk.getHeadlessName())));
            } else {
                return Future.succeededFuture();
            }
        }

        private Future<Void> patchStatefulSet(ZookeeperCluster zk, String namespace, ClusterDiffResult diff) {
            if (diff.isDifferent()) {
                return statefulSetOperations.patch(namespace, zk.getName(), false,
                        zk.patchStatefulSet(statefulSetOperations.get(namespace, zk.getName())));
            } else {
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
            } else {
                rollingUpdate.complete();
            }

            return rollingUpdate;
        }

        private Future<Void> scaleUp(ZookeeperCluster zk, String namespace, ClusterDiffResult diff) {
            if (diff.isScaleUp()) {
                return statefulSetOperations.scaleUp(namespace, zk.getName(), zk.getReplicas());
            } else {
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

    private final CompositeOperation<TopicController> updateTopicController = new CompositeOperation<TopicController>() {
        @Override
        public String operationType() {
            return OP_UPDATE;
        }

        @Override
        public String clusterType() {
            return CLUSTER_TYPE_TOPIC_CONTROLLER;
        }

        @Override
        public Future<?> composite(String namespace, ClusterOperation<TopicController> operation) {
            TopicController topicController = operation.cluster();
            ClusterDiffResult diff = operation.diff();

            Future<Void> fut;
            if (diff != null) {
                fut = patchDeployment(topicController, namespace, diff);
            } else {
                // that's the case we are moving from no topic controller deployed to an updated cluster ConfigMap
                // which contains topic controller configuration for deploying it (so there is no Deployment to patch
                // but to create)
                fut = deploymentOperations.create(topicController.generateDeployment());
            }

            return fut;
        }

        @Override
        public ClusterOperation<TopicController> getCluster(String namespace, String name) {
            ClusterDiffResult diff = null;
            TopicController topicController = null;
            ConfigMap tcConfigMap = configMapOperations.get(namespace, name);

            if (tcConfigMap != null) {
                topicController = TopicController.fromConfigMap(tcConfigMap);
                if (topicController != null) {
                    log.info("Updating Topic Controller {} in namespace {}", topicController.getName(), namespace);
                    Deployment dep = deploymentOperations.get(namespace, topicController.getName());
                    diff = topicController.diff(dep);
                }
            } else {
                throw new IllegalStateException("ConfigMap " + name + " doesn't exist anymore in namespace " + namespace);
            }

            return new ClusterOperation<>(topicController, diff);
        }

        private Future<Void> patchDeployment(TopicController topicController, String namespace, ClusterDiffResult diff) {
            if (diff.isDifferent()) {
                return deploymentOperations.patch(namespace, topicController.getName(),
                        topicController.patchDeployment(deploymentOperations.get(namespace, topicController.getName())));
            } else {
                return Future.succeededFuture();
            }
        }
    };

    @Override
    public void update(String namespace, String name, Handler<AsyncResult<Void>> handler) {
        execute(namespace, name, updateZk, zookeeperResult -> {
            if (zookeeperResult.failed()) {
                handler.handle(zookeeperResult);
            } else {
                execute(namespace, name, updateKafka, kafkaResult -> {
                    if (kafkaResult.failed()) {
                        handler.handle(kafkaResult);
                    } else {
                        ClusterOperation<TopicController> clusterOp = updateTopicController.getCluster(namespace, name);
                        if (clusterOp.cluster() != null) {
                            execute(namespace, name, updateTopicController, handler);
                        } else {
                            handler.handle(kafkaResult);
                        }
                    }
                });
            }
        });
    }

    @Override
    public String clusterType() {
        return CLUSTER_TYPE_KAFKA;
    }

    @Override
    protected List<StatefulSet> getResources(String namespace, Labels selector) {
        return statefulSetOperations.list(namespace, selector);
    }


}
