/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.strimzi.controller.cluster.operations.resource.ConfigMapOperations;
import io.strimzi.controller.cluster.operations.resource.DeploymentOperations;
import io.strimzi.controller.cluster.operations.resource.PvcOperations;
import io.strimzi.controller.cluster.operations.resource.ServiceOperations;
import io.strimzi.controller.cluster.operations.resource.StatefulSetOperations;
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
    private final DeploymentOperations deploymentOperations;

    /**
     * @param vertx The Vertx instance
     * @param isOpenShift Whether we're running with OpenShift
     * @param configMapOperations For operating on ConfigMaps
     * @param serviceOperations For operating on Services
     * @param statefulSetOperations For operating on StatefulSets
     * @param pvcOperations For operating on PersistentVolumeClaims
     * @param deploymentOperations For operating on Deployments
     */
    public KafkaClusterOperations(Vertx vertx, boolean isOpenShift,
                                  long operationTimeoutMs,
                                  ConfigMapOperations configMapOperations,
                                  ServiceOperations serviceOperations,
                                  StatefulSetOperations statefulSetOperations,
                                  PvcOperations pvcOperations,
                                  DeploymentOperations deploymentOperations) {
        super(vertx, isOpenShift, "Kafka", configMapOperations);
        this.operationTimeoutMs = operationTimeoutMs;
        this.statefulSetOperations = statefulSetOperations;
        this.serviceOperations = serviceOperations;
        this.pvcOperations = pvcOperations;
        this.deploymentOperations = deploymentOperations;
    }

    @Override
    public void create(String namespace, String name, Handler<AsyncResult<Void>> handler) {
        execute(namespace, name, updateZk, zookeeperResult -> {
            if (zookeeperResult.failed()) {
                handler.handle(zookeeperResult);
            } else {
                execute(namespace, name, updateKafka, kafkaResult -> {
                    if (kafkaResult.failed()) {
                        handler.handle(kafkaResult);
                    } else {
                        TopicController clusterOp = createTopicController.getCluster(namespace, name);
                        if (clusterOp != null) {
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
        public KafkaCluster getCluster(String namespace, String name) {
            return KafkaCluster.fromConfigMap(configMapOperations.get(namespace, name));
        }

        @Override
        public Future<?> composite(String namespace, KafkaCluster kafka) {
            Future<Void> fut = Future.future();
            List<Future> result = new ArrayList<>(4);
            // start creating configMap operation only if metrics are enabled,
            // otherwise the future is already complete (for the "join")
            ConfigMap metricsConfigMap = kafka.generateMetricsConfigMap();
            Service service = kafka.generateService();
            Service headlessService = kafka.generateHeadlessService();
            StatefulSet statefulSet = kafka.generateStatefulSet(isOpenShift);

            result.add(configMapOperations.reconcile(namespace, kafka.getMetricsConfigName(), metricsConfigMap));
            result.add(serviceOperations.reconcile(namespace, kafka.getName(), service));
            result.add(serviceOperations.reconcile(namespace, kafka.getHeadlessName(), headlessService));
            result.add(statefulSetOperations.reconcile(namespace, kafka.getName(), statefulSet));

            CompositeFuture
                .join(result)
                .compose(i -> serviceOperations.endpointReadiness(namespace, service, 1_000, operationTimeoutMs))
                .compose(i -> serviceOperations.endpointReadiness(namespace, headlessService, 1_000, operationTimeoutMs))
                .compose(res -> {
                    fut.complete();
                }, fut);

            return fut;
        }
    };

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
        public KafkaCluster getCluster(String namespace, String name) {
            KafkaCluster kafka;
            ConfigMap kafkaConfigMap = configMapOperations.get(namespace, name);

            if (kafkaConfigMap != null)    {
                kafka = KafkaCluster.fromConfigMap(kafkaConfigMap);
                log.info("Updating Kafka cluster {} in namespace {}", kafka.getName(), namespace);
            } else {
                throw new IllegalStateException("ConfigMap " + name + " doesn't exist anymore in namespace " + namespace);
            }
            return kafka;
        }

        @Override
        public Future<?> composite(String namespace, KafkaCluster kafka) {

            Service service = kafka.generateService();
            Service headlessService = kafka.generateHeadlessService();
            ConfigMap metricsConfigMap = kafka.generateMetricsConfigMap();
            StatefulSet statefulSet = kafka.generateStatefulSet(isOpenShift);

            Future<Void> chainFuture = Future.future();
            statefulSetOperations.scaleDown(namespace, kafka.getName(), kafka.getReplicas())
                    .compose(i -> serviceOperations.reconcile(namespace, kafka.getName(), service))
                    .compose(i -> serviceOperations.reconcile(namespace, kafka.getHeadlessName(), headlessService))
                    .compose(i -> configMapOperations.reconcile(namespace, kafka.getMetricsConfigName(), metricsConfigMap))
                    .compose(i -> statefulSetOperations.reconcile(namespace, kafka.getName(), statefulSet))
                    .compose(i -> statefulSetOperations.scaleUp(namespace, kafka.getName(), kafka.getReplicas()))
                    .compose(i -> serviceOperations.endpointReadiness(namespace, service, 1_000, operationTimeoutMs))
                    .compose(i -> serviceOperations.endpointReadiness(namespace, headlessService, 1_000, operationTimeoutMs))
                    .compose(chainFuture::complete, chainFuture);

            return chainFuture;
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
        public KafkaCluster getCluster(String namespace, String name) {
            StatefulSet ss = statefulSetOperations.get(namespace, KafkaCluster.kafkaClusterName(name));
            return KafkaCluster.fromStatefulSet(ss, namespace, name);
        }

        @Override
        public Future<?> composite(String namespace, KafkaCluster kafka) {
            boolean deleteClaims = kafka.getStorage().type() == Storage.StorageType.PERSISTENT_CLAIM
                    && kafka.getStorage().isDeleteClaim();
            List<Future> result = new ArrayList<>(4 + (deleteClaims ? kafka.getReplicas() : 0));

            result.add(configMapOperations.reconcile(namespace, kafka.getMetricsConfigName(), null));
            result.add(serviceOperations.reconcile(namespace, kafka.getName(), null));
            result.add(serviceOperations.reconcile(namespace, kafka.getHeadlessName(), null));
            result.add(statefulSetOperations.reconcile(namespace, kafka.getName(), null));

            if (deleteClaims) {
                for (int i = 0; i < kafka.getReplicas(); i++) {
                    result.add(pvcOperations.reconcile(namespace, kafka.getPersistentVolumeClaimName(i), null));
                }
            }

            // TODO wait for endpoints and pods to disappear

            return CompositeFuture.join(result);
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
        public ZookeeperCluster getCluster(String namespace, String name) {
            return ZookeeperCluster.fromConfigMap(configMapOperations.get(namespace, name));
        }

        @Override
        public Future<?> composite(String namespace, ZookeeperCluster zk) {
            Future<Void> fut = Future.future();
            List<Future> createResult = new ArrayList<>(4);

            ConfigMap metricsConfigMap = zk.generateMetricsConfigMap();
            Service service = zk.generateService();
            Service headlessService = zk.generateHeadlessService();
            StatefulSet statefulSet = zk.generateStatefulSet(isOpenShift);

            createResult.add(configMapOperations.reconcile(namespace, zk.getMetricsConfigName(), metricsConfigMap));
            createResult.add(serviceOperations.reconcile(namespace, zk.getName(), service));
            createResult.add(serviceOperations.reconcile(namespace, zk.getHeadlessName(), headlessService));
            createResult.add(statefulSetOperations.reconcile(namespace, zk.getName(), statefulSet));

            CompositeFuture
                .join(createResult)
                .compose(res -> {
                    List<Future> waitEndpointResult = new ArrayList<>(2);
                    waitEndpointResult.add(serviceOperations.endpointReadiness(namespace, service, 1_000, operationTimeoutMs));
                    waitEndpointResult.add(serviceOperations.endpointReadiness(namespace, headlessService, 1_000, operationTimeoutMs));
                    return CompositeFuture.join(waitEndpointResult);
                })
                .compose(res -> {
                    fut.complete();
                }, fut);

            return fut;
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
        public ZookeeperCluster getCluster(String namespace, String name) {
            ZookeeperCluster zk;
            ConfigMap zkConfigMap = configMapOperations.get(namespace, name);
            if (zkConfigMap != null)    {
                zk = ZookeeperCluster.fromConfigMap(zkConfigMap);
                log.info("Updating Zookeeper cluster {} in namespace {}", zk.getName(), namespace);
            } else {
                throw new IllegalStateException("ConfigMap " + name + " doesn't exist anymore in namespace " + namespace);
            }
            return zk;
        }

        @Override
        public Future<?> composite(String namespace, ZookeeperCluster zk) {
            Service service = zk.generateService();
            Service headlessService = zk.generateHeadlessService();
            Future<Void> chainFuture = Future.future();
            statefulSetOperations.scaleDown(namespace, zk.getName(), zk.getReplicas())
                    .compose(i -> serviceOperations.reconcile(namespace, zk.getName(), service))
                    .compose(i -> serviceOperations.reconcile(namespace, zk.getHeadlessName(), headlessService))
                    .compose(i -> configMapOperations.reconcile(namespace, zk.getMetricsConfigName(), zk.generateMetricsConfigMap()))
                    .compose(i -> statefulSetOperations.reconcile(namespace, zk.getName(), zk.generateStatefulSet(isOpenShift)))
                    .compose(i -> statefulSetOperations.scaleUp(namespace, zk.getName(), zk.getReplicas()))
                    .compose(i -> serviceOperations.endpointReadiness(namespace, service, 1_000, operationTimeoutMs))
                    .compose(i -> serviceOperations.endpointReadiness(namespace, headlessService, 1_000, operationTimeoutMs))
                    .compose(chainFuture::complete, chainFuture);
            return chainFuture;
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
        public ZookeeperCluster getCluster(String namespace, String name) {
            StatefulSet ss = statefulSetOperations.get(namespace, ZookeeperCluster.zookeeperClusterName(name));
            return ZookeeperCluster.fromStatefulSet(ss, namespace, name);
        }

        @Override
        public Future<?> composite(String namespace, ZookeeperCluster zk) {
            boolean deleteClaims = zk.getStorage().type() == Storage.StorageType.PERSISTENT_CLAIM
                    && zk.getStorage().isDeleteClaim();
            List<Future> result = new ArrayList<>(4 + (deleteClaims ? zk.getReplicas() : 0));

            result.add(configMapOperations.reconcile(namespace, zk.getMetricsConfigName(), null));
            result.add(serviceOperations.reconcile(namespace, zk.getName(), null));
            result.add(serviceOperations.reconcile(namespace, zk.getHeadlessName(), null));
            result.add(statefulSetOperations.reconcile(namespace, zk.getName(), null));

            if (deleteClaims) {
                for (int i = 0; i < zk.getReplicas(); i++) {
                    result.add(pvcOperations.reconcile(namespace, zk.getPersistentVolumeClaimName(i), null));
                }
            }

            // TODO wait for endpoints and pods to disappear

            return CompositeFuture.join(result);
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
        public TopicController getCluster(String namespace, String name) {
            return TopicController.fromConfigMap(configMapOperations.get(namespace, name));
        }

        @Override
        public Future<?> composite(String namespace, TopicController topicController) {
            return deploymentOperations.createOrUpdate(topicController.generateDeployment());
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
        public TopicController getCluster(String namespace, String name) {
            TopicController topicController = null;
            ConfigMap tcConfigMap = configMapOperations.get(namespace, name);

            if (tcConfigMap != null) {
                topicController = TopicController.fromConfigMap(tcConfigMap);
                if (topicController != null) {
                    log.info("Updating Topic Controller {} in namespace {}", topicController.getName(), namespace);
                }
            } else {
                throw new IllegalStateException("ConfigMap " + name + " doesn't exist anymore in namespace " + namespace);
            }

            return topicController;
        }

        @Override
        public Future<?> composite(String namespace, TopicController topicController) {
            Deployment deployment = topicController.generateDeployment();
            return deploymentOperations.reconcile(namespace, deployment.getMetadata().getName(), deployment);
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
        public TopicController getCluster(String namespace, String name) {
            Deployment dep = deploymentOperations.get(namespace, TopicController.topicControllerName(name));
            return TopicController.fromDeployment(namespace, name, dep);
        }

        @Override
        public Future<?> composite(String namespace, TopicController topicController) {
            return deploymentOperations.reconcile(namespace, topicController.getName(), null);
            // TODO wait for pod to disappear
        }

    };

    @Override
    protected void delete(String namespace, String name, Handler<AsyncResult<Void>> handler) {

        // first check if the topic controller was really deployed
        TopicController clusterOp = deleteTopicController.getCluster(namespace, name);
        if (clusterOp != null) {
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
                        TopicController clusterOp = updateTopicController.getCluster(namespace, name);
                        if (clusterOp != null) {
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
