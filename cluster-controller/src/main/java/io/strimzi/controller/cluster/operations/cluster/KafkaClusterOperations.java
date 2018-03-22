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
import io.strimzi.controller.cluster.operations.resource.ReconcileResult;
import io.strimzi.controller.cluster.operations.resource.ServiceOperations;
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

import static io.strimzi.controller.cluster.resources.TopicController.topicControllerName;

/**
 * <p>Cluster operations for a "Kafka" cluster. A KafkaClusterOperations is
 * an AbstractClusterOperations that really manages two clusters,
 * for of Kafka nodes and another of ZooKeeper nodes.</p>
 */
public class KafkaClusterOperations extends AbstractClusterOperations<KafkaCluster, StatefulSet> {
    private static final Logger log = LoggerFactory.getLogger(KafkaClusterOperations.class.getName());
    private static final String CLUSTER_TYPE_ZOOKEEPER = "zookeeper";
    static final String CLUSTER_TYPE_KAFKA = "kafka";
    private static final String CLUSTER_TYPE_TOPIC_CONTROLLER = "topic-controller";

    private final long operationTimeoutMs;

    private final ZookeeperSetOperations zkSetOperations;
    private final KafkaSetOperations kafkaSetOperations;
    private final ServiceOperations serviceOperations;
    private final PvcOperations pvcOperations;
    private final DeploymentOperations deploymentOperations;

    /**
     * @param vertx The Vertx instance
     * @param isOpenShift Whether we're running with OpenShift
     * @param configMapOperations For operating on ConfigMaps
     * @param serviceOperations For operating on Services
     * @param zkSetOperations For operating on StatefulSets
     * @param pvcOperations For operating on PersistentVolumeClaims
     * @param deploymentOperations For operating on Deployments
     */
    public KafkaClusterOperations(Vertx vertx, boolean isOpenShift,
                                  long operationTimeoutMs,
                                  ConfigMapOperations configMapOperations,
                                  ServiceOperations serviceOperations,
                                  ZookeeperSetOperations zkSetOperations,
                                  KafkaSetOperations kafkaSetOperations,
                                  PvcOperations pvcOperations,
                                  DeploymentOperations deploymentOperations) {
        super(vertx, isOpenShift, "Kafka", configMapOperations);
        this.operationTimeoutMs = operationTimeoutMs;
        this.zkSetOperations = zkSetOperations;
        this.serviceOperations = serviceOperations;
        this.pvcOperations = pvcOperations;
        this.deploymentOperations = deploymentOperations;
        this.kafkaSetOperations = kafkaSetOperations;
    }

    @Override
    public void createOrUpdate(String namespace, String name, Handler<AsyncResult<Void>> handler) {
        execute(namespace, name, updateZk, zookeeperResult -> {
            if (zookeeperResult.failed()) {
                handler.handle(zookeeperResult);
            } else {
                execute(namespace, name, updateKafka, kafkaResult -> {
                    if (kafkaResult.failed()) {
                        handler.handle(kafkaResult);
                    } else {
                        execute(namespace, name, updateTopicController, handler);
                    }
                });
            }
        });
    }

    private final CompositeOperation updateKafka = new CompositeOperation(OP_CREATE_UPDATE, CLUSTER_TYPE_KAFKA) {
        @Override
        public Future<?> composite(String namespace, String name) {
            ConfigMap kafkaConfigMap = configMapOperations.get(namespace, name);
            KafkaCluster kafka = KafkaCluster.fromConfigMap(kafkaConfigMap);
            Service service = kafka.generateService();
            Service headlessService = kafka.generateHeadlessService();
            ConfigMap metricsConfigMap = kafka.generateMetricsConfigMap();
            StatefulSet statefulSet = kafka.generateStatefulSet(isOpenShift);

            Future<Void> chainFuture = Future.future();
            kafkaSetOperations.scaleDown(namespace, kafka.getName(), kafka.getReplicas())
                    .compose(scale -> serviceOperations.reconcile(namespace, kafka.getName(), service))
                    .compose(i -> serviceOperations.reconcile(namespace, kafka.getHeadlessName(), headlessService))
                    .compose(i -> configMapOperations.reconcile(namespace, kafka.getMetricsConfigName(), metricsConfigMap))
                    .compose(i -> kafkaSetOperations.reconcile(namespace, kafka.getName(), statefulSet))
                    .compose(diffs -> {
                        if (diffs instanceof ReconcileResult.Patched
                                && ((ReconcileResult.Patched<Boolean>) diffs).differences()) {
                            return kafkaSetOperations.rollingUpdate(namespace, kafka.getName());
                        } else {
                            return Future.succeededFuture();
                        }
                    })
                    .compose(i -> kafkaSetOperations.scaleUp(namespace, kafka.getName(), kafka.getReplicas()))
                    .compose(scale -> serviceOperations.endpointReadiness(namespace, service, 1_000, operationTimeoutMs))
                    .compose(i -> serviceOperations.endpointReadiness(namespace, headlessService, 1_000, operationTimeoutMs))
                    .compose(chainFuture::complete, chainFuture);

            return chainFuture;
        }
    };

    private final CompositeOperation deleteKafka = new CompositeOperation(OP_DELETE, CLUSTER_TYPE_KAFKA) {

        @Override
        public Future<?> composite(String namespace, String name) {
            StatefulSet ss = kafkaSetOperations.get(namespace, KafkaCluster.kafkaClusterName(name));
            KafkaCluster kafka = KafkaCluster.fromStatefulSet(ss, namespace, name);
            boolean deleteClaims = kafka.getStorage().type() == Storage.StorageType.PERSISTENT_CLAIM
                    && kafka.getStorage().isDeleteClaim();
            List<Future> result = new ArrayList<>(4 + (deleteClaims ? kafka.getReplicas() : 0));

            result.add(configMapOperations.reconcile(namespace, kafka.getMetricsConfigName(), null));
            result.add(serviceOperations.reconcile(namespace, kafka.getName(), null));
            result.add(serviceOperations.reconcile(namespace, kafka.getHeadlessName(), null));
            result.add(kafkaSetOperations.reconcile(namespace, kafka.getName(), null));

            if (deleteClaims) {
                for (int i = 0; i < kafka.getReplicas(); i++) {
                    result.add(pvcOperations.reconcile(namespace, kafka.getPersistentVolumeClaimName(i), null));
                }
            }

            // TODO wait for endpoints and pods to disappear

            return CompositeFuture.join(result);
        }

    };

    private final CompositeOperation updateZk = new CompositeOperation(OP_CREATE_UPDATE, CLUSTER_TYPE_ZOOKEEPER) {
        @Override
        public Future<?> composite(String namespace, String name) {
            ConfigMap zkConfigMap = configMapOperations.get(namespace, name);
            ZookeeperCluster zk = ZookeeperCluster.fromConfigMap(zkConfigMap);
            Service service = zk.generateService();
            Service headlessService = zk.generateHeadlessService();
            Future<Void> chainFuture = Future.future();
            zkSetOperations.scaleDown(namespace, zk.getName(), zk.getReplicas())
                    .compose(scale -> serviceOperations.reconcile(namespace, zk.getName(), service))
                    .compose(i -> serviceOperations.reconcile(namespace, zk.getHeadlessName(), headlessService))
                    .compose(i -> configMapOperations.reconcile(namespace, zk.getMetricsConfigName(), zk.generateMetricsConfigMap()))
                    .compose(i -> zkSetOperations.reconcile(namespace, zk.getName(), zk.generateStatefulSet(isOpenShift)))
                    //.compose(i -> zkSetOperations.rollingUpdate(namespace, zk.getName()))
                    .compose(diffs -> {
                        if (diffs instanceof ReconcileResult.Patched
                                && ((ReconcileResult.Patched<Boolean>) diffs).differences()) {
                            return zkSetOperations.rollingUpdate(namespace, zk.getName());
                        } else {
                            return Future.succeededFuture();
                        }
                    })
                    .compose(i -> zkSetOperations.scaleUp(namespace, zk.getName(), zk.getReplicas()))
                    .compose(scale -> serviceOperations.endpointReadiness(namespace, service, 1_000, operationTimeoutMs))
                    .compose(i -> serviceOperations.endpointReadiness(namespace, headlessService, 1_000, operationTimeoutMs))
                    .compose(chainFuture::complete, chainFuture);
            return chainFuture;
        }
    };

    private final CompositeOperation deleteZk = new CompositeOperation(OP_DELETE, CLUSTER_TYPE_ZOOKEEPER) {
        @Override
        public Future<?> composite(String namespace, String name) {
            StatefulSet ss = zkSetOperations.get(namespace, ZookeeperCluster.zookeeperClusterName(name));
            ZookeeperCluster zk = ZookeeperCluster.fromStatefulSet(ss, namespace, name);
            boolean deleteClaims = zk.getStorage().type() == Storage.StorageType.PERSISTENT_CLAIM
                    && zk.getStorage().isDeleteClaim();
            List<Future> result = new ArrayList<>(4 + (deleteClaims ? zk.getReplicas() : 0));

            result.add(configMapOperations.reconcile(namespace, zk.getMetricsConfigName(), null));
            result.add(serviceOperations.reconcile(namespace, zk.getName(), null));
            result.add(serviceOperations.reconcile(namespace, zk.getHeadlessName(), null));
            result.add(zkSetOperations.reconcile(namespace, zk.getName(), null));

            if (deleteClaims) {
                for (int i = 0; i < zk.getReplicas(); i++) {
                    result.add(pvcOperations.reconcile(namespace, zk.getPersistentVolumeClaimName(i), null));
                }
            }

            // TODO wait for endpoints and pods to disappear

            return CompositeFuture.join(result);
        }

    };

    private final CompositeOperation updateTopicController = new CompositeOperation(OP_CREATE_UPDATE, CLUSTER_TYPE_TOPIC_CONTROLLER) {
        @Override
        public Future<?> composite(String namespace, String name) {
            ConfigMap tcConfigMap = configMapOperations.get(namespace, name);
            TopicController topicController = TopicController.fromConfigMap(tcConfigMap);
            Deployment deployment = topicController != null ? topicController.generateDeployment() : null;
            return deploymentOperations.reconcile(namespace, topicControllerName(name), deployment);
        }
    };

    private final CompositeOperation deleteTopicController = new CompositeOperation(OP_DELETE, CLUSTER_TYPE_TOPIC_CONTROLLER) {
        @Override
        public Future<?> composite(String namespace, String name) {
            return deploymentOperations.reconcile(namespace, topicControllerName(name), null);
            // TODO wait for pod to disappear
        }

    };

    @Override
    protected void delete(String namespace, String name, Handler<AsyncResult<Void>> handler) {

        execute(namespace, name, deleteTopicController, topicControllerResult -> {
            if (topicControllerResult.failed()) {
                handler.handle(topicControllerResult);
            } else {
                execute(namespace, name, deleteKafka, kafkaResult -> {
                    if (kafkaResult.failed()) {
                        handler.handle(kafkaResult);
                    } else {
                        execute(namespace, name, deleteZk, handler);
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
        return kafkaSetOperations.list(namespace, selector);
    }
}