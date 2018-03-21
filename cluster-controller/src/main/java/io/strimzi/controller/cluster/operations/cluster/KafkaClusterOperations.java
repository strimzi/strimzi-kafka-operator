/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.strimzi.controller.cluster.ClusterController;
import io.strimzi.controller.cluster.operations.resource.ConfigMapOperations;
import io.strimzi.controller.cluster.operations.resource.DeploymentOperations;
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
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiPredicate;

import static io.strimzi.controller.cluster.resources.TopicController.topicControllerName;


/*
TODO
1. We do a rolling update whether or not one is really necessary
2. We ned to cope with the dependency between labels/selectors/services
3. Locking bug
4. Tests
 */

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
                        execute(namespace, name, createTopicController, handler);
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
        public Future<?> composite(String namespace, String name) {
            KafkaCluster kafka = KafkaCluster.fromConfigMap(configMapOperations.get(namespace, name));
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
        public Future<?> composite(String namespace, String name) {
            ConfigMap kafkaConfigMap = configMapOperations.get(namespace, name);
            KafkaCluster kafka = KafkaCluster.fromConfigMap(kafkaConfigMap);
            Service service = kafka.generateService();
            Service headlessService = kafka.generateHeadlessService();
            ConfigMap metricsConfigMap = kafka.generateMetricsConfigMap();
            StatefulSet statefulSet = kafka.generateStatefulSet(isOpenShift);

            Future<Void> chainFuture = Future.future();
            statefulSetOperations.scaleDown(namespace, kafka.getName(), kafka.getReplicas())
                    .compose(scale -> serviceOperations.reconcile(namespace, kafka.getName(), service))
                    .compose(i -> serviceOperations.reconcile(namespace, kafka.getHeadlessName(), headlessService))
                    .compose(i -> configMapOperations.reconcile(namespace, kafka.getMetricsConfigName(), metricsConfigMap))
                    .compose(i -> statefulSetOperations.reconcile(namespace, kafka.getName(), statefulSet))
                    .compose(i -> statefulSetOperations.rollingUpdate(namespace, kafka.getName()))
                    .compose(i -> statefulSetOperations.scaleUp(namespace, kafka.getName(), kafka.getReplicas()))
                    .compose(scale -> serviceOperations.endpointReadiness(namespace, service, 1_000, operationTimeoutMs))
                    .compose(i -> serviceOperations.endpointReadiness(namespace, headlessService, 1_000, operationTimeoutMs))
                    .compose(chainFuture::complete, chainFuture);

            return chainFuture;
        }
    };

    private static <T extends HasMetadata> boolean differingLabels(T a, T b) {
        return (a == null) != (b == null)
            || !a.getMetadata().getLabels().equals(b.getMetadata().getLabels());
    }

    private static <T> boolean differingLists(List<T> a, List<T> b, BiPredicate<T, T> elementsDiffer) {
        if ((a == null) != (b == null)) {
            return true;
        }
        if (a == null && b == null) {
            return false;
        }
        if (a.size() != b.size()) {
            return true;
        }
        Iterator<T> ai = a.iterator();
        Iterator<T> bi = b.iterator();
        while (ai.hasNext()) {
            if (elementsDiffer.test(ai.next(), bi.next())) {
                return true;
            }
        }
        return false;
    }

    private static boolean differingImages(StatefulSet a, StatefulSet b) {
        return (a == null) != (b == null)
                || differingLists(a.getSpec().getTemplate().getSpec().getContainers(),
                b.getSpec().getTemplate().getSpec().getContainers(),
                KafkaClusterOperations::differingContainers);
    }

    private static boolean differingContainers(Container a, Container b) {
        return (a == null) != (b == null)
                || !Objects.equals(a.getImage(), b.getImage())
                || (a.getReadinessProbe() == null) != (b.getReadinessProbe() == null)
                || !Objects.equals(a.getReadinessProbe().getInitialDelaySeconds(), b.getReadinessProbe().getInitialDelaySeconds())
                || !Objects.equals(a.getReadinessProbe().getTimeoutSeconds(), b.getReadinessProbe().getTimeoutSeconds());
    }
/*
    public ClusterDiffResult diff(
            ConfigMap metricsConfigMap,
            StatefulSet ss)  {

        boolean scaleDown = false;
        boolean scaleUp = false;
        boolean different = false;
        boolean rollingUpdate = false;
        boolean metricsChanged = false;
        if (replicas > ss.getSpec().getReplicas()) {
            log.info("Diff: Expected replicas {}, actual replicas {}", replicas, ss.getSpec().getReplicas());
            scaleUp = true;
        } else if (replicas < ss.getSpec().getReplicas()) {
            log.info("Diff: Expected replicas {}, actual replicas {}", replicas, ss.getSpec().getReplicas());
            scaleDown = true;
        }

        if (!getLabelsWithName().equals(ss.getMetadata().getLabels()))    {
            log.info("Diff: Expected labels {}, actual labels {}", getLabelsWithName(), ss.getMetadata().getLabels());
            different = true;
            rollingUpdate = true;
        }

        Container container = ss.getSpec().getTemplate().getSpec().getContainers().get(0);
        if (!image.equals(container.getImage())) {
            log.info("Diff: Expected image {}, actual image {}", image, container.getImage());
            different = true;
            rollingUpdate = true;
        }

        Map<String, String> vars = containerEnvVars(container);

        if (!zookeeperConnect.equals(vars.getOrDefault(KEY_KAFKA_ZOOKEEPER_CONNECT, DEFAULT_KAFKA_ZOOKEEPER_CONNECT))
                || defaultReplicationFactor != Integer.parseInt(vars.getOrDefault(KEY_KAFKA_DEFAULT_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_DEFAULT_REPLICATION_FACTOR)))
                || offsetsTopicReplicationFactor != Integer.parseInt(vars.getOrDefault(KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR)))
                || transactionStateLogReplicationFactor != Integer.parseInt(vars.getOrDefault(KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR)))) {
            log.info("Diff: Kafka options changed");
            different = true;
            rollingUpdate = true;
        }

        if (healthCheckInitialDelay != container.getReadinessProbe().getInitialDelaySeconds()
                || healthCheckTimeout != container.getReadinessProbe().getTimeoutSeconds()) {
            log.info("Diff: Kafka healthcheck timing changed");
            different = true;
            rollingUpdate = true;
        }

        if (isMetricsEnabled != Boolean.parseBoolean(vars.getOrDefault(KEY_KAFKA_METRICS_ENABLED, String.valueOf(DEFAULT_KAFKA_METRICS_ENABLED)))) {
            log.info("Diff: Kafka metrics enabled/disabled");
            metricsChanged = true;
            rollingUpdate = true;
        } else {

            if (isMetricsEnabled) {
                JsonObject metricsConfig = new JsonObject(metricsConfigMap.getData().get(METRICS_CONFIG_FILE));
                if (!this.metricsConfig.equals(metricsConfig)) {
                    metricsChanged = true;
                }
            }
        }

        // get the current (deployed) kind of storage
        Storage ssStorage;
        if (ss.getSpec().getVolumeClaimTemplates().isEmpty()) {
            ssStorage = new Storage(Storage.StorageType.EPHEMERAL);
        } else {
            ssStorage = Storage.fromPersistentVolumeClaim(ss.getSpec().getVolumeClaimTemplates().get(0));
            // the delete-claim flack is backed by the StatefulSets
            if (ss.getMetadata().getAnnotations() != null) {
                String deleteClaimAnnotation = String.format("%s/%s", ClusterController.STRIMZI_CLUSTER_CONTROLLER_DOMAIN, Storage.DELETE_CLAIM_FIELD);
                ssStorage.withDeleteClaim(Boolean.valueOf(ss.getMetadata().getAnnotations().computeIfAbsent(deleteClaimAnnotation, s -> "false")));
            }
        }

        // compute the differences with the requested storage (from the updated ConfigMap)
        Storage.StorageDiffResult storageDiffResult = storage.diff(ssStorage);

        // check for all the not allowed changes to the storage
        boolean isStorageRejected = storageDiffResult.isType() || storageDiffResult.isSize() ||
                storageDiffResult.isStorageClass() || storageDiffResult.isSelector();

        // only delete-claim flag can be changed
        if (!isStorageRejected && (storage.type() == Storage.StorageType.PERSISTENT_CLAIM)) {
            if (storageDiffResult.isDeleteClaim()) {
                different = true;
            }
        } else if (isStorageRejected) {
            log.warn("Changing storage configuration other than delete-claim is not supported !");
        }

        return new ClusterDiffResult(different, rollingUpdate, scaleUp, scaleDown, metricsChanged);
    }
*/
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
        public Future<?> composite(String namespace, String name) {
            StatefulSet ss = statefulSetOperations.get(namespace, KafkaCluster.kafkaClusterName(name));
            KafkaCluster kafka = KafkaCluster.fromStatefulSet(ss, namespace, name);
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
        public Future<?> composite(String namespace, String name) {
            ZookeeperCluster zk = ZookeeperCluster.fromConfigMap(configMapOperations.get(namespace, name));
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
        public Future<?> composite(String namespace, String name) {
            ConfigMap zkConfigMap = configMapOperations.get(namespace, name);
            ZookeeperCluster zk = ZookeeperCluster.fromConfigMap(zkConfigMap);
            Service service = zk.generateService();
            Service headlessService = zk.generateHeadlessService();
            Future<Void> chainFuture = Future.future();
            statefulSetOperations.scaleDown(namespace, zk.getName(), zk.getReplicas())
                    .compose(i -> serviceOperations.reconcile(namespace, zk.getName(), service))
                    .compose(i -> serviceOperations.reconcile(namespace, zk.getHeadlessName(), headlessService))
                    .compose(i -> configMapOperations.reconcile(namespace, zk.getMetricsConfigName(), zk.generateMetricsConfigMap()))
                    .compose(i -> statefulSetOperations.reconcile(namespace, zk.getName(), zk.generateStatefulSet(isOpenShift)))
                    .compose(i -> statefulSetOperations.rollingUpdate(namespace, zk.getName()))
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
        public Future<?> composite(String namespace, String name) {
            StatefulSet ss = statefulSetOperations.get(namespace, ZookeeperCluster.zookeeperClusterName(name));
            ZookeeperCluster zk = ZookeeperCluster.fromStatefulSet(ss, namespace, name);
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
        public Future<?> composite(String namespace, String name) {
            TopicController topicController = TopicController.fromConfigMap(configMapOperations.get(namespace, name));
            return deploymentOperations.reconcile(namespace, topicControllerName(name), topicController == null ? null : topicController.generateDeployment());
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
        public Future<?> composite(String namespace, String name) {
            ConfigMap tcConfigMap = configMapOperations.get(namespace, name);
            TopicController topicController = TopicController.fromConfigMap(tcConfigMap);
            Deployment deployment = topicController != null ? topicController.generateDeployment() : null;
            return deploymentOperations.reconcile(namespace, topicControllerName(name), deployment);
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
        public Future<?> composite(String namespace, String name) {
            Deployment dep = deploymentOperations.get(namespace, topicControllerName(name));
            TopicController topicController = TopicController.fromDeployment(namespace, name, dep);
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
    public void update(String namespace, String name, Handler<AsyncResult<Void>> handler) {
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

    @Override
    public String clusterType() {
        return CLUSTER_TYPE_KAFKA;
    }

    @Override
    protected List<StatefulSet> getResources(String namespace, Labels selector) {
        return statefulSetOperations.list(namespace, selector);
    }


}
