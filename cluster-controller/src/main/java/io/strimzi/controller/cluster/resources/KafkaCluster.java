/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.resources;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.strimzi.controller.cluster.ClusterController;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaCluster extends AbstractCluster {

    public static final String TYPE = "kafka";

    protected static final int CLIENT_PORT = 9092;
    protected static final String CLIENT_PORT_NAME = "clients";

    protected static final int REPLICATION_PORT = 9091;
    protected static final String REPLICATION_PORT_NAME = "replication";

    private static final String NAME_SUFFIX = "-kafka";
    private static final String HEADLESS_NAME_SUFFIX = NAME_SUFFIX + "-headless";
    private static final String METRICS_CONFIG_SUFFIX = NAME_SUFFIX + "-metrics-config";

    // Kafka configuration
    private String zookeeperConnect = DEFAULT_KAFKA_ZOOKEEPER_CONNECT;
    private int defaultReplicationFactor = DEFAULT_KAFKA_DEFAULT_REPLICATION_FACTOR;
    private int offsetsTopicReplicationFactor = DEFAULT_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR;
    private int transactionStateLogReplicationFactor = DEFAULT_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR;

    // Configuration defaults
    private static final String DEFAULT_IMAGE = "strimzi/kafka:latest";
    private static final int DEFAULT_REPLICAS = 3;
    private static final int DEFAULT_HEALTHCHECK_DELAY = 15;
    private static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;
    private static final boolean DEFAULT_KAFKA_METRICS_ENABLED = false;

    // Kafka configuration defaults
    private static final String DEFAULT_KAFKA_ZOOKEEPER_CONNECT = "zookeeper:2181";
    private static final int DEFAULT_KAFKA_DEFAULT_REPLICATION_FACTOR = 3;
    private static final int DEFAULT_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR = 3;
    private static final int DEFAULT_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR = 3;

    // Configuration keys
    public static final String KEY_IMAGE = "kafka-image";
    public static final String KEY_REPLICAS = "kafka-nodes";
    public static final String KEY_HEALTHCHECK_DELAY = "kafka-healthcheck-delay";
    public static final String KEY_HEALTHCHECK_TIMEOUT = "kafka-healthcheck-timeout";
    public static final String KEY_METRICS_CONFIG = "kafka-metrics-config";
    public static final String KEY_STORAGE = "kafka-storage";

    // Kafka configuration keys
    private static final String KEY_KAFKA_ZOOKEEPER_CONNECT = "KAFKA_ZOOKEEPER_CONNECT";
    private static final String KEY_KAFKA_DEFAULT_REPLICATION_FACTOR = "KAFKA_DEFAULT_REPLICATION_FACTOR";
    private static final String KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR = "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR";
    private static final String KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR = "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR";
    private static final String KEY_KAFKA_METRICS_ENABLED = "KAFKA_METRICS_ENABLED";

    /**
     * Constructor
     *
     * @param namespace Kubernetes/OpenShift namespace where Kafka cluster resources are going to be created
     * @param cluster  overall cluster name
     */
    private KafkaCluster(String namespace, String cluster, Labels labels) {

        super(namespace, cluster, labels);
        this.name = kafkaClusterName(cluster);
        this.headlessName = headlessName(cluster);
        this.metricsConfigName = metricConfigsName(cluster);
        this.image = DEFAULT_IMAGE;
        this.replicas = DEFAULT_REPLICAS;
        this.healthCheckPath = "/opt/kafka/kafka_healthcheck.sh";
        this.healthCheckTimeout = DEFAULT_HEALTHCHECK_TIMEOUT;
        this.healthCheckInitialDelay = DEFAULT_HEALTHCHECK_DELAY;
        this.isMetricsEnabled = DEFAULT_KAFKA_METRICS_ENABLED;

        this.mountPath = "/var/lib/kafka";
        this.metricsConfigVolumeName = "kafka-metrics-config";
        this.metricsConfigMountPath = "/opt/prometheus/config/";
    }

    public static String kafkaClusterName(String cluster) {
        return cluster + KafkaCluster.NAME_SUFFIX;
    }

    public static String metricConfigsName(String cluster) {
        return cluster + KafkaCluster.METRICS_CONFIG_SUFFIX;
    }

    public static String headlessName(String cluster) {
        return cluster + KafkaCluster.HEADLESS_NAME_SUFFIX;
    }

    /**
     * Create a Kafka cluster from the related ConfigMap resource
     *
     * @param kafkaClusterCm ConfigMap with cluster configuration
     * @return Kafka cluster instance
     */
    public static KafkaCluster fromConfigMap(ConfigMap kafkaClusterCm) {

        KafkaCluster kafka = new KafkaCluster(kafkaClusterCm.getMetadata().getNamespace(),
                kafkaClusterCm.getMetadata().getName(),
                Labels.fromResource(kafkaClusterCm));

        kafka.setReplicas(Integer.parseInt(kafkaClusterCm.getData().getOrDefault(KEY_REPLICAS, String.valueOf(DEFAULT_REPLICAS))));
        kafka.setImage(kafkaClusterCm.getData().getOrDefault(KEY_IMAGE, DEFAULT_IMAGE));
        kafka.setHealthCheckInitialDelay(Integer.parseInt(kafkaClusterCm.getData().getOrDefault(KEY_HEALTHCHECK_DELAY, String.valueOf(DEFAULT_HEALTHCHECK_DELAY))));
        kafka.setHealthCheckTimeout(Integer.parseInt(kafkaClusterCm.getData().getOrDefault(KEY_HEALTHCHECK_TIMEOUT, String.valueOf(DEFAULT_HEALTHCHECK_TIMEOUT))));

        kafka.setZookeeperConnect(kafkaClusterCm.getData().getOrDefault(KEY_KAFKA_ZOOKEEPER_CONNECT, kafkaClusterCm.getMetadata().getName() + "-zookeeper:2181"));
        kafka.setDefaultReplicationFactor(Integer.parseInt(kafkaClusterCm.getData().getOrDefault(KEY_KAFKA_DEFAULT_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_DEFAULT_REPLICATION_FACTOR))));
        kafka.setOffsetsTopicReplicationFactor(Integer.parseInt(kafkaClusterCm.getData().getOrDefault(KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR))));
        kafka.setTransactionStateLogReplicationFactor(Integer.parseInt(kafkaClusterCm.getData().getOrDefault(KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR))));

        String metricsConfig = kafkaClusterCm.getData().get(KEY_METRICS_CONFIG);
        kafka.setMetricsEnabled(metricsConfig != null);
        if (kafka.isMetricsEnabled()) {
            kafka.setMetricsConfig(new JsonObject(metricsConfig));
        }

        String storageConfig = kafkaClusterCm.getData().get(KEY_STORAGE);
        kafka.setStorage(Storage.fromJson(new JsonObject(storageConfig)));

        return kafka;
    }

    /**
     * Create a Kafka cluster from the deployed StatefulSet resource
     *
     * @param ss The StatefulSet from which the cluster state should be recovered.
     * @param namespace Kubernetes/OpenShift namespace where cluster resources belong to
     * @param cluster   overall cluster name
     * @return  Kafka cluster instance
     */
    public static KafkaCluster fromStatefulSet(StatefulSet ss, String namespace, String cluster) {

        KafkaCluster kafka =  new KafkaCluster(namespace, cluster, Labels.fromResource(ss));

        kafka.setReplicas(ss.getSpec().getReplicas());
        Container container = ss.getSpec().getTemplate().getSpec().getContainers().get(0);
        kafka.setImage(container.getImage());
        kafka.setHealthCheckInitialDelay(container.getReadinessProbe().getInitialDelaySeconds());
        kafka.setHealthCheckTimeout(container.getReadinessProbe().getTimeoutSeconds());

        Map<String, String> vars = containerEnvVars(container);

        kafka.setZookeeperConnect(vars.getOrDefault(KEY_KAFKA_ZOOKEEPER_CONNECT, ss.getMetadata().getName() + "-zookeeper:2181"));
        kafka.setDefaultReplicationFactor(Integer.parseInt(vars.getOrDefault(KEY_KAFKA_DEFAULT_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_DEFAULT_REPLICATION_FACTOR))));
        kafka.setOffsetsTopicReplicationFactor(Integer.parseInt(vars.getOrDefault(KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR))));
        kafka.setTransactionStateLogReplicationFactor(Integer.parseInt(vars.getOrDefault(KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR))));

        kafka.setMetricsEnabled(Boolean.parseBoolean(vars.getOrDefault(KEY_KAFKA_METRICS_ENABLED, String.valueOf(DEFAULT_KAFKA_METRICS_ENABLED))));
        if (kafka.isMetricsEnabled()) {
            kafka.setMetricsConfigName(metricConfigsName(cluster));
        }

        if (!ss.getSpec().getVolumeClaimTemplates().isEmpty()) {

            Storage storage = Storage.fromPersistentVolumeClaim(ss.getSpec().getVolumeClaimTemplates().get(0));
            if (ss.getMetadata().getAnnotations() != null) {
                String deleteClaimAnnotation = String.format("%s/%s", ClusterController.STRIMZI_CLUSTER_CONTROLLER_DOMAIN, Storage.DELETE_CLAIM_FIELD);
                storage.withDeleteClaim(Boolean.valueOf(ss.getMetadata().getAnnotations().computeIfAbsent(deleteClaimAnnotation, s -> "false")));
            }
            kafka.setStorage(storage);
        } else {
            Storage storage = new Storage(Storage.StorageType.EPHEMERAL);
            kafka.setStorage(storage);
        }

        return kafka;
    }

    /**
     * Return the differences between the current Kafka cluster and the deployed one
     *
     * @param metricsConfigMap The CM to compare with
     * @param ss The SS to compare with
     * @return  ClusterDiffResult instance with differences
     */
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

    private List<ServicePort> getServicePorts() {
        List<ServicePort> ports = new ArrayList<>(2);
        ports.add(createServicePort(CLIENT_PORT_NAME, CLIENT_PORT, CLIENT_PORT, "TCP"));
        ports.add(createServicePort(REPLICATION_PORT_NAME, REPLICATION_PORT, REPLICATION_PORT, "TCP"));
        return ports;
    }

    /**
     * Generates a Service according to configured defaults
     * @return The generated Service
     */
    public Service generateService() {

        return createService("ClusterIP", getServicePorts());
    }

    /**
     * Generates a headless Service according to configured defaults
     * @return The generated Service
     */
    public Service generateHeadlessService() {
        Map<String, String> annotations = Collections.singletonMap("service.alpha.kubernetes.io/tolerate-unready-endpoints", "true");
        return createHeadlessService(headlessName, getServicePorts(), annotations);
    }

    /**
     * Patches the given headless Service
     * @param svc The service to patch
     * @return The same service
     */
    public Service patchHeadlessService(Service svc) {

        return patchHeadlessService(headlessName, svc);
    }

    /**
     * Generates a StatefulSet according to configured defaults
     * @param isOpenShift True iff this controller is operating within OpenShift.
     * @return The generate StatefulSet
     */
    public StatefulSet generateStatefulSet(boolean isOpenShift) {

        return createStatefulSet(
                getContainerPortList(),
                getVolumes(),
                getVolumeClaims(),
                getVolumeMounts(),
                createExecProbe(healthCheckPath, healthCheckInitialDelay, healthCheckTimeout),
                createExecProbe(healthCheckPath, healthCheckInitialDelay, healthCheckTimeout),
                isOpenShift);
    }


    /**
     * Generates a metrics ConfigMap according to configured defaults
     * @return The generated ConfigMap
     */
    public ConfigMap generateMetricsConfigMap() {

        Map<String, String> data = new HashMap<>();
        data.put(METRICS_CONFIG_FILE, metricsConfig.toString());

        return createConfigMap(metricsConfigName, data);
    }

    /**
     * Patches the given headless metrics ConfigMap
     * @param cm The metrics ConfigMap to patch
     * @return The same ConfigMap
     */
    public ConfigMap patchMetricsConfigMap(ConfigMap cm) {

        Map<String, String> data = new HashMap<>();
        data.put(METRICS_CONFIG_FILE, metricsConfig != null ? metricsConfig.toString() : null);

        return patchConfigMap(cm, data);
    }

    /**
     * Patches the given StatefulSet
     * @param statefulSet The StatefulSet to patch
     * @return The patched StatefulSet
     */
    public StatefulSet patchStatefulSet(StatefulSet statefulSet) {

        Map<String, String> annotations = new HashMap<>();
        annotations.put(String.format("%s/%s", ClusterController.STRIMZI_CLUSTER_CONTROLLER_DOMAIN, Storage.DELETE_CLAIM_FIELD),
                String.valueOf(storage.isDeleteClaim()));

        return patchStatefulSet(statefulSet,
                createExecProbe(healthCheckPath, healthCheckInitialDelay, healthCheckTimeout),
                createExecProbe(healthCheckPath, healthCheckInitialDelay, healthCheckTimeout),
                annotations);
    }

    private List<ContainerPort> getContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>(3);
        portList.add(createContainerPort(CLIENT_PORT_NAME, CLIENT_PORT, "TCP"));
        portList.add(createContainerPort(REPLICATION_PORT_NAME, REPLICATION_PORT, "TCP"));
        if (isMetricsEnabled) {
            portList.add(createContainerPort(metricsPortName, metricsPort, "TCP"));
        }

        return portList;
    }

    private List<Volume> getVolumes() {
        List<Volume> volumeList = new ArrayList<>();
        if (storage.type() == Storage.StorageType.EPHEMERAL) {
            volumeList.add(createEmptyDirVolume(volumeName));
        }
        if (isMetricsEnabled) {
            volumeList.add(createConfigMapVolume(metricsConfigVolumeName, metricsConfigName));
        }

        return volumeList;
    }

    private List<PersistentVolumeClaim> getVolumeClaims() {
        List<PersistentVolumeClaim> pvcList = new ArrayList<>();
        if (storage.type() == Storage.StorageType.PERSISTENT_CLAIM) {
            pvcList.add(createPersistentVolumeClaim(volumeName));
        }
        return pvcList;
    }

    private List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> volumeMountList = new ArrayList<>();
        volumeMountList.add(createVolumeMount(volumeName, mountPath));
        if (isMetricsEnabled) {
            volumeMountList.add(createVolumeMount(metricsConfigVolumeName, metricsConfigMountPath));
        }

        return volumeMountList;
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(buildEnvVar(KEY_KAFKA_ZOOKEEPER_CONNECT, zookeeperConnect));
        varList.add(buildEnvVar(KEY_KAFKA_DEFAULT_REPLICATION_FACTOR, String.valueOf(defaultReplicationFactor)));
        varList.add(buildEnvVar(KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, String.valueOf(offsetsTopicReplicationFactor)));
        varList.add(buildEnvVar(KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR, String.valueOf(transactionStateLogReplicationFactor)));
        varList.add(buildEnvVar(KEY_KAFKA_METRICS_ENABLED, String.valueOf(isMetricsEnabled)));

        return varList;
    }

    protected void setZookeeperConnect(String zookeeperConnect) {
        this.zookeeperConnect = zookeeperConnect;
    }

    protected void setDefaultReplicationFactor(int defaultReplicationFactor) {
        this.defaultReplicationFactor = defaultReplicationFactor;
    }

    protected void setOffsetsTopicReplicationFactor(int offsetsTopicReplicationFactor) {
        this.offsetsTopicReplicationFactor = offsetsTopicReplicationFactor;
    }

    protected void setTransactionStateLogReplicationFactor(int transactionStateLogReplicationFactor) {
        this.transactionStateLogReplicationFactor = transactionStateLogReplicationFactor;
    }
}