package io.strimzi.controller.cluster.resources;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.strimzi.controller.cluster.ClusterController;
import io.strimzi.controller.cluster.operations.resource.ConfigMapOperations;
import io.strimzi.controller.cluster.operations.resource.StatefulSetOperations;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KafkaCluster extends AbstractCluster {

    public static final String TYPE = "kafka";

    private static final int CLIENT_PORT = 9092;
    private static final String CLIENT_PORT_NAME = "clients";

    private static String NAME_SUFFIX = "-kafka";
    private static String HEADLESS_NAME_SUFFIX = NAME_SUFFIX + "-headless";
    private static String METRICS_CONFIG_SUFFIX = NAME_SUFFIX + "-metrics-config";

    // Kafka configuration
    private String zookeeperConnect = DEFAULT_KAFKA_ZOOKEEPER_CONNECT;
    private int defaultReplicationFactor = DEFAULT_KAFKA_DEFAULT_REPLICATION_FACTOR;
    private int offsetsTopicReplicationFactor = DEFAULT_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR;
    private int transactionStateLogReplicationFactor = DEFAULT_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR;

    // Configuration defaults
    private static String DEFAULT_IMAGE = "strimzi/kafka:latest";
    private static int DEFAULT_REPLICAS = 3;
    private static int DEFAULT_HEALTHCHECK_DELAY = 15;
    private static int DEFAULT_HEALTHCHECK_TIMEOUT = 5;
    private static boolean DEFAULT_KAFKA_METRICS_ENABLED = false;

    // Kafka configuration defaults
    private static String DEFAULT_KAFKA_ZOOKEEPER_CONNECT = "zookeeper:2181";
    private static int DEFAULT_KAFKA_DEFAULT_REPLICATION_FACTOR = 3;
    private static int DEFAULT_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR = 3;
    private static int DEFAULT_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR = 3;

    // Configuration keys
    public static final String KEY_IMAGE = "kafka-image";
    public static final String KEY_REPLICAS = "kafka-nodes";
    public static final String KEY_HEALTHCHECK_DELAY = "kafka-healthcheck-delay";
    public static final String KEY_HEALTHCHECK_TIMEOUT = "kafka-healthcheck-timeout";
    public static final String KEY_METRICS_CONFIG = "kafka-metrics-config";
    public static final String KEY_STORAGE = "kafka-storage";

    // Kafka configuration keys
    private static String KEY_KAFKA_ZOOKEEPER_CONNECT = "KAFKA_ZOOKEEPER_CONNECT";
    private static String KEY_KAFKA_DEFAULT_REPLICATION_FACTOR = "KAFKA_DEFAULT_REPLICATION_FACTOR";
    private static String KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR = "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR";
    private static String KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR = "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR";
    private static String KEY_KAFKA_METRICS_ENABLED = "KAFKA_METRICS_ENABLED";

    /**
     * Constructor
     *
     * @param namespace Kubernetes/OpenShift namespace where Kafka cluster resources are going to be created
     * @param cluster  overall cluster name
     */
    private KafkaCluster(String namespace, String cluster) {

        super(namespace, cluster);
        this.name = cluster + KafkaCluster.NAME_SUFFIX;
        this.headlessName = cluster + KafkaCluster.HEADLESS_NAME_SUFFIX;
        this.metricsConfigName = cluster + KafkaCluster.METRICS_CONFIG_SUFFIX;
        this.image = DEFAULT_IMAGE;
        this.replicas = DEFAULT_REPLICAS;
        this.healthCheckPath = "/opt/kafka/kafka_healthcheck.sh";
        this.healthCheckTimeout = DEFAULT_HEALTHCHECK_TIMEOUT;
        this.healthCheckInitialDelay = DEFAULT_HEALTHCHECK_DELAY;
        this.isMetricsEnabled = DEFAULT_KAFKA_METRICS_ENABLED;

        this.mounthPath = "/var/lib/kafka";
        this.volumeName = "kafka-storage";
        this.metricsConfigVolumeName = "kafka-metrics-config";
        this.metricsConfigMountPath = "/opt/prometheus/config/";
    }

    /**
     * Create a Kafka cluster from the related ConfigMap resource
     *
     * @param cm    ConfigMap with cluster configuration
     * @return   Kafka cluster instance
     */
    public static KafkaCluster fromConfigMap(ConfigMap cm) {

        KafkaCluster kafka = new KafkaCluster(cm.getMetadata().getNamespace(), cm.getMetadata().getName());
        kafka.setLabels(cm.getMetadata().getLabels());

        kafka.setReplicas(Integer.parseInt(cm.getData().getOrDefault(KEY_REPLICAS, String.valueOf(DEFAULT_REPLICAS))));
        kafka.setImage(cm.getData().getOrDefault(KEY_IMAGE, DEFAULT_IMAGE));
        kafka.setHealthCheckInitialDelay(Integer.parseInt(cm.getData().getOrDefault(KEY_HEALTHCHECK_DELAY, String.valueOf(DEFAULT_HEALTHCHECK_DELAY))));
        kafka.setHealthCheckTimeout(Integer.parseInt(cm.getData().getOrDefault(KEY_HEALTHCHECK_TIMEOUT, String.valueOf(DEFAULT_HEALTHCHECK_TIMEOUT))));

        kafka.setZookeeperConnect(cm.getData().getOrDefault(KEY_KAFKA_ZOOKEEPER_CONNECT, cm.getMetadata().getName() + "-zookeeper:2181"));
        kafka.setDefaultReplicationFactor(Integer.parseInt(cm.getData().getOrDefault(KEY_KAFKA_DEFAULT_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_DEFAULT_REPLICATION_FACTOR))));
        kafka.setOffsetsTopicReplicationFactor(Integer.parseInt(cm.getData().getOrDefault(KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR))));
        kafka.setTransactionStateLogReplicationFactor(Integer.parseInt(cm.getData().getOrDefault(KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR))));

        String metricsConfig = cm.getData().get(KEY_METRICS_CONFIG);
        kafka.setMetricsEnabled(metricsConfig != null);
        if (kafka.isMetricsEnabled()) {
            kafka.setMetricsConfig(new JsonObject(metricsConfig));
        }

        String storageConfig = cm.getData().get(KEY_STORAGE);
        kafka.setStorage(Storage.fromJson(new JsonObject(storageConfig)));

        return kafka;
    }

    /**
     * Create a Kafka cluster from the deployed StatefulSet resource
     *
     * @param statefulSetOperations The means of setting the SS to obtain the state from
     * @param namespace Kubernetes/OpenShift namespace where cluster resources belong to
     * @param cluster   overall cluster name
     * @return  Kafka cluster instance
     */
    public static KafkaCluster fromStatefulSet(StatefulSetOperations statefulSetOperations, String namespace, String cluster) {

        StatefulSet ss = statefulSetOperations.get(namespace, cluster + KafkaCluster.NAME_SUFFIX);

        KafkaCluster kafka =  new KafkaCluster(namespace, cluster);

        kafka.setLabels(ss.getMetadata().getLabels());
        kafka.setReplicas(ss.getSpec().getReplicas());
        kafka.setImage(ss.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
        kafka.setHealthCheckInitialDelay(ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds());
        kafka.setHealthCheckInitialDelay(ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds());

        Map<String, String> vars = ss.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().stream().collect(
                Collectors.toMap(EnvVar::getName, EnvVar::getValue));

        kafka.setZookeeperConnect(vars.getOrDefault(KEY_KAFKA_ZOOKEEPER_CONNECT, ss.getMetadata().getName() + "-zookeeper:2181"));
        kafka.setDefaultReplicationFactor(Integer.parseInt(vars.getOrDefault(KEY_KAFKA_DEFAULT_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_DEFAULT_REPLICATION_FACTOR))));
        kafka.setOffsetsTopicReplicationFactor(Integer.parseInt(vars.getOrDefault(KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR))));
        kafka.setTransactionStateLogReplicationFactor(Integer.parseInt(vars.getOrDefault(KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR))));

        kafka.setMetricsEnabled(Boolean.parseBoolean(vars.getOrDefault(KEY_KAFKA_METRICS_ENABLED, String.valueOf(DEFAULT_KAFKA_METRICS_ENABLED))));
        if (kafka.isMetricsEnabled()) {
            kafka.setMetricsConfigName(cluster + KafkaCluster.METRICS_CONFIG_SUFFIX);
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
     * @param configMapOperations The means of getting the CM to compare with
     * @param statefulSetOperations The means of getting the SS to compare with
     * @param namespace Kubernetes/OpenShift namespace where cluster resources belong to
     * @return  ClusterDiffResult instance with differences
     */
    public ClusterDiffResult diff(
            ConfigMapOperations configMapOperations,
            StatefulSetOperations statefulSetOperations,
                                  String namespace)  {
        StatefulSet ss = statefulSetOperations.get(namespace, getName());
        ConfigMap metricsConfigMap = configMapOperations.get(namespace, getMetricsConfigName());

        ClusterDiffResult diff = new ClusterDiffResult();

        if (replicas > ss.getSpec().getReplicas()) {
            log.info("Diff: Expected replicas {}, actual replicas {}", replicas, ss.getSpec().getReplicas());
            diff.setScaleUp(true);
        }
        else if (replicas < ss.getSpec().getReplicas()) {
            log.info("Diff: Expected replicas {}, actual replicas {}", replicas, ss.getSpec().getReplicas());
            diff.setScaleDown(true);
        }

        if (!getLabelsWithName().equals(ss.getMetadata().getLabels()))    {
            log.info("Diff: Expected labels {}, actual labels {}", getLabelsWithName(), ss.getMetadata().getLabels());
            diff.setDifferent(true);
            diff.setRollingUpdate(true);
        }

        if (!image.equals(ss.getSpec().getTemplate().getSpec().getContainers().get(0).getImage())) {
            log.info("Diff: Expected image {}, actual image {}", image, ss.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
            diff.setDifferent(true);
            diff.setRollingUpdate(true);
        }

        Map<String, String> vars = ss.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().stream().collect(
                Collectors.toMap(EnvVar::getName, EnvVar::getValue));

        if (!zookeeperConnect.equals(vars.getOrDefault(KEY_KAFKA_ZOOKEEPER_CONNECT, DEFAULT_KAFKA_ZOOKEEPER_CONNECT))
                || defaultReplicationFactor != Integer.parseInt(vars.getOrDefault(KEY_KAFKA_DEFAULT_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_DEFAULT_REPLICATION_FACTOR)))
                || offsetsTopicReplicationFactor != Integer.parseInt(vars.getOrDefault(KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR)))
                || transactionStateLogReplicationFactor != Integer.parseInt(vars.getOrDefault(KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR, String.valueOf(DEFAULT_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR)))
                ) {
            log.info("Diff: Kafka options changed");
            diff.setDifferent(true);
            diff.setRollingUpdate(true);
        }

        if (healthCheckInitialDelay != ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds()
                || healthCheckTimeout != ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds()) {
            log.info("Diff: Kafka healthcheck timing changed");
            diff.setDifferent(true);
            diff.setRollingUpdate(true);
        }

        if (isMetricsEnabled != Boolean.parseBoolean(vars.getOrDefault(KEY_KAFKA_METRICS_ENABLED, String.valueOf(DEFAULT_KAFKA_METRICS_ENABLED)))) {
            log.info("Diff: Kafka metrics enabled/disabled");
            diff.setMetricsChanged(true);
            diff.setRollingUpdate(true);
        } else {

            if (isMetricsEnabled) {
                JsonObject metricsConfig = new JsonObject(metricsConfigMap.getData().get(METRICS_CONFIG_FILE));
                if (!this.metricsConfig.equals(metricsConfig)) {
                    diff.setMetricsChanged(true);
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
        boolean isStorageRejected = (storageDiffResult.isType() || storageDiffResult.isSize() ||
                storageDiffResult.isStorageClass() || storageDiffResult.isSelector());

        // only delete-claim flag can be changed
        if (!isStorageRejected && (storage.type() == Storage.StorageType.PERSISTENT_CLAIM)) {
            if (storageDiffResult.isDeleteClaim()) {
                diff.setDifferent(true);
            }
        } else if (isStorageRejected) {
            log.warn("Changing storage configuration other than delete-claim is not supported !");
        }

        return diff;
    }

    /**
     * Generates a Service according to configured defaults
     * @return The generated Service
     */
    public Service generateService() {

        return createService("ClusterIP",
                Collections.singletonList(createServicePort(CLIENT_PORT_NAME, CLIENT_PORT, CLIENT_PORT, "TCP")));
    }

    /**
     * Generates a headless Service according to configured defaults
     * @return The generated Service
     */
    public Service generateHeadlessService() {

        return createHeadlessService(headlessName,
                Collections.singletonList(createServicePort(CLIENT_PORT_NAME, CLIENT_PORT, CLIENT_PORT, "TCP")));
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
        List<ContainerPort> portList = new ArrayList<>();
        portList.add(createContainerPort(CLIENT_PORT_NAME, CLIENT_PORT, "TCP"));
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
        volumeMountList.add(createVolumeMount(volumeName, mounthPath));
        if (isMetricsEnabled) {
            volumeMountList.add(createVolumeMount(metricsConfigVolumeName, metricsConfigMountPath));
        }

        return volumeMountList;
    }

    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(new EnvVarBuilder().withName(KEY_KAFKA_ZOOKEEPER_CONNECT).withValue(zookeeperConnect).build());
        varList.add(new EnvVarBuilder().withName(KEY_KAFKA_DEFAULT_REPLICATION_FACTOR).withValue(String.valueOf(defaultReplicationFactor)).build());
        varList.add(new EnvVarBuilder().withName(KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR).withValue(String.valueOf(offsetsTopicReplicationFactor)).build());
        varList.add(new EnvVarBuilder().withName(KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR).withValue(String.valueOf(transactionStateLogReplicationFactor)).build());
        varList.add(new EnvVarBuilder().withName(KEY_KAFKA_METRICS_ENABLED).withValue(String.valueOf(isMetricsEnabled)).build());

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