package io.strimzi.controller.cluster.resources;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KafkaCluster extends AbstractCluster {

    public static final String TYPE = "kafka";

    private final int clientPort = 9092;
    private final String clientPortName = "clients";
    private final String mounthPath = "/var/lib/kafka";
    private final String volumeName = "kafka-storage";
    private final String metricsConfigVolumeName = "kafka-metrics-config";
    private final String metricsConfigMountPath = "/opt/prometheus/config/";

    // Kafka configuration
    private String zookeeperConnect = DEFAULT_KAFKA_ZOOKEEPER_CONNECT;
    private int defaultReplicationFactor = DEFAULT_KAFKA_DEFAULT_REPLICATION_FACTOR;
    private int offsetsTopicReplicationFactor = DEFAULT_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR;
    private int transactionStateLogReplicationFactor = DEFAULT_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR;

    // Configuration defaults
    private static String DEFAULT_IMAGE = "strimzi/kafka-statefulsets:latest";
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
    private static String KEY_IMAGE = "kafka-image";
    private static String KEY_REPLICAS = "kafka-nodes";
    private static String KEY_HEALTHCHECK_DELAY = "kafka-healthcheck-delay";
    private static String KEY_HEALTHCHECK_TIMEOUT = "kafka-healthcheck-timeout";
    private static String KEY_METRICS_CONFIG = "kafka-metrics-config";
    private static String KEY_STORAGE = "kafka-storage";

    // Kafka configuration keys
    private static String KEY_KAFKA_ZOOKEEPER_CONNECT = "KAFKA_ZOOKEEPER_CONNECT";
    private static String KEY_KAFKA_DEFAULT_REPLICATION_FACTOR = "KAFKA_DEFAULT_REPLICATION_FACTOR";
    private static String KEY_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR = "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR";
    private static String KEY_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR = "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR";
    private static String KEY_KAFKA_METRICS_ENABLED = "KAFKA_METRICS_ENABLED";

    private KafkaCluster(String namespace, String name) {
        super(namespace, name);
        this.headlessName = name + "-headless";
        this.image = DEFAULT_IMAGE;
        this.replicas = DEFAULT_REPLICAS;
        this.healthCheckPath = "/opt/kafka/kafka_healthcheck.sh";
        this.healthCheckTimeout = DEFAULT_HEALTHCHECK_TIMEOUT;
        this.healthCheckInitialDelay = DEFAULT_HEALTHCHECK_DELAY;
        this.isMetricsEnabled = DEFAULT_KAFKA_METRICS_ENABLED;
    }

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

        // TODO : making more checks for exception on JSON ?
        String metricsConfig = cm.getData().get(KEY_METRICS_CONFIG);
        kafka.setMetricsConfigName(cm.getMetadata().getName() + "-metrics-config");
        kafka.setMetricsEnabled(metricsConfig != null);
        if (kafka.isMetricsEnabled()) {
            kafka.setMetricsConfig(new JsonObject(metricsConfig));
        }

        // TODO : making more checks for exception on JSON ?
        String storageConfig = cm.getData().get(KEY_STORAGE);
        kafka.setStorage(Storage.fromJson(new JsonObject(storageConfig)));

        return kafka;
    }

    // This is currently needed only to delete the headless service. All other deletions can be done just using namespace
    // and name. Do we need this as it is? Or would it be enough to create just and empty shell from name and namespace
    // which would generate the headless name?
    public static KafkaCluster fromStatefulSet(StatefulSet ss) {
        KafkaCluster kafka =  new KafkaCluster(ss.getMetadata().getNamespace(), ss.getMetadata().getName());

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
            kafka.setMetricsConfigName(ss.getMetadata().getName() + "-metrics-config");
        }

        if (!ss.getSpec().getVolumeClaimTemplates().isEmpty()) {
            Storage storage = Storage.fromPersistentVolumeClaim(ss.getSpec().getVolumeClaimTemplates().get(0));
            kafka.setStorage(storage);
        } else {
            Storage storage = new Storage(Storage.StorageType.EPHEMERAL);
            kafka.setStorage(storage);
        }

        return kafka;
    }

    public ClusterDiffResult diff(StatefulSet ss, ConfigMap metricsConfigMap)  {
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
                JsonObject metricsConfig = new JsonObject(metricsConfigMap.getData().get("config.yml"));
                if (!this.metricsConfig.equals(metricsConfig)) {
                    diff.setMetricsChanged(true);
                }
            }
        }

        return diff;
    }

    public Service generateService() {

        return createService("ClusterIP",
                Collections.singletonList(createServicePort(clientPortName, clientPort, clientPort, "TCP")));
    }

    public Service generateHeadlessService() {

        return createHeadlessService(headlessName,
                Collections.singletonList(createServicePort(clientPortName, clientPort, clientPort, "TCP")));
    }

    public Service patchHeadlessService(Service svc) {

        return patchHeadlessService(headlessName, svc);
    }

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

    public ConfigMap generateMetricsConfigMap() {

        // TODO: making data field and configMap name constants ?
        Map<String, String> data = new HashMap<>();
        data.put("config.yml", metricsConfig.toString());

        return createConfigMap(metricsConfigName, data);
    }

    public ConfigMap patchMetricsConfigMap(ConfigMap cm) {

        Map<String, String> data = new HashMap<>();
        data.put("config.yml", metricsConfig != null ? metricsConfig.toString() : null);

        return patchConfigMap(cm, data);
    }

    public StatefulSet patchStatefulSet(StatefulSet statefulSet) {

        return patchStatefulSet(statefulSet,
                createExecProbe(healthCheckPath, healthCheckInitialDelay, healthCheckTimeout),
                createExecProbe(healthCheckPath, healthCheckInitialDelay, healthCheckTimeout));
    }

    private List<ContainerPort> getContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>();
        portList.add(createContainerPort(clientPortName, clientPort, "TCP"));
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