/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PodAffinityTerm;
import io.fabric8.kubernetes.api.model.PodAntiAffinity;
import io.fabric8.kubernetes.api.model.PodAntiAffinityBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.WeightedPodAffinityTerm;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.strimzi.operator.cluster.ClusterOperator;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaCluster extends AbstractModel {

    public static final String KAFKA_SERVICE_ACCOUNT = "strimzi-kafka";

    private static final String KAFKA_INIT_IMAGE = "strimzi/kafka-init:latest";
    private static final String KAFKA_INIT_NAME = "kafka-init";
    private static final String KAFKA_INIT_VOLUME_NAME = "rack-volume";
    private static final String KAFKA_INIT_VOLUME_MOUNT = "/rack";
    private static final String ENV_VAR_KAFKA_INIT_RACK_TOPOLOGY_KEY = "RACK_TOPOLOGY_KEY";
    private static final String ENV_VAR_KAFKA_INIT_NODE_NAME = "NODE_NAME";

    protected static final int CLIENT_PORT = 9092;
    protected static final String CLIENT_PORT_NAME = "clients";

    protected static final int REPLICATION_PORT = 9091;
    protected static final String REPLICATION_PORT_NAME = "replication";

    private static final String NAME_SUFFIX = "-kafka";
    private static final String HEADLESS_NAME_SUFFIX = NAME_SUFFIX + "-headless";
    private static final String METRICS_CONFIG_SUFFIX = NAME_SUFFIX + "-metrics-config";

    // Kafka configuration
    private String zookeeperConnect = DEFAULT_KAFKA_ZOOKEEPER_CONNECT;
    private RackConfig rackConfig;

    // Configuration defaults
    private static final String DEFAULT_IMAGE =
            System.getenv().getOrDefault("STRIMZI_DEFAULT_KAFKA_IMAGE", "strimzi/kafka:latest");

    private static final int DEFAULT_REPLICAS = 3;
    private static final int DEFAULT_HEALTHCHECK_DELAY = 15;
    private static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;
    private static final boolean DEFAULT_KAFKA_METRICS_ENABLED = false;

    // Kafka configuration defaults
    private static final String DEFAULT_KAFKA_ZOOKEEPER_CONNECT = "zookeeper:2181";

    // Configuration keys (in ConfigMap)
    public static final String KEY_IMAGE = "kafka-image";
    public static final String KEY_REPLICAS = "kafka-nodes";
    public static final String KEY_HEALTHCHECK_DELAY = "kafka-healthcheck-delay";
    public static final String KEY_HEALTHCHECK_TIMEOUT = "kafka-healthcheck-timeout";
    public static final String KEY_METRICS_CONFIG = "kafka-metrics-config";
    public static final String KEY_STORAGE = "kafka-storage";
    public static final String KEY_KAFKA_CONFIG = "kafka-config";
    public static final String KEY_JVM_OPTIONS = "kafka-jvmOptions";
    public static final String KEY_RESOURCES = "kafka-resources";
    public static final String KEY_RACK = "kafka-rack";

    // Kafka configuration keys (EnvVariables)
    public static final String ENV_VAR_KAFKA_ZOOKEEPER_CONNECT = "KAFKA_ZOOKEEPER_CONNECT";
    private static final String ENV_VAR_KAFKA_METRICS_ENABLED = "KAFKA_METRICS_ENABLED";
    protected static final String ENV_VAR_KAFKA_CONFIGURATION = "KAFKA_CONFIGURATION";

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

    public static String kafkaPodName(String cluster, int pod) {
        return kafkaClusterName(cluster) + "-" + pod;
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

        Map<String, String> data = kafkaClusterCm.getData();
        kafka.setReplicas(Utils.getInteger(data, KEY_REPLICAS, DEFAULT_REPLICAS));
        kafka.setImage(Utils.getNonEmptyString(data, KEY_IMAGE, DEFAULT_IMAGE));
        kafka.setHealthCheckInitialDelay(Utils.getInteger(data, KEY_HEALTHCHECK_DELAY, DEFAULT_HEALTHCHECK_DELAY));
        kafka.setHealthCheckTimeout(Utils.getInteger(data, KEY_HEALTHCHECK_TIMEOUT, DEFAULT_HEALTHCHECK_TIMEOUT));

        kafka.setZookeeperConnect(kafkaClusterCm.getMetadata().getName() + "-zookeeper:2181");

        JsonObject metricsConfig = Utils.getJson(data, KEY_METRICS_CONFIG);
        kafka.setMetricsEnabled(metricsConfig != null);
        if (kafka.isMetricsEnabled()) {
            kafka.setMetricsConfig(metricsConfig);
        }

        kafka.setStorage(Utils.getStorage(data, KEY_STORAGE));

        kafka.setConfiguration(Utils.getKafkaConfiguration(data, KEY_KAFKA_CONFIG));

        kafka.setResources(Resources.fromJson(data.get(KEY_RESOURCES)));
        kafka.setJvmOptions(JvmOptions.fromJson(data.get(KEY_JVM_OPTIONS)));

        RackConfig rackConfig = RackConfig.fromJson(data.get(KEY_RACK));
        if (rackConfig != null) {
            kafka.setRackConfig(rackConfig);
        }

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
    public static KafkaCluster fromAssembly(StatefulSet ss, String namespace, String cluster) {

        KafkaCluster kafka = new KafkaCluster(namespace, cluster, Labels.fromResource(ss));

        kafka.setReplicas(ss.getSpec().getReplicas());
        Container container = ss.getSpec().getTemplate().getSpec().getContainers().get(0);
        kafka.setImage(container.getImage());
        kafka.setHealthCheckInitialDelay(container.getReadinessProbe().getInitialDelaySeconds());
        kafka.setHealthCheckTimeout(container.getReadinessProbe().getTimeoutSeconds());

        Map<String, String> vars = containerEnvVars(container);

        kafka.setZookeeperConnect(vars.getOrDefault(ENV_VAR_KAFKA_ZOOKEEPER_CONNECT, ss.getMetadata().getName() + "-zookeeper:2181"));

        kafka.setMetricsEnabled(Utils.getBoolean(vars, ENV_VAR_KAFKA_METRICS_ENABLED, DEFAULT_KAFKA_METRICS_ENABLED));
        if (kafka.isMetricsEnabled()) {
            kafka.setMetricsConfigName(metricConfigsName(cluster));
        }

        if (!ss.getSpec().getVolumeClaimTemplates().isEmpty()) {

            Storage storage = Storage.fromPersistentVolumeClaim(ss.getSpec().getVolumeClaimTemplates().get(0));
            if (ss.getMetadata().getAnnotations() != null) {
                String deleteClaimAnnotation = String.format("%s/%s", ClusterOperator.STRIMZI_CLUSTER_OPERATOR_DOMAIN, Storage.DELETE_CLAIM_FIELD);
                storage.withDeleteClaim(Boolean.valueOf(ss.getMetadata().getAnnotations().computeIfAbsent(deleteClaimAnnotation, s -> "false")));
            }
            kafka.setStorage(storage);
        } else {
            Storage storage = new Storage(Storage.StorageType.EPHEMERAL);
            kafka.setStorage(storage);
        }

        String kafkaConfiguration = containerEnvVars(container).get(ENV_VAR_KAFKA_CONFIGURATION);
        if (kafkaConfiguration != null) {
            kafka.setConfiguration(new KafkaConfiguration(kafkaConfiguration));
        }

        Affinity affinity = ss.getSpec().getTemplate().getSpec().getAffinity();
        if (affinity != null) {
            String rackTopologyKey = affinity.getPodAntiAffinity().getPreferredDuringSchedulingIgnoredDuringExecution().get(0).getPodAffinityTerm().getTopologyKey();
            kafka.setRackConfig(new RackConfig(rackTopologyKey));
        }

        return kafka;
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
     * Generates a StatefulSet according to configured defaults
     * @param isOpenShift True iff this operator is operating within OpenShift.
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
                resources(),
                getAffinity(),
                getInitContainers(),
                isOpenShift);
    }


    /**
     * Generates a metrics ConfigMap according to configured defaults
     * @return The generated ConfigMap
     */
    public ConfigMap generateMetricsConfigMap() {
        if (isMetricsEnabled()) {
            Map<String, String> data = new HashMap<>();
            data.put(METRICS_CONFIG_FILE, getMetricsConfig().toString());
            return createConfigMap(getMetricsConfigName(), data);
        } else {
            return null;
        }
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
            volumeList.add(createEmptyDirVolume(VOLUME_NAME));
        }
        if (isMetricsEnabled) {
            volumeList.add(createConfigMapVolume(metricsConfigVolumeName, metricsConfigName));
        }
        if (rackConfig != null) {
            volumeList.add(createEmptyDirVolume(KAFKA_INIT_VOLUME_NAME));
        }

        return volumeList;
    }

    private List<PersistentVolumeClaim> getVolumeClaims() {
        List<PersistentVolumeClaim> pvcList = new ArrayList<>();
        if (storage.type() == Storage.StorageType.PERSISTENT_CLAIM) {
            pvcList.add(createPersistentVolumeClaim(VOLUME_NAME));
        }
        return pvcList;
    }

    private List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> volumeMountList = new ArrayList<>();
        volumeMountList.add(createVolumeMount(VOLUME_NAME, mountPath));
        if (isMetricsEnabled) {
            volumeMountList.add(createVolumeMount(metricsConfigVolumeName, metricsConfigMountPath));
        }
        if (rackConfig != null) {
            volumeMountList.add(createVolumeMount(KAFKA_INIT_VOLUME_NAME, KAFKA_INIT_VOLUME_MOUNT));
        }

        return volumeMountList;
    }

    @Override
    protected Affinity getAffinity() {

        List<WeightedPodAffinityTerm> weightedPodAffinityTerms = new ArrayList<>();
        Affinity affinity = null;

        // adding the affinity term for rack feature only if it's enabled
        if (rackConfig != null) {
            Map<String, String> matchLabels = new HashMap<>();
            matchLabels.put(Labels.STRIMZI_CLUSTER_LABEL, cluster);
            matchLabels.put(Labels.STRIMZI_NAME_LABEL, name);
            matchLabels.put(Labels.STRIMZI_TYPE_LABEL, AssemblyType.KAFKA.toString());
            LabelSelector labelSelector = new LabelSelector(null, matchLabels);

            PodAffinityTerm podAffinityTerm = new PodAffinityTerm(labelSelector, null, rackConfig.getTopologyKey());
            WeightedPodAffinityTerm weightedPodAffinityTerm = new WeightedPodAffinityTerm(podAffinityTerm, 100);
            weightedPodAffinityTerms.add(weightedPodAffinityTerm);
        }

        // creating the affinity only if related terms were added
        if (weightedPodAffinityTerms.size() > 0) {
            PodAntiAffinity podAntiAffinity = new PodAntiAffinityBuilder()
                    .withPreferredDuringSchedulingIgnoredDuringExecution(weightedPodAffinityTerms)
                    .build();

            affinity = new AffinityBuilder()
                    .withPodAntiAffinity(podAntiAffinity)
                    .build();
        }

        return affinity;
    }

    @Override
    protected List<Container> getInitContainers() {

        List<Container> initContainers = new ArrayList<>();

        if (rackConfig != null) {

            List<EnvVar> varList =
                    Arrays.asList(buildEnvVarFromFieldRef(ENV_VAR_KAFKA_INIT_NODE_NAME, "spec.nodeName"),
                            buildEnvVar(ENV_VAR_KAFKA_INIT_RACK_TOPOLOGY_KEY, rackConfig.getTopologyKey()));

            Container initContainer = new ContainerBuilder()
                    .withName(KAFKA_INIT_NAME)
                    .withImage(KAFKA_INIT_IMAGE)
                    .withImagePullPolicy("IfNotPresent") // TODO: just for testing locally ... to remove!!!
                    .withEnv(varList)
                    .withVolumeMounts(createVolumeMount(KAFKA_INIT_VOLUME_NAME, KAFKA_INIT_VOLUME_MOUNT))
                    .build();

            initContainers.add(initContainer);
        }

        return initContainers;
    }

    @Override
    protected String getServiceAccountName() {
        return KAFKA_SERVICE_ACCOUNT;
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(buildEnvVar(ENV_VAR_KAFKA_ZOOKEEPER_CONNECT, zookeeperConnect));
        varList.add(buildEnvVar(ENV_VAR_KAFKA_METRICS_ENABLED, String.valueOf(isMetricsEnabled)));
        kafkaHeapOptions(varList, 0.5, 5L * 1024L * 1024L * 1024L);

        if (configuration != null) {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_CONFIGURATION, configuration.getConfiguration()));
        }

        return varList;
    }

    protected void setZookeeperConnect(String zookeeperConnect) {
        this.zookeeperConnect = zookeeperConnect;
    }

    protected void setRackConfig(RackConfig rackConfig) {
        this.rackConfig = rackConfig;
    }
}