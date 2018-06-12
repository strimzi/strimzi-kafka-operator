/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

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
import io.strimzi.operator.cluster.ClusterOperator;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZookeeperCluster extends AbstractModel {

    protected static final int CLIENT_PORT = 2181;
    protected static final String CLIENT_PORT_NAME = "clients";
    protected static final int CLUSTERING_PORT = 2888;
    protected static final String CLUSTERING_PORT_NAME = "clustering";
    protected static final int LEADER_ELECTION_PORT = 3888;
    protected static final String LEADER_ELECTION_PORT_NAME = "leader-election";
    protected static final int METRICS_PORT = 9404;
    protected static final String METRICS_PORT_NAME = "metrics";

    private static final String NAME_SUFFIX = "-zookeeper";
    private static final String HEADLESS_NAME_SUFFIX = NAME_SUFFIX + "-headless";
    private static final String METRICS_CONFIG_SUFFIX = NAME_SUFFIX + "-metrics-config";

    // Zookeeper configuration
    // N/A

    // Configuration defaults
    private static final String DEFAULT_IMAGE =
            System.getenv().getOrDefault("STRIMZI_DEFAULT_ZOOKEEPER_IMAGE", "strimzi/zookeeper:latest");
    private static final int DEFAULT_REPLICAS = 3;
    private static final int DEFAULT_HEALTHCHECK_DELAY = 15;
    private static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;
    private static final boolean DEFAULT_ZOOKEEPER_METRICS_ENABLED = false;

    // Configuration keys (ConfigMap)
    public static final String KEY_IMAGE = "zookeeper-image";
    public static final String KEY_REPLICAS = "zookeeper-nodes";
    public static final String KEY_HEALTHCHECK_DELAY = "zookeeper-healthcheck-delay";
    public static final String KEY_HEALTHCHECK_TIMEOUT = "zookeeper-healthcheck-timeout";
    public static final String KEY_METRICS_CONFIG = "zookeeper-metrics-config";
    public static final String KEY_STORAGE = "zookeeper-storage";
    public static final String KEY_JVM_OPTIONS = "zookeeper-jvmOptions";
    public static final String KEY_RESOURCES = "zookeeper-resources";
    public static final String KEY_ZOOKEEPER_CONFIG = "zookeeper-config";
    public static final String KEY_AFFINITY = "zookeeper-affinity";

    // Zookeeper configuration keys (EnvVariables)
    public static final String ENV_VAR_ZOOKEEPER_NODE_COUNT = "ZOOKEEPER_NODE_COUNT";
    public static final String ENV_VAR_ZOOKEEPER_METRICS_ENABLED = "ZOOKEEPER_METRICS_ENABLED";
    public static final String ENV_VAR_ZOOKEEPER_CONFIGURATION = "ZOOKEEPER_CONFIGURATION";

    public static String zookeeperClusterName(String cluster) {
        return cluster + ZookeeperCluster.NAME_SUFFIX;
    }

    public static String zookeeperMetricsName(String cluster) {
        return cluster + ZookeeperCluster.METRICS_CONFIG_SUFFIX;
    }

    public static String zookeeperHeadlessName(String cluster) {
        return cluster + ZookeeperCluster.HEADLESS_NAME_SUFFIX;
    }

    public static String zookeeperPodName(String cluster, int pod) {
        return zookeeperClusterName(cluster) + "-" + pod;
    }

    public static String getPersistentVolumeClaimName(String clusterName, int podId) {
        return VOLUME_NAME + "-" + clusterName + "-" + podId;
    }

    /**
     * Constructor
     *
     * @param namespace Kubernetes/OpenShift namespace where Zookeeper cluster resources are going to be created
     * @param cluster   overall cluster name
     */
    private ZookeeperCluster(String namespace, String cluster, Labels labels) {

        super(namespace, cluster, labels.withType(AssemblyType.KAFKA));
        this.name = zookeeperClusterName(cluster);
        this.headlessName = zookeeperHeadlessName(cluster);
        this.metricsConfigName = zookeeperMetricsName(cluster);
        this.image = DEFAULT_IMAGE;
        this.replicas = DEFAULT_REPLICAS;
        this.healthCheckPath = "/opt/kafka/zookeeper_healthcheck.sh";
        this.healthCheckTimeout = DEFAULT_HEALTHCHECK_TIMEOUT;
        this.healthCheckInitialDelay = DEFAULT_HEALTHCHECK_DELAY;
        this.isMetricsEnabled = DEFAULT_ZOOKEEPER_METRICS_ENABLED;

        this.mountPath = "/var/lib/zookeeper";
        this.metricsConfigVolumeName = "zookeeper-metrics-config";
        this.metricsConfigMountPath = "/opt/prometheus/config/";
    }


    /**
     * Create a Zookeeper cluster from the related ConfigMap resource
     *
     * @param kafkaClusterCm ConfigMap with cluster configuration
     * @return Zookeeper cluster instance
     */
    public static ZookeeperCluster fromConfigMap(ConfigMap kafkaClusterCm) {
        ZookeeperCluster zk = new ZookeeperCluster(kafkaClusterCm.getMetadata().getNamespace(), kafkaClusterCm.getMetadata().getName(),
                Labels.fromResource(kafkaClusterCm));

        Map<String, String> data = kafkaClusterCm.getData();
        zk.setReplicas(Utils.getInteger(data, KEY_REPLICAS, DEFAULT_REPLICAS));
        zk.setImage(Utils.getNonEmptyString(data, KEY_IMAGE, DEFAULT_IMAGE));
        zk.setHealthCheckInitialDelay(Utils.getInteger(data, KEY_HEALTHCHECK_DELAY, DEFAULT_HEALTHCHECK_DELAY));
        zk.setHealthCheckTimeout(Utils.getInteger(data, KEY_HEALTHCHECK_TIMEOUT, DEFAULT_HEALTHCHECK_TIMEOUT));

        JsonObject metricsConfig = Utils.getJson(data, KEY_METRICS_CONFIG);
        zk.setMetricsEnabled(metricsConfig != null);
        if (zk.isMetricsEnabled()) {
            zk.setMetricsConfig(metricsConfig);
        }

        zk.setStorage(Utils.getStorage(data, KEY_STORAGE));

        zk.setConfiguration(Utils.getZookeeperConfiguration(data, KEY_ZOOKEEPER_CONFIG));

        zk.setResources(Resources.fromJson(data.get(KEY_RESOURCES)));
        zk.setJvmOptions(JvmOptions.fromJson(data.get(KEY_JVM_OPTIONS)));
        zk.setUserAffinity(Utils.getAffinity(data.get(KEY_AFFINITY)));

        return zk;
    }

    /**
     * Create a Zookeeper cluster from the deployed StatefulSet resource
     *
     * @param namespace Kubernetes/OpenShift namespace where cluster resources belong to
     * @param cluster   overall cluster name
     * @return  Zookeeper cluster instance
     */
    public static ZookeeperCluster fromAssembly(StatefulSet ss,
                                                String namespace, String cluster) {
        ZookeeperCluster zk =  new ZookeeperCluster(namespace, cluster,
                Labels.fromResource(ss));

        zk.setReplicas(ss.getSpec().getReplicas());
        Container container = ss.getSpec().getTemplate().getSpec().getContainers().get(0);
        zk.setImage(container.getImage());
        zk.setHealthCheckInitialDelay(container.getReadinessProbe().getInitialDelaySeconds());
        zk.setHealthCheckTimeout(container.getReadinessProbe().getTimeoutSeconds());

        Map<String, String> vars = containerEnvVars(container);

        zk.setMetricsEnabled(Utils.getBoolean(vars, ENV_VAR_ZOOKEEPER_METRICS_ENABLED, DEFAULT_ZOOKEEPER_METRICS_ENABLED));
        if (zk.isMetricsEnabled()) {
            zk.setMetricsConfigName(zookeeperMetricsName(cluster));
        }

        if (!ss.getSpec().getVolumeClaimTemplates().isEmpty()) {

            Storage storage = Storage.fromPersistentVolumeClaim(ss.getSpec().getVolumeClaimTemplates().get(0));
            if (ss.getMetadata().getAnnotations() != null) {
                String deleteClaimAnnotation = String.format("%s/%s", ClusterOperator.STRIMZI_CLUSTER_OPERATOR_DOMAIN, Storage.DELETE_CLAIM_FIELD);
                storage.withDeleteClaim(Boolean.valueOf(ss.getMetadata().getAnnotations().computeIfAbsent(deleteClaimAnnotation, s -> "false")));
            }
            zk.setStorage(storage);
        } else {
            Storage storage = new Storage(Storage.StorageType.EPHEMERAL);
            zk.setStorage(storage);
        }

        String zookeeperConfiguration = containerEnvVars(container).getOrDefault(ENV_VAR_ZOOKEEPER_CONFIGURATION, "");
        zk.setConfiguration(new ZookeeperConfiguration(zookeeperConfiguration));

        return zk;
    }

    public Service generateService() {
        List<ServicePort> ports = new ArrayList<>(2);
        if (isMetricsEnabled()) {
            ports.add(createServicePort(METRICS_PORT_NAME, METRICS_PORT, METRICS_PORT, "TCP"));
        }
        ports.add(createServicePort(CLIENT_PORT_NAME, CLIENT_PORT, CLIENT_PORT, "TCP"));

        return createService("ClusterIP", ports);
    }

    public Service generateHeadlessService() {
        Map<String, String> annotations = Collections.singletonMap("service.alpha.kubernetes.io/tolerate-unready-endpoints", "true");
        return createHeadlessService(headlessName, getServicePortList(), annotations);
    }

    public StatefulSet generateStatefulSet(boolean isOpenShift) {

        return createStatefulSet(
                getContainerPortList(),
                getVolumes(),
                getVolumeClaims(),
                getVolumeMounts(),
                createExecProbe(healthCheckPath, healthCheckInitialDelay, healthCheckTimeout),
                createExecProbe(healthCheckPath, healthCheckInitialDelay, healthCheckTimeout),
                resources(),
                getMergedAffinity(),
                getInitContainers(),
                isOpenShift);
    }

    public ConfigMap generateMetricsConfigMap() {
        if (isMetricsEnabled()) {
            Map<String, String> data = new HashMap<>();
            data.put(METRICS_CONFIG_FILE, getMetricsConfig().toString());
            return createConfigMap(getMetricsConfigName(), data);
        } else {
            return null;
        }
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(buildEnvVar(ENV_VAR_ZOOKEEPER_NODE_COUNT, Integer.toString(replicas)));
        varList.add(buildEnvVar(ENV_VAR_ZOOKEEPER_METRICS_ENABLED, String.valueOf(isMetricsEnabled)));
        heapOptions(varList, 0.75, 2L * 1024L * 1024L * 1024L);
        jvmPerformanceOptions(varList);
        varList.add(buildEnvVar(ENV_VAR_ZOOKEEPER_CONFIGURATION, configuration.getConfiguration()));

        return varList;
    }

    private List<ServicePort> getServicePortList() {
        List<ServicePort> portList = new ArrayList<>();
        portList.add(createServicePort(CLIENT_PORT_NAME, CLIENT_PORT, CLIENT_PORT, "TCP"));
        portList.add(createServicePort(CLUSTERING_PORT_NAME, CLUSTERING_PORT, CLUSTERING_PORT, "TCP"));
        portList.add(createServicePort(LEADER_ELECTION_PORT_NAME, LEADER_ELECTION_PORT, LEADER_ELECTION_PORT, "TCP"));

        return portList;
    }

    private List<ContainerPort> getContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>();
        portList.add(createContainerPort(CLIENT_PORT_NAME, CLIENT_PORT, "TCP"));
        portList.add(createContainerPort(CLUSTERING_PORT_NAME, CLUSTERING_PORT, "TCP"));
        portList.add(createContainerPort(LEADER_ELECTION_PORT_NAME, LEADER_ELECTION_PORT, "TCP"));
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

        return volumeMountList;
    }

}
