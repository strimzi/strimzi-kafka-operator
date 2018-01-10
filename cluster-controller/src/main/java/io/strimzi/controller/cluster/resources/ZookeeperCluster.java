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

public class ZookeeperCluster extends AbstractCluster {

    public static final String TYPE = "zookeeper";

    private final int clientPort = 2181;
    private final String clientPortName = "clients";
    private final int clusteringPort = 2888;
    private final String clusteringPortName = "clustering";
    private final int leaderElectionPort = 3888;
    private final String leaderElectionPortName = "leader-election";
    private final String mounthPath = "/var/lib/zookeeper";
    private final String volumeName = "zookeeper-storage";
    private final String metricsConfigVolumeName = "zookeeper-metrics-config";
    private final String metricsConfigMountPath = "/opt/prometheus/config/";

    // Zookeeper configuration
    // N/A

    // Configuration defaults
    private static String DEFAULT_IMAGE = "strimzi/zookeeper:latest";
    private static int DEFAULT_REPLICAS = 3;
    private static int DEFAULT_HEALTHCHECK_DELAY = 15;
    private static int DEFAULT_HEALTHCHECK_TIMEOUT = 5;
    private static boolean DEFAULT_ZOOKEEPER_METRICS_ENABLED = false;

    // Zookeeper configuration defaults
    // N/A

    // Configuration keys
    private static String KEY_IMAGE = "zookeeper-image";
    private static String KEY_REPLICAS = "zookeeper-nodes";
    private static String KEY_HEALTHCHECK_DELAY = "zookeeper-healthcheck-delay";
    private static String KEY_HEALTHCHECK_TIMEOUT = "zookeeper-healthcheck-timeout";
    private static String KEY_METRICS_CONFIG = "zookeeper-metrics-config";
    private static String KEY_STORAGE = "zookeeper-storage";

    // Zookeeper configuration keys
    private static String KEY_ZOOKEEPER_NODE_COUNT = "ZOOKEEPER_NODE_COUNT";
    private static String KEY_ZOOKEEPER_METRICS_ENABLED = "ZOOKEEPER_METRICS_ENABLED";

    private ZookeeperCluster(String namespace, String name) {
        super(namespace, name);
        this.headlessName = name + "-headless";
        this.image = DEFAULT_IMAGE;
        this.replicas = DEFAULT_REPLICAS;
        this.healthCheckPath = "/opt/zookeeper/zookeeper_healthcheck.sh";
        this.healthCheckTimeout = DEFAULT_HEALTHCHECK_TIMEOUT;
        this.healthCheckInitialDelay = DEFAULT_HEALTHCHECK_DELAY;
        this.isMetricsEnabled = DEFAULT_ZOOKEEPER_METRICS_ENABLED;
    }

    public static ZookeeperCluster fromConfigMap(ConfigMap cm) {
        String name = cm.getMetadata().getName() + "-zookeeper";
        ZookeeperCluster zk = new ZookeeperCluster(cm.getMetadata().getNamespace(), name);

        zk.setLabels(cm.getMetadata().getLabels());

        zk.setReplicas(Integer.parseInt(cm.getData().getOrDefault(KEY_REPLICAS, String.valueOf(DEFAULT_REPLICAS))));
        zk.setImage(cm.getData().getOrDefault(KEY_IMAGE, DEFAULT_IMAGE));
        zk.setHealthCheckInitialDelay(Integer.parseInt(cm.getData().getOrDefault(KEY_HEALTHCHECK_DELAY, String.valueOf(DEFAULT_HEALTHCHECK_DELAY))));
        zk.setHealthCheckTimeout(Integer.parseInt(cm.getData().getOrDefault(KEY_HEALTHCHECK_TIMEOUT, String.valueOf(DEFAULT_HEALTHCHECK_TIMEOUT))));

        // TODO : making more checks for exception on JSON ?
        String metricsConfig = cm.getData().get(KEY_METRICS_CONFIG);
        zk.setMetricsConfigName(cm.getMetadata().getName() + "-zookeeper-metrics-config");
        zk.setMetricsEnabled(metricsConfig != null);
        if (zk.isMetricsEnabled()) {
            zk.setMetricsConfig(new JsonObject(metricsConfig));
        }

        // TODO : making more checks for exception on JSON ?
        String storageConfig = cm.getData().get(KEY_STORAGE);
        zk.setStorage(Storage.fromJson(new JsonObject(storageConfig)));

        return zk;
    }

    // This is currently needed only to delete the headless service. All other deletions can be done just using namespace
    // and name. Do we need this as it is? Or would it be enough to create just and empty shell from name and namespace
    // which would generate the headless name?
    public static ZookeeperCluster fromStatefulSet(StatefulSet ss) {
        ZookeeperCluster zk =  new ZookeeperCluster(ss.getMetadata().getNamespace(), ss.getMetadata().getName());

        zk.setLabels(ss.getMetadata().getLabels());
        zk.setReplicas(ss.getSpec().getReplicas());
        zk.setImage(ss.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
        zk.setHealthCheckInitialDelay(ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds());
        zk.setHealthCheckInitialDelay(ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds());

        Map<String, String> vars = ss.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().stream().collect(
                Collectors.toMap(EnvVar::getName, EnvVar::getValue));

        zk.setMetricsEnabled(Boolean.parseBoolean(vars.getOrDefault(KEY_ZOOKEEPER_METRICS_ENABLED, String.valueOf(DEFAULT_ZOOKEEPER_METRICS_ENABLED))));
        if (zk.isMetricsEnabled()) {
            zk.setMetricsConfigName(ss.getMetadata().getName() + "-metrics-config");
        }

        if (!ss.getSpec().getVolumeClaimTemplates().isEmpty()) {
            Storage storage = Storage.fromPersistentVolumeClaim(ss.getSpec().getVolumeClaimTemplates().get(0));
            zk.setStorage(storage);
        } else {
            Storage storage = new Storage(Storage.StorageType.EPHEMERAL);
            zk.setStorage(storage);
        }

        return zk;
    }

    public ClusterDiffResult diff(StatefulSet ss, ConfigMap metricsConfigMap)  {
        ClusterDiffResult diff = new ClusterDiffResult();

        if (replicas > ss.getSpec().getReplicas()) {
            log.info("Diff: Expected replicas {}, actual replicas {}", replicas, ss.getSpec().getReplicas());
            diff.setScaleUp(true);
            diff.setRollingUpdate(true);
        }
        else if (replicas < ss.getSpec().getReplicas()) {
            log.info("Diff: Expected replicas {}, actual replicas {}", replicas, ss.getSpec().getReplicas());
            diff.setScaleDown(true);
            diff.setRollingUpdate(true);
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

        if (healthCheckInitialDelay != ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds()
                || healthCheckTimeout != ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds()) {
            log.info("Diff: Zookeeper healthcheck timing changed");
            diff.setDifferent(true);
            diff.setRollingUpdate(true);
        }

        Map<String, String> vars = ss.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().stream().collect(
                Collectors.toMap(EnvVar::getName, EnvVar::getValue));

        if (isMetricsEnabled != Boolean.parseBoolean(vars.getOrDefault(KEY_ZOOKEEPER_METRICS_ENABLED, String.valueOf(DEFAULT_ZOOKEEPER_METRICS_ENABLED)))) {
            log.info("Diff: Zookeeper metrics enabled/disabled");
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

        return createHeadlessService(headlessName, getServicePortList());
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

    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(new EnvVarBuilder().withName(KEY_ZOOKEEPER_NODE_COUNT).withValue(Integer.toString(replicas)).build());
        varList.add(new EnvVarBuilder().withName(KEY_ZOOKEEPER_METRICS_ENABLED).withValue(String.valueOf(isMetricsEnabled)).build());

        return varList;
    }

    private List<ServicePort> getServicePortList() {
        List<ServicePort> portList = new ArrayList<>();
        portList.add(createServicePort(clientPortName, clientPort, clientPort, "TCP"));
        portList.add(createServicePort(clusteringPortName, clusteringPort, clusteringPort, "TCP"));
        portList.add(createServicePort(leaderElectionPortName, leaderElectionPort, leaderElectionPort, "TCP"));

        return portList;
    }

    private List<ContainerPort> getContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>();
        portList.add(createContainerPort(clientPortName, clientPort, "TCP"));
        portList.add(createContainerPort(clusteringPortName, clusteringPort, "TCP"));
        portList.add(createContainerPort(leaderElectionPortName, leaderElectionPort,"TCP"));
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

    protected void setLabels(Map<String, String> labels) {
        Map<String, String> newLabels = new HashMap<>(labels);

        if (newLabels.containsKey("type") && newLabels.get("type").equals(KafkaCluster.TYPE)) {
            newLabels.put("type", TYPE);
        }

        super.setLabels(newLabels);
    }
}
