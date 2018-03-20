/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.resources;

import io.fabric8.kubernetes.api.model.ConfigMap;
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

public class ZookeeperCluster extends AbstractCluster {

    public static final String TYPE = "zookeeper";

    private static final int CLIENT_PORT = 2181;
    private static final String CLIENT_PORT_NAME = "clients";
    private static final int CLUSTERING_PORT = 2888;
    private static final String CLUSTERING_PORT_NAME = "clustering";
    private static final int LEADER_ELECTION_PORT = 3888;
    private static final String LEADER_ELECTION_PORT_NAME = "leader-election";

    private static final String NAME_SUFFIX = "-zookeeper";
    private static final String HEADLESS_NAME_SUFFIX = NAME_SUFFIX + "-headless";
    private static final String METRICS_CONFIG_SUFFIX = NAME_SUFFIX + "-metrics-config";

    // Zookeeper configuration
    // N/A

    // Configuration defaults
    private static final String DEFAULT_IMAGE = "strimzi/zookeeper:latest";
    private static final int DEFAULT_REPLICAS = 3;
    private static final int DEFAULT_HEALTHCHECK_DELAY = 15;
    private static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;
    private static final boolean DEFAULT_ZOOKEEPER_METRICS_ENABLED = false;

    // Zookeeper configuration defaults
    // N/A

    // Configuration keys
    public static final String KEY_IMAGE = "zookeeper-image";
    public static final String KEY_REPLICAS = "zookeeper-nodes";
    public static final String KEY_HEALTHCHECK_DELAY = "zookeeper-healthcheck-delay";
    public static final String KEY_HEALTHCHECK_TIMEOUT = "zookeeper-healthcheck-timeout";
    public static final String KEY_METRICS_CONFIG = "zookeeper-metrics-config";
    public static final String KEY_STORAGE = "zookeeper-storage";

    // Zookeeper configuration keys
    private static final String KEY_ZOOKEEPER_NODE_COUNT = "ZOOKEEPER_NODE_COUNT";
    private static final String KEY_ZOOKEEPER_METRICS_ENABLED = "ZOOKEEPER_METRICS_ENABLED";

    public static String zookeeperClusterName(String cluster) {
        return cluster + ZookeeperCluster.NAME_SUFFIX;
    }

    public static String zookeeperMetricsName(String cluster) {
        return cluster + ZookeeperCluster.METRICS_CONFIG_SUFFIX;
    }

    public static String zookeeperHeadlessName(String cluster) {
        return cluster + ZookeeperCluster.HEADLESS_NAME_SUFFIX;
    }

    /**
     * Constructor
     *
     * @param namespace Kubernetes/OpenShift namespace where Zookeeper cluster resources are going to be created
     * @param cluster   overall cluster name
     */
    private ZookeeperCluster(String namespace, String cluster, Labels labels) {

        super(namespace, cluster, labels.withType(ZookeeperCluster.TYPE));
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

        zk.setReplicas(Integer.parseInt(kafkaClusterCm.getData().getOrDefault(KEY_REPLICAS, String.valueOf(DEFAULT_REPLICAS))));
        zk.setImage(kafkaClusterCm.getData().getOrDefault(KEY_IMAGE, DEFAULT_IMAGE));
        zk.setHealthCheckInitialDelay(Integer.parseInt(kafkaClusterCm.getData().getOrDefault(KEY_HEALTHCHECK_DELAY, String.valueOf(DEFAULT_HEALTHCHECK_DELAY))));
        zk.setHealthCheckTimeout(Integer.parseInt(kafkaClusterCm.getData().getOrDefault(KEY_HEALTHCHECK_TIMEOUT, String.valueOf(DEFAULT_HEALTHCHECK_TIMEOUT))));

        String metricsConfig = kafkaClusterCm.getData().get(KEY_METRICS_CONFIG);
        zk.setMetricsEnabled(metricsConfig != null);
        if (zk.isMetricsEnabled()) {
            zk.setMetricsConfig(new JsonObject(metricsConfig));
        }

        String storageConfig = kafkaClusterCm.getData().get(KEY_STORAGE);
        zk.setStorage(Storage.fromJson(new JsonObject(storageConfig)));

        return zk;
    }

    /**
     * Create a Zookeeper cluster from the deployed StatefulSet resource
     *
     * @param namespace Kubernetes/OpenShift namespace where cluster resources belong to
     * @param cluster   overall cluster name
     * @return  Zookeeper cluster instance
     */
    public static ZookeeperCluster fromStatefulSet(StatefulSet ss,
                                                   String namespace, String cluster) {
        ZookeeperCluster zk =  new ZookeeperCluster(namespace, cluster,
                Labels.fromResource(ss));

        zk.setReplicas(ss.getSpec().getReplicas());
        zk.setImage(ss.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
        zk.setHealthCheckInitialDelay(ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds());
        zk.setHealthCheckInitialDelay(ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds());

        Map<String, String> vars = containerEnvVars(ss.getSpec().getTemplate().getSpec().getContainers().get(0));

        zk.setMetricsEnabled(Boolean.parseBoolean(vars.getOrDefault(KEY_ZOOKEEPER_METRICS_ENABLED, String.valueOf(DEFAULT_ZOOKEEPER_METRICS_ENABLED))));
        if (zk.isMetricsEnabled()) {
            zk.setMetricsConfigName(zookeeperMetricsName(cluster));
        }

        if (!ss.getSpec().getVolumeClaimTemplates().isEmpty()) {

            Storage storage = Storage.fromPersistentVolumeClaim(ss.getSpec().getVolumeClaimTemplates().get(0));
            if (ss.getMetadata().getAnnotations() != null) {
                String deleteClaimAnnotation = String.format("%s/%s", ClusterController.STRIMZI_CLUSTER_CONTROLLER_DOMAIN, Storage.DELETE_CLAIM_FIELD);
                storage.withDeleteClaim(Boolean.valueOf(ss.getMetadata().getAnnotations().computeIfAbsent(deleteClaimAnnotation, s -> "false")));
            }
            zk.setStorage(storage);
        } else {
            Storage storage = new Storage(Storage.StorageType.EPHEMERAL);
            zk.setStorage(storage);
        }

        return zk;
    }

    /**
     * Return the differences between the current Zookeeper cluster and the deployed one
     *
     * @return  ClusterDiffResult instance with differences
     */
    public ClusterDiffResult diff(ConfigMap metricsConfigMap,
                                  StatefulSet ss)  {
        boolean scaleUp = false;
        boolean scaleDown = false;
        boolean rollingUpdate = false;
        boolean different = false;
        boolean metricsChanged = false;

        if (replicas > ss.getSpec().getReplicas()) {
            log.info("Diff: Expected replicas {}, actual replicas {}", replicas, ss.getSpec().getReplicas());
            scaleUp = true;
            rollingUpdate = true;
        } else if (replicas < ss.getSpec().getReplicas()) {
            log.info("Diff: Expected replicas {}, actual replicas {}", replicas, ss.getSpec().getReplicas());
            scaleDown = true;
            rollingUpdate = true;
        }

        if (!getLabelsWithName().equals(ss.getMetadata().getLabels()))    {
            log.info("Diff: Expected labels {}, actual labels {}", getLabelsWithName(), ss.getMetadata().getLabels());
            different = true;
            rollingUpdate = true;
        }

        if (!image.equals(ss.getSpec().getTemplate().getSpec().getContainers().get(0).getImage())) {
            log.info("Diff: Expected image {}, actual image {}", image, ss.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
            different = true;
            rollingUpdate = true;
        }

        if (healthCheckInitialDelay != ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds()
                || healthCheckTimeout != ss.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds()) {
            log.info("Diff: Zookeeper healthcheck timing changed");
            different = true;
            rollingUpdate = true;
        }

        Map<String, String> vars = containerEnvVars(ss.getSpec().getTemplate().getSpec().getContainers().get(0));

        if (isMetricsEnabled != Boolean.parseBoolean(vars.getOrDefault(KEY_ZOOKEEPER_METRICS_ENABLED, String.valueOf(DEFAULT_ZOOKEEPER_METRICS_ENABLED)))) {
            log.info("Diff: Zookeeper metrics enabled/disabled");
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

    public Service generateService() {

        return createService("ClusterIP",
                Collections.singletonList(createServicePort(CLIENT_PORT_NAME, CLIENT_PORT, CLIENT_PORT, "TCP")));
    }

    public Service generateHeadlessService() {
        Map<String, String> annotations = Collections.singletonMap("service.alpha.kubernetes.io/tolerate-unready-endpoints", "true");
        return createHeadlessService(headlessName, getServicePortList(), annotations);
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

        Map<String, String> data = new HashMap<>();
        data.put(METRICS_CONFIG_FILE, metricsConfig.toString());

        return createConfigMap(metricsConfigName, data);
    }

    public ConfigMap patchMetricsConfigMap(ConfigMap cm) {

        Map<String, String> data = new HashMap<>();
        data.put(METRICS_CONFIG_FILE, metricsConfig != null ? metricsConfig.toString() : null);

        return patchConfigMap(cm, data);
    }

    public StatefulSet patchStatefulSet(StatefulSet statefulSet) {

        Map<String, String> annotations = new HashMap<>();
        annotations.put(String.format("%s/%s", ClusterController.STRIMZI_CLUSTER_CONTROLLER_DOMAIN, Storage.DELETE_CLAIM_FIELD),
                String.valueOf(storage.isDeleteClaim()));

        return patchStatefulSet(statefulSet,
                createExecProbe(healthCheckPath, healthCheckInitialDelay, healthCheckTimeout),
                createExecProbe(healthCheckPath, healthCheckInitialDelay, healthCheckTimeout),
                annotations);
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(buildEnvVar(KEY_ZOOKEEPER_NODE_COUNT, Integer.toString(replicas)));
        varList.add(buildEnvVar(KEY_ZOOKEEPER_METRICS_ENABLED, String.valueOf(isMetricsEnabled)));

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

}
