/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.extensions.NetworkPolicy;
import io.fabric8.kubernetes.api.model.extensions.NetworkPolicyBuilder;
import io.fabric8.kubernetes.api.model.extensions.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.extensions.NetworkPolicyIngressRuleBuilder;
import io.fabric8.kubernetes.api.model.extensions.NetworkPolicyPeer;
import io.fabric8.kubernetes.api.model.extensions.NetworkPolicyPort;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.strimzi.api.kafka.model.EphemeralStorage;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.Logging;
import io.strimzi.api.kafka.model.PersistentClaimStorage;
import io.strimzi.api.kafka.model.Resources;
import io.strimzi.api.kafka.model.TlsSidecar;
import io.strimzi.api.kafka.model.TlsSidecarLogLevel;
import io.strimzi.api.kafka.model.ZookeeperClusterSpec;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.common.model.Labels;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

public class ZookeeperCluster extends AbstractModel {

    protected static final int CLIENT_PORT = 2181;
    protected static final String CLIENT_PORT_NAME = "clients";
    protected static final int CLUSTERING_PORT = 2888;
    protected static final String CLUSTERING_PORT_NAME = "clustering";
    protected static final int LEADER_ELECTION_PORT = 3888;
    protected static final String LEADER_ELECTION_PORT_NAME = "leader-election";

    protected static final String ZOOKEEPER_NAME = "zookeeper";
    protected static final String TLS_SIDECAR_NAME = "tls-sidecar";
    protected static final String TLS_SIDECAR_NODES_VOLUME_NAME = "zookeeper-nodes";
    protected static final String TLS_SIDECAR_NODES_VOLUME_MOUNT = "/etc/tls-sidecar/zookeeper-nodes/";
    protected static final String TLS_SIDECAR_CLUSTER_CA_VOLUME_NAME = "cluster-ca-certs";
    protected static final String TLS_SIDECAR_CLUSTER_CA_VOLUME_MOUNT = "/etc/tls-sidecar/cluster-ca-certs/";
    private static final String NAME_SUFFIX = "-zookeeper";
    private static final String SERVICE_NAME_SUFFIX = NAME_SUFFIX + "-client";
    private static final String HEADLESS_SERVICE_NAME_SUFFIX = NAME_SUFFIX + "-nodes";
    private static final String METRICS_AND_LOG_CONFIG_SUFFIX = NAME_SUFFIX + "-config";
    private static final String LOG_CONFIG_SUFFIX = NAME_SUFFIX + "-logging";
    private static final String NODES_CERTS_SUFFIX = NAME_SUFFIX + "-nodes";

    // Zookeeper configuration
    private TlsSidecar tlsSidecar;

    // Configuration defaults
    private static final int DEFAULT_HEALTHCHECK_DELAY = 15;
    private static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;
    private static final boolean DEFAULT_ZOOKEEPER_METRICS_ENABLED = false;

    // Zookeeper configuration keys (EnvVariables)
    public static final String ENV_VAR_ZOOKEEPER_NODE_COUNT = "ZOOKEEPER_NODE_COUNT";
    public static final String ENV_VAR_ZOOKEEPER_METRICS_ENABLED = "ZOOKEEPER_METRICS_ENABLED";
    public static final String ENV_VAR_ZOOKEEPER_CONFIGURATION = "ZOOKEEPER_CONFIGURATION";
    public static final String ENV_VAR_ZOOKEEPER_LOG_CONFIGURATION = "ZOOKEEPER_LOG_CONFIGURATION";
    public static final String ENV_VAR_TLS_SIDECAR_LOG_LEVEL = "TLS_SIDECAR_LOG_LEVEL";

    public static String zookeeperClusterName(String cluster) {
        return cluster + ZookeeperCluster.NAME_SUFFIX;
    }

    public static String zookeeperMetricAndLogConfigsName(String cluster) {
        return cluster + ZookeeperCluster.METRICS_AND_LOG_CONFIG_SUFFIX;
    }

    public static String logConfigsName(String cluster) {
        return cluster + ZookeeperCluster.LOG_CONFIG_SUFFIX;
    }

    public static String serviceName(String cluster) {
        return cluster + ZookeeperCluster.SERVICE_NAME_SUFFIX;
    }

    public static String headlessServiceName(String cluster) {
        return cluster + ZookeeperCluster.HEADLESS_SERVICE_NAME_SUFFIX;
    }

    public static String zookeeperPodName(String cluster, int pod) {
        return zookeeperClusterName(cluster) + "-" + pod;
    }

    public static String getPersistentVolumeClaimName(String clusterName, int podId) {
        return VOLUME_NAME + "-" + clusterName + "-" + podId;
    }

    public static String nodesSecretName(String cluster) {
        return cluster + ZookeeperCluster.NODES_CERTS_SUFFIX;
    }

    /**
     * Constructor
     *
     * @param namespace Kubernetes/OpenShift namespace where Zookeeper cluster resources are going to be created
     * @param cluster   overall cluster name
     * @param labels    labels to add to the cluster
     */
    private ZookeeperCluster(String namespace, String cluster, Labels labels) {

        super(namespace, cluster, labels);
        this.name = zookeeperClusterName(cluster);
        this.serviceName = serviceName(cluster);
        this.headlessServiceName = headlessServiceName(cluster);
        this.ancillaryConfigName = zookeeperMetricAndLogConfigsName(cluster);
        this.image = ZookeeperClusterSpec.DEFAULT_IMAGE;
        this.replicas = ZookeeperClusterSpec.DEFAULT_REPLICAS;
        this.readinessPath = "/opt/kafka/zookeeper_healthcheck.sh";
        this.readinessTimeout = DEFAULT_HEALTHCHECK_TIMEOUT;
        this.readinessInitialDelay = DEFAULT_HEALTHCHECK_DELAY;
        this.livenessPath = "/opt/kafka/zookeeper_healthcheck.sh";
        this.livenessTimeout = DEFAULT_HEALTHCHECK_TIMEOUT;
        this.livenessInitialDelay = DEFAULT_HEALTHCHECK_DELAY;
        this.isMetricsEnabled = DEFAULT_ZOOKEEPER_METRICS_ENABLED;

        this.mountPath = "/var/lib/zookeeper";

        this.logAndMetricsConfigVolumeName = "zookeeper-metrics-and-logging";
        this.logAndMetricsConfigMountPath = "/opt/kafka/custom-config/";
        this.validLoggerFields = getDefaultLogConfig();
    }

    public static ZookeeperCluster fromCrd(Kafka kafkaAssembly) {
        ZookeeperCluster zk = new ZookeeperCluster(kafkaAssembly.getMetadata().getNamespace(), kafkaAssembly.getMetadata().getName(),
                Labels.fromResource(kafkaAssembly).withKind(kafkaAssembly.getKind()));
        zk.setOwnerReference(kafkaAssembly);
        ZookeeperClusterSpec zookeeperClusterSpec = kafkaAssembly.getSpec().getZookeeper();
        int replicas = zookeeperClusterSpec.getReplicas();
        if (replicas <= 0) {
            replicas = ZookeeperClusterSpec.DEFAULT_REPLICAS;
        }
        zk.setReplicas(replicas);
        String image = zookeeperClusterSpec.getImage();
        if (image == null) {
            image = ZookeeperClusterSpec.DEFAULT_IMAGE;
        }
        zk.setImage(image);
        if (zookeeperClusterSpec.getReadinessProbe() != null) {
            zk.setReadinessInitialDelay(zookeeperClusterSpec.getReadinessProbe().getInitialDelaySeconds());
            zk.setReadinessTimeout(zookeeperClusterSpec.getReadinessProbe().getTimeoutSeconds());
        }
        if (zookeeperClusterSpec.getLivenessProbe() != null) {
            zk.setLivenessInitialDelay(zookeeperClusterSpec.getLivenessProbe().getInitialDelaySeconds());
            zk.setLivenessTimeout(zookeeperClusterSpec.getLivenessProbe().getTimeoutSeconds());
        }
        Logging logging = zookeeperClusterSpec.getLogging();
        zk.setLogging(logging == null ? new InlineLogging() : logging);
        Map<String, Object> metrics = zookeeperClusterSpec.getMetrics();
        if (metrics != null && !metrics.isEmpty()) {
            zk.setMetricsEnabled(true);
            zk.setMetricsConfig(metrics.entrySet());
        }
        zk.setStorage(zookeeperClusterSpec.getStorage());
        zk.setConfiguration(new ZookeeperConfiguration(zookeeperClusterSpec.getConfig().entrySet()));
        zk.setResources(zookeeperClusterSpec.getResources());
        zk.setJvmOptions(zookeeperClusterSpec.getJvmOptions());
        zk.setUserAffinity(zookeeperClusterSpec.getAffinity());
        zk.setTolerations(zookeeperClusterSpec.getTolerations());
        zk.setTlsSidecar(zookeeperClusterSpec.getTlsSidecar());
        return zk;
    }

    public Service generateService() {
        List<ServicePort> ports = new ArrayList<>(2);
        if (isMetricsEnabled()) {
            ports.add(createServicePort(METRICS_PORT_NAME, METRICS_PORT, METRICS_PORT, "TCP"));
        }
        ports.add(createServicePort(CLIENT_PORT_NAME, CLIENT_PORT, CLIENT_PORT, "TCP"));

        return createService("ClusterIP", ports, getPrometheusAnnotations());
    }

    public static String policyName(String cluster) {
        return cluster + NETWORK_POLICY_KEY_SUFFIX + NAME_SUFFIX;
    }

    public NetworkPolicy generateNetworkPolicy() {
        NetworkPolicyPort port1 = new NetworkPolicyPort();
        port1.setPort(new IntOrString(CLIENT_PORT));

        NetworkPolicyPort port2 = new NetworkPolicyPort();
        port2.setPort(new IntOrString(CLUSTERING_PORT));

        NetworkPolicyPort port3 = new NetworkPolicyPort();
        port3.setPort(new IntOrString(LEADER_ELECTION_PORT));

        NetworkPolicyPeer kafkaClusterPeer = new NetworkPolicyPeer();
        LabelSelector labelSelector = new LabelSelector();
        Map<String, String> expressions = new HashMap<>();
        expressions.put(Labels.STRIMZI_NAME_LABEL, KafkaCluster.kafkaClusterName(cluster));
        labelSelector.setMatchLabels(expressions);
        kafkaClusterPeer.setPodSelector(labelSelector);

        NetworkPolicyPeer zookeeperClusterPeer = new NetworkPolicyPeer();
        LabelSelector labelSelector2 = new LabelSelector();
        Map<String, String> expressions2 = new HashMap<>();
        expressions2.put(Labels.STRIMZI_NAME_LABEL, zookeeperClusterName(cluster));
        labelSelector2.setMatchLabels(expressions2);
        zookeeperClusterPeer.setPodSelector(labelSelector2);

        NetworkPolicyPeer entityOperatorPeer = new NetworkPolicyPeer();
        LabelSelector labelSelector3 = new LabelSelector();
        Map<String, String> expressions3 = new HashMap<>();
        expressions3.put(Labels.STRIMZI_NAME_LABEL, EntityOperator.entityOperatorName(cluster));
        labelSelector3.setMatchLabels(expressions3);
        entityOperatorPeer.setPodSelector(labelSelector3);

        NetworkPolicyIngressRule networkPolicyIngressRule = new NetworkPolicyIngressRuleBuilder()
                .withPorts(port1, port2, port3)
                .withFrom(kafkaClusterPeer, zookeeperClusterPeer, entityOperatorPeer)
                .build();

        NetworkPolicy networkPolicy = new NetworkPolicyBuilder()
                .withNewMetadata()
                    .withName(policyName(cluster))
                    .withNamespace(namespace)
                    .withLabels(labels.toMap())
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withNewSpec()
                    .withPodSelector(labelSelector2)
                    .withIngress(networkPolicyIngressRule)
                .endSpec()
                .build();

        log.trace("Created network policy {}", networkPolicy);
        return networkPolicy;
    }

    public Service generateHeadlessService() {
        Map<String, String> annotations = Collections.singletonMap("service.alpha.kubernetes.io/tolerate-unready-endpoints", "true");
        return createHeadlessService(getServicePortList(), annotations);
    }

    public StatefulSet generateStatefulSet(boolean isOpenShift) {

        return createStatefulSet(
                getVolumes(),
                getVolumeClaims(),
                getVolumeMounts(),
                getMergedAffinity(),
                getInitContainers(),
                getContainers(),
                isOpenShift);
    }

    /**
     * Generate the Secret containing CA self-signed certificate for internal communication.
     * It also contains the private key-certificate (signed by internal CA) for each brokers for communicating
     * internally within the cluster
     * @return The generated Secret
     */
    public Secret generateNodesSecret(ClusterCa clusterCa, Kafka kafka) {

        Map<String, String> data = new HashMap<>();

        log.debug("Generating certificates");
        Map<String, CertAndKey> certs;
        try {
            log.debug("Cluster communication certificates");
            certs = clusterCa.generateZkCerts(kafka);
            log.debug("End generating certificates");
            for (int i = 0; i < replicas; i++) {
                CertAndKey cert = certs.get(ZookeeperCluster.zookeeperPodName(cluster, i));
                data.put(ZookeeperCluster.zookeeperPodName(cluster, i) + ".key", cert.keyAsBase64String());
                data.put(ZookeeperCluster.zookeeperPodName(cluster, i) + ".crt", cert.certAsBase64String());
            }

        } catch (IOException e) {
            log.warn("Error while generating certificates", e);
        }

        return createSecret(ZookeeperCluster.nodesSecretName(cluster), data);
    }

    @Override
    protected List<Container> getContainers() {

        List<Container> containers = new ArrayList<>();

        Container container = new ContainerBuilder()
                .withName(ZOOKEEPER_NAME)
                .withImage(getImage())
                .withEnv(getEnvVars())
                .withVolumeMounts(getVolumeMounts())
                .withPorts(getContainerPortList())
                .withLivenessProbe(createExecProbe(livenessPath, livenessInitialDelay, livenessTimeout))
                .withReadinessProbe(createExecProbe(readinessPath, readinessInitialDelay, readinessTimeout))
                .withResources(resources(getResources()))
                .build();

        String tlsSidecarImage = (tlsSidecar != null && tlsSidecar.getImage() != null) ?
                tlsSidecar.getImage() : ZookeeperClusterSpec.DEFAULT_TLS_SIDECAR_IMAGE;

        Resources tlsSidecarResources = (tlsSidecar != null) ? tlsSidecar.getResources() : null;

        TlsSidecarLogLevel tlsSidecarLogLevel = (tlsSidecar != null) ? tlsSidecar.getLogLevel() : TlsSidecarLogLevel.NOTICE;

        Container tlsSidecarContainer = new ContainerBuilder()
                .withName(TLS_SIDECAR_NAME)
                .withImage(tlsSidecarImage)
                .withResources(resources(tlsSidecarResources))
                .withEnv(asList(buildEnvVar(ENV_VAR_TLS_SIDECAR_LOG_LEVEL, tlsSidecarLogLevel.toValue()),
                        buildEnvVar(ENV_VAR_ZOOKEEPER_NODE_COUNT, Integer.toString(replicas))))
                .withVolumeMounts(createVolumeMount(TLS_SIDECAR_NODES_VOLUME_NAME, TLS_SIDECAR_NODES_VOLUME_MOUNT),
                        createVolumeMount(TLS_SIDECAR_CLUSTER_CA_VOLUME_NAME, TLS_SIDECAR_CLUSTER_CA_VOLUME_MOUNT))
                .withPorts(asList(createContainerPort(CLUSTERING_PORT_NAME, CLUSTERING_PORT, "TCP"),
                                createContainerPort(LEADER_ELECTION_PORT_NAME, LEADER_ELECTION_PORT, "TCP"),
                                createContainerPort(CLIENT_PORT_NAME, CLIENT_PORT, "TCP")))
                .build();

        containers.add(container);
        containers.add(tlsSidecarContainer);

        return containers;
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
        if (isMetricsEnabled) {
            portList.add(createContainerPort(METRICS_PORT_NAME, METRICS_PORT, "TCP"));
        }

        return portList;
    }

    private List<Volume> getVolumes() {
        List<Volume> volumeList = new ArrayList<>();
        if (storage instanceof EphemeralStorage) {
            volumeList.add(createEmptyDirVolume(VOLUME_NAME));
        }
        volumeList.add(createConfigMapVolume(logAndMetricsConfigVolumeName, ancillaryConfigName));
        volumeList.add(createSecretVolume(TLS_SIDECAR_NODES_VOLUME_NAME, ZookeeperCluster.nodesSecretName(cluster)));
        volumeList.add(createSecretVolume(TLS_SIDECAR_CLUSTER_CA_VOLUME_NAME, AbstractModel.getClusterCaName(cluster)));
        return volumeList;
    }

    private List<PersistentVolumeClaim> getVolumeClaims() {
        List<PersistentVolumeClaim> pvcList = new ArrayList<>();
        if (storage instanceof PersistentClaimStorage) {
            pvcList.add(createPersistentVolumeClaim(VOLUME_NAME));
        }
        return pvcList;
    }

    private List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> volumeMountList = new ArrayList<>();
        volumeMountList.add(createVolumeMount(VOLUME_NAME, mountPath));
        volumeMountList.add(createVolumeMount(logAndMetricsConfigVolumeName, logAndMetricsConfigMountPath));

        return volumeMountList;
    }

    protected void setTlsSidecar(TlsSidecar tlsSidecar) {
        this.tlsSidecar = tlsSidecar;
    }

    @Override
    protected String getDefaultLogConfigFileName() {
        return "zookeeperDefaultLoggingProperties";
    }
}
