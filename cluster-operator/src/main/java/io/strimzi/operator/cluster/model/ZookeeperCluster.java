/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.strimzi.api.kafka.model.EphemeralStorage;
import io.strimzi.api.kafka.model.KafkaAssembly;
import io.strimzi.api.kafka.model.PersistentClaimStorage;
import io.strimzi.api.kafka.model.Resources;
import io.strimzi.api.kafka.model.Sidecar;
import io.strimzi.api.kafka.model.Zookeeper;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.CertManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class ZookeeperCluster extends AbstractModel {

    protected static final int CLIENT_PORT = 2181;
    protected static final String CLIENT_PORT_NAME = "clients";
    protected static final int CLUSTERING_PORT = 2888;
    protected static final String CLUSTERING_PORT_NAME = "clustering";
    protected static final int LEADER_ELECTION_PORT = 3888;
    protected static final String LEADER_ELECTION_PORT_NAME = "leader-election";
    protected static final int METRICS_PORT = 9404;
    protected static final String METRICS_PORT_NAME = "metrics";

    protected static final String ZOOKEEPER_NAME = "zookeeper";
    protected static final String TLS_SIDECAR_NAME = "tls-sidecar";
    protected static final String TLS_SIDECAR_VOLUME_NAME = "tls-sidecar-certs";
    protected static final String TLS_SIDECAR_VOLUME_MOUNT = "/etc/tls-sidecar/certs/";
    private static final String NAME_SUFFIX = "-zookeeper";
    private static final String SERVICE_NAME_SUFFIX = NAME_SUFFIX + "-client";
    private static final String HEADLESS_SERVICE_NAME_SUFFIX = NAME_SUFFIX + "-nodes";
    private static final String METRICS_AND_LOG_CONFIG_SUFFIX = NAME_SUFFIX + "-config";
    private static final String LOG_CONFIG_SUFFIX = NAME_SUFFIX + "-logging";
    private static final String NODES_CERTS_SUFFIX = NAME_SUFFIX + "-nodes";

    // Zookeeper configuration
    private Sidecar tlsSidecar;

    // Configuration defaults
    private static final int DEFAULT_HEALTHCHECK_DELAY = 15;
    private static final int DEFAULT_HEALTHCHECK_TIMEOUT = 5;
    private static final boolean DEFAULT_ZOOKEEPER_METRICS_ENABLED = false;

    // Zookeeper configuration keys (EnvVariables)
    public static final String ENV_VAR_ZOOKEEPER_NODE_COUNT = "ZOOKEEPER_NODE_COUNT";
    public static final String ENV_VAR_ZOOKEEPER_METRICS_ENABLED = "ZOOKEEPER_METRICS_ENABLED";
    public static final String ENV_VAR_ZOOKEEPER_CONFIGURATION = "ZOOKEEPER_CONFIGURATION";
    public static final String ENV_VAR_ZOOKEEPER_LOG_CONFIGURATION = "ZOOKEEPER_LOG_CONFIGURATION";

    /**
     * Private key and certificate for each Zookeeper Pod name
     * used for for encrypting the communication in the Zookeeper ensemble
     */
    private Map<String, CertAndKey> certs;

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
     */
    private ZookeeperCluster(String namespace, String cluster, Labels labels) {

        super(namespace, cluster, labels);
        this.name = zookeeperClusterName(cluster);
        this.serviceName = serviceName(cluster);
        this.headlessServiceName = headlessServiceName(cluster);
        this.ancillaryConfigName = zookeeperMetricAndLogConfigsName(cluster);
        this.image = Zookeeper.DEFAULT_IMAGE;
        this.replicas = Zookeeper.DEFAULT_REPLICAS;
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

    public static ZookeeperCluster fromCrd(CertManager certManager, KafkaAssembly kafkaAssembly, List<Secret> secrets) {
        ZookeeperCluster zk = new ZookeeperCluster(kafkaAssembly.getMetadata().getNamespace(), kafkaAssembly.getMetadata().getName(),
                Labels.fromResource(kafkaAssembly).withKind(kafkaAssembly.getKind()));
        Zookeeper zookeeper = kafkaAssembly.getSpec().getZookeeper();
        int replicas = zookeeper.getReplicas();
        if (replicas <= 0) {
            replicas = Zookeeper.DEFAULT_REPLICAS;
        }
        zk.setReplicas(replicas);
        String image = zookeeper.getImage();
        if (image == null) {
            image = Zookeeper.DEFAULT_IMAGE;
        }
        zk.setImage(image);
        if (zookeeper.getReadinessProbe() != null) {
            zk.setReadinessInitialDelay(zookeeper.getReadinessProbe().getInitialDelaySeconds());
            zk.setReadinessTimeout(zookeeper.getReadinessProbe().getTimeoutSeconds());
        }
        if (zookeeper.getLivenessProbe() != null) {
            zk.setLivenessInitialDelay(zookeeper.getLivenessProbe().getInitialDelaySeconds());
            zk.setLivenessTimeout(zookeeper.getLivenessProbe().getTimeoutSeconds());
        }
        zk.setLogging(zookeeper.getLogging());
        Map<String, Object> metrics = zookeeper.getMetrics();
        if (metrics != null && !metrics.isEmpty()) {
            zk.setMetricsEnabled(true);
            zk.setMetricsConfig(metrics.entrySet());
        }
        zk.setStorage(zookeeper.getStorage());
        zk.setConfiguration(new ZookeeperConfiguration(zookeeper.getConfig().entrySet()));
        zk.setResources(zookeeper.getResources());
        zk.setJvmOptions(zookeeper.getJvmOptions());
        zk.setUserAffinity(zookeeper.getAffinity());
        zk.setTolerations(zookeeper.getTolerations());
        zk.generateCertificates(certManager, secrets);
        zk.setTlsSidecar(zookeeper.getTlsSidecar());
        return zk;
    }

    /**
     * Manage certificates generation based on those already present in the Secrets
     *
     * @param certManager CertManager instance for handling certificates creation
     * @param secrets The Secrets storing certificates
     */
    public void generateCertificates(CertManager certManager, List<Secret> secrets) {
        log.debug("Generating certificates");

        try {
            Optional<Secret> internalCAsecret = secrets.stream().filter(s -> s.getMetadata().getName().equals(getClusterCaName(cluster)))
                    .findFirst();
            if (internalCAsecret.isPresent()) {

                // get the generated CA private key + self-signed certificate for internal communications
                clusterCA = new CertAndKey(
                        decodeFromSecret(internalCAsecret.get(), "cluster-ca.key"),
                        decodeFromSecret(internalCAsecret.get(), "cluster-ca.crt"));

                // recover or generates the private key + certificate for each node
                Optional<Secret> nodesSecret = secrets.stream().filter(s -> s.getMetadata().getName().equals(ZookeeperCluster.nodesSecretName(cluster)))
                        .findFirst();

                int replicasSecret = !nodesSecret.isPresent() ? 0 : (nodesSecret.get().getData().size() - 1) / 2;

                log.debug("Cluster communication certificates");
                certs = maybeCopyOrGenerateCerts(certManager, nodesSecret, replicasSecret, clusterCA, ZookeeperCluster::zookeeperPodName);
            } else {
                throw new NoCertificateSecretException("The cluster CA certificate Secret is missing");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        log.debug("End generating certificates");
    }

    public Service generateService() {
        List<ServicePort> ports = new ArrayList<>(2);
        if (isMetricsEnabled()) {
            ports.add(createServicePort(METRICS_PORT_NAME, METRICS_PORT, METRICS_PORT, "TCP"));
        }
        ports.add(createServicePort(CLIENT_PORT_NAME, CLIENT_PORT, CLIENT_PORT, "TCP"));

        return createService("ClusterIP", ports, getPrometheusAnnotations());
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
    public Secret generateNodesSecret() {
        Base64.Encoder encoder = Base64.getEncoder();

        Map<String, String> data = new HashMap<>();
        data.put("cluster-ca.crt", encoder.encodeToString(clusterCA.cert()));

        for (int i = 0; i < replicas; i++) {
            CertAndKey cert = certs.get(ZookeeperCluster.zookeeperPodName(cluster, i));
            data.put(ZookeeperCluster.zookeeperPodName(cluster, i) + ".key", encoder.encodeToString(cert.key()));
            data.put(ZookeeperCluster.zookeeperPodName(cluster, i) + ".crt", encoder.encodeToString(cert.cert()));
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
                tlsSidecar.getImage() : Zookeeper.DEFAULT_TLS_SIDECAR_IMAGE;

        Resources tlsSidecarResources = (tlsSidecar != null) ? tlsSidecar.getResources() : null;

        Container tlsSidecarContainer = new ContainerBuilder()
                .withName(TLS_SIDECAR_NAME)
                .withImage(tlsSidecarImage)
                .withResources(resources(tlsSidecarResources))
                .withEnv(singletonList(buildEnvVar(ENV_VAR_ZOOKEEPER_NODE_COUNT, Integer.toString(replicas))))
                .withVolumeMounts(createVolumeMount(TLS_SIDECAR_VOLUME_NAME, TLS_SIDECAR_VOLUME_MOUNT))
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
        if (getLogging() != null && getLogging().getCm() != null) {
            varList.add(buildEnvVar(ENV_VAR_ZOOKEEPER_LOG_CONFIGURATION, getLogging().getCm().toString()));
        }
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
            portList.add(createContainerPort(metricsPortName, metricsPort, "TCP"));
        }

        return portList;
    }

    private List<Volume> getVolumes() {
        List<Volume> volumeList = new ArrayList<>();
        if (storage instanceof EphemeralStorage) {
            volumeList.add(createEmptyDirVolume(VOLUME_NAME));
        }
        volumeList.add(createConfigMapVolume(logAndMetricsConfigVolumeName, ancillaryConfigName));
        volumeList.add(createSecretVolume(TLS_SIDECAR_VOLUME_NAME, ZookeeperCluster.nodesSecretName(cluster)));
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

    protected void setTlsSidecar(Sidecar tlsSidecar) {
        this.tlsSidecar = tlsSidecar;
    }

    @Override
    protected String getDefaultLogConfigFileName() {
        return "zookeeperDefaultLoggingProperties";
    }
}
