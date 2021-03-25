/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRuleBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeer;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPort;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.Logging;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.ZookeeperClusterSpec;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.storage.EphemeralStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.api.kafka.model.template.ZookeeperClusterTemplate;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.common.MetricsAndLogging;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.StatusUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZookeeperCluster extends AbstractModel {
    protected static final String APPLICATION_NAME = "zookeeper";

    public static final int CLIENT_PLAINTEXT_PORT = 12181; // This port is internal only, not exposed => no need for name
    public static final int CLIENT_TLS_PORT = 2181;
    protected static final String CLIENT_TLS_PORT_NAME = "tcp-clients";
    public static final int CLUSTERING_PORT = 2888;
    protected static final String CLUSTERING_PORT_NAME = "tcp-clustering";
    public static final int LEADER_ELECTION_PORT = 3888;
    protected static final String LEADER_ELECTION_PORT_NAME = "tcp-election";

    public static final String ZOOKEEPER_NAME = "zookeeper";
    protected static final String ZOOKEEPER_NODE_CERTIFICATES_VOLUME_NAME = "zookeeper-nodes";
    protected static final String ZOOKEEPER_NODE_CERTIFICATES_VOLUME_MOUNT = "/opt/kafka/zookeeper-node-certs/";
    protected static final String ZOOKEEPER_CLUSTER_CA_VOLUME_NAME = "cluster-ca-certs";
    protected static final String ZOOKEEPER_CLUSTER_CA_VOLUME_MOUNT = "/opt/kafka/cluster-ca-certs/";
    private static final String NAME_SUFFIX = "-zookeeper";
    private static final String SERVICE_NAME_SUFFIX = NAME_SUFFIX + "-client";
    private static final String HEADLESS_SERVICE_NAME_SUFFIX = NAME_SUFFIX + "-nodes";
    private static final String NODES_CERTS_SUFFIX = NAME_SUFFIX + "-nodes";

    // Zookeeper configuration
    private boolean isSnapshotCheckEnabled;
    private String version;

    public static final Probe DEFAULT_HEALTHCHECK_OPTIONS = new ProbeBuilder()
            .withTimeoutSeconds(5)
            .withInitialDelaySeconds(15)
            .build();
    private static final boolean DEFAULT_ZOOKEEPER_METRICS_ENABLED = false;
    private static final boolean DEFAULT_ZOOKEEPER_SNAPSHOT_CHECK_ENABLED = true;

    // Zookeeper configuration keys (EnvVariables)
    public static final String ENV_VAR_ZOOKEEPER_METRICS_ENABLED = "ZOOKEEPER_METRICS_ENABLED";
    public static final String ENV_VAR_ZOOKEEPER_CONFIGURATION = "ZOOKEEPER_CONFIGURATION";
    public static final String ENV_VAR_ZOOKEEPER_SNAPSHOT_CHECK_ENABLED = "ZOOKEEPER_SNAPSHOT_CHECK_ENABLED";

    // Config map keys
    public static final String CONFIG_MAP_KEY_ZOOKEEPER_NODE_COUNT = "zookeeper.node-count";

    // Templates
    protected List<ContainerEnvVar> templateZookeeperContainerEnvVars;
    protected SecurityContext templateZookeeperContainerSecurityContext;

    /**
     * Private key and certificate for each ZooKeeper Pod name
     * used as server certificates for ZooKeeper nodes
     */
    private Map<String, CertAndKey> nodeCerts;

    public static String zookeeperClusterName(String cluster) {
        return KafkaResources.zookeeperStatefulSetName(cluster);
    }

    public static String zookeeperMetricAndLogConfigsName(String cluster) {
        return KafkaResources.zookeeperMetricsAndLogConfigMapName(cluster);
    }

    public static String serviceName(String cluster) {
        return cluster + ZookeeperCluster.SERVICE_NAME_SUFFIX;
    }

    public static String headlessServiceName(String cluster) {
        return cluster + ZookeeperCluster.HEADLESS_SERVICE_NAME_SUFFIX;
    }

    /**
     * Generates the DNS name of the pod including the cluster suffix
     * (i.e. usually with the cluster.local - but can be different on different clusters)
     * Example: my-cluster-zookeeper-1.my-cluster-zookeeper-nodes.svc.cluster.local
     *
     * @param namespace     Namespace of the pod
     * @param cluster       Name of the cluster
     * @param podId         Id of the pod within the STS
     *
     * @return              DNS name of the pod
     */
    public static String podDnsName(String namespace, String cluster, int podId) {
        return DnsNameGenerator.podDnsName(
                namespace,
                ZookeeperCluster.headlessServiceName(cluster),
                ZookeeperCluster.zookeeperPodName(cluster, podId));
    }

    /**
     * Generates the full DNS name of the pod without the cluster suffix
     * (i.e. usually without the cluster.local - but can be different on different clusters)
     * Example: my-cluster-zookeeper-1.my-cluster-zookeeper-nodes.svc
     *
     * @param namespace     Namespace of the pod
     * @param cluster       Name of the cluster
     * @param podId         Id of the pod within the STS
     *
     * @return              DNS name of the pod without the cluster domain suffix
     */
    public static String podDnsNameWithoutSuffix(String namespace, String cluster, int podId) {
        return DnsNameGenerator.podDnsNameWithoutClusterDomain(
                namespace,
                ZookeeperCluster.headlessServiceName(cluster),
                ZookeeperCluster.zookeeperPodName(cluster, podId));
    }

    public static String zookeeperPodName(String cluster, int pod) {
        return KafkaResources.zookeeperPodName(cluster, pod);
    }

    public static String nodesSecretName(String cluster) {
        return cluster + ZookeeperCluster.NODES_CERTS_SUFFIX;
    }

    /**
     * Constructor
     *
     * @param resource Kubernetes resource with metadata containing the namespace and cluster name
     */
    private ZookeeperCluster(HasMetadata resource) {
        super(resource, APPLICATION_NAME);
        this.name = zookeeperClusterName(cluster);
        this.serviceName = serviceName(cluster);
        this.headlessServiceName = headlessServiceName(cluster);
        this.ancillaryConfigMapName = zookeeperMetricAndLogConfigsName(cluster);
        this.image = null;
        this.replicas = ZookeeperClusterSpec.DEFAULT_REPLICAS;
        this.readinessPath = "/opt/kafka/zookeeper_healthcheck.sh";
        this.readinessProbeOptions = DEFAULT_HEALTHCHECK_OPTIONS;
        this.livenessPath = "/opt/kafka/zookeeper_healthcheck.sh";
        this.livenessProbeOptions = DEFAULT_HEALTHCHECK_OPTIONS;
        this.isMetricsEnabled = DEFAULT_ZOOKEEPER_METRICS_ENABLED;
        this.isSnapshotCheckEnabled = DEFAULT_ZOOKEEPER_SNAPSHOT_CHECK_ENABLED;

        this.mountPath = "/var/lib/zookeeper";

        this.logAndMetricsConfigVolumeName = "zookeeper-metrics-and-logging";
        this.logAndMetricsConfigMountPath = "/opt/kafka/custom-config/";
    }

    public static ZookeeperCluster fromCrd(Kafka kafkaAssembly, KafkaVersion.Lookup versions) {
        return fromCrd(kafkaAssembly, versions, null, 0);
    }

    @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:CyclomaticComplexity", "deprecation"})
    public static ZookeeperCluster fromCrd(Kafka kafkaAssembly, KafkaVersion.Lookup versions, Storage oldStorage, int oldReplicas) {
        ZookeeperCluster zk = new ZookeeperCluster(kafkaAssembly);
        zk.setOwnerReference(kafkaAssembly);
        ZookeeperClusterSpec zookeeperClusterSpec = kafkaAssembly.getSpec().getZookeeper();

        int replicas = zookeeperClusterSpec.getReplicas();
        if (replicas <= 0) {
            replicas = ZookeeperClusterSpec.DEFAULT_REPLICAS;
        }
        if (replicas == 1 && zookeeperClusterSpec.getStorage() != null && "ephemeral".equals(zookeeperClusterSpec.getStorage().getType())) {
            log.warn("A ZooKeeper cluster with a single replica and ephemeral storage will be in a defective state after any restart or rolling update. It is recommended that a minimum of three replicas are used.");
        }
        zk.setReplicas(replicas);

        // Get the ZK version information from either the CRD or from the default setting
        KafkaClusterSpec kafkaClusterSpec = kafkaAssembly.getSpec().getKafka();
        String version = versions.version(kafkaClusterSpec != null ? kafkaClusterSpec.getVersion() : null).zookeeperVersion();
        zk.setVersion(version);

        String image = zookeeperClusterSpec.getImage();
        if (image == null) {
            image = versions.kafkaImage(kafkaClusterSpec != null ? kafkaClusterSpec.getImage() : null,
                    kafkaClusterSpec != null ? kafkaClusterSpec.getVersion() : null);
        }
        zk.setImage(image);

        if (zookeeperClusterSpec.getReadinessProbe() != null) {
            zk.setReadinessProbe(zookeeperClusterSpec.getReadinessProbe());
        }
        if (zookeeperClusterSpec.getLivenessProbe() != null) {
            zk.setLivenessProbe(zookeeperClusterSpec.getLivenessProbe());
        }

        Logging logging = zookeeperClusterSpec.getLogging();
        zk.setLogging(logging == null ? new InlineLogging() : logging);
        zk.setGcLoggingEnabled(zookeeperClusterSpec.getJvmOptions() == null ? DEFAULT_JVM_GC_LOGGING_ENABLED : zookeeperClusterSpec.getJvmOptions().isGcLoggingEnabled());
        if (zookeeperClusterSpec.getJvmOptions() != null) {
            zk.setJavaSystemProperties(zookeeperClusterSpec.getJvmOptions().getJavaSystemProperties());
        }

        // Parse different types of metrics configurations
        ModelUtils.parseMetrics(zk, zookeeperClusterSpec);

        if (oldStorage != null) {
            Storage newStorage = zookeeperClusterSpec.getStorage();
            AbstractModel.validatePersistentStorage(newStorage);

            StorageDiff diff = new StorageDiff(oldStorage, newStorage, oldReplicas, zookeeperClusterSpec.getReplicas());

            if (!diff.isEmpty()) {
                log.warn("Only the following changes to Zookeeper storage are allowed: " +
                        "changing the deleteClaim flag, " +
                        "changing overrides to nodes which do not exist yet " +
                        "and increasing size of persistent claim volumes (depending on the volume type and used storage class).");
                log.warn("The desired ZooKeeper storage configuration in the custom resource {}/{} contains changes which are not allowed. As " +
                        "a result, all storage changes will be ignored. Use DEBUG level logging for more information " +
                        "about the detected changes.", kafkaAssembly.getMetadata().getNamespace(), kafkaAssembly.getMetadata().getName());

                Condition warning = StatusUtils.buildWarningCondition("ZooKeeperStorage",
                        "The desired ZooKeeper storage configuration contains changes which are not allowed. As a " +
                                "result, all storage changes will be ignored. Use DEBUG level logging for more information " +
                                "about the detected changes.");
                zk.addWarningCondition(warning);

                zk.setStorage(oldStorage);
            } else {
                zk.setStorage(newStorage);
            }
        } else {
            zk.setStorage(zookeeperClusterSpec.getStorage());
        }

        zk.setConfiguration(new ZookeeperConfiguration(zookeeperClusterSpec.getConfig().entrySet()));

        zk.setResources(zookeeperClusterSpec.getResources());

        zk.setJvmOptions(zookeeperClusterSpec.getJvmOptions());

        if (zookeeperClusterSpec.getTemplate() != null) {
            ZookeeperClusterTemplate template = zookeeperClusterSpec.getTemplate();

            if (template.getStatefulset() != null) {
                if (template.getStatefulset().getPodManagementPolicy() != null) {
                    zk.templatePodManagementPolicy = template.getStatefulset().getPodManagementPolicy();
                }

                if (template.getStatefulset().getMetadata() != null) {
                    zk.templateStatefulSetLabels = template.getStatefulset().getMetadata().getLabels();
                    zk.templateStatefulSetAnnotations = template.getStatefulset().getMetadata().getAnnotations();
                }
            }

            ModelUtils.parsePodTemplate(zk, template.getPod());

            if (template.getClientService() != null && template.getClientService().getMetadata() != null)  {
                zk.templateServiceLabels = template.getClientService().getMetadata().getLabels();
                zk.templateServiceAnnotations = template.getClientService().getMetadata().getAnnotations();
            }

            if (template.getNodesService() != null && template.getNodesService().getMetadata() != null)  {
                zk.templateHeadlessServiceLabels = template.getNodesService().getMetadata().getLabels();
                zk.templateHeadlessServiceAnnotations = template.getNodesService().getMetadata().getAnnotations();
            }

            if (template.getPersistentVolumeClaim() != null && template.getPersistentVolumeClaim().getMetadata() != null) {
                zk.templatePersistentVolumeClaimLabels = Util.mergeLabelsOrAnnotations(template.getPersistentVolumeClaim().getMetadata().getLabels(),
                        zk.templateStatefulSetLabels);
                zk.templatePersistentVolumeClaimAnnotations = template.getPersistentVolumeClaim().getMetadata().getAnnotations();
            }

            if (template.getZookeeperContainer() != null && template.getZookeeperContainer().getEnv() != null) {
                zk.templateZookeeperContainerEnvVars = template.getZookeeperContainer().getEnv();
            }

            if (template.getZookeeperContainer() != null && template.getZookeeperContainer().getSecurityContext() != null) {
                zk.templateZookeeperContainerSecurityContext = template.getZookeeperContainer().getSecurityContext();
            }

            ModelUtils.parsePodDisruptionBudgetTemplate(zk, template.getPodDisruptionBudget());
        }

        return zk;
    }

    public Service generateService() {
        List<ServicePort> ports = new ArrayList<>(1);
        ports.add(createServicePort(CLIENT_TLS_PORT_NAME, CLIENT_TLS_PORT, CLIENT_TLS_PORT, "TCP"));

        return createService("ClusterIP", ports, templateServiceAnnotations);
    }

    public static String policyName(String cluster) {
        return cluster + NETWORK_POLICY_KEY_SUFFIX + NAME_SUFFIX;
    }

    /**
     * Generates the NetworkPolicies relevant for ZooKeeper nodes
     *
     * @param operatorNamespace                             Namespace where the Strimzi Cluster Operator runs. Null if not configured.
     * @param operatorNamespaceLabels                       Labels of the namespace where the Strimzi Cluster Operator runs. Null if not configured.
     *
     * @return The network policy.
     */
    public NetworkPolicy generateNetworkPolicy(String operatorNamespace, Labels operatorNamespaceLabels) {
        List<NetworkPolicyIngressRule> rules = new ArrayList<>(2);

        NetworkPolicyPort clientsPort = new NetworkPolicyPort();
        clientsPort.setPort(new IntOrString(CLIENT_TLS_PORT));

        NetworkPolicyPort clusteringPort = new NetworkPolicyPort();
        clusteringPort.setPort(new IntOrString(CLUSTERING_PORT));

        NetworkPolicyPort leaderElectionPort = new NetworkPolicyPort();
        leaderElectionPort.setPort(new IntOrString(LEADER_ELECTION_PORT));

        NetworkPolicyPeer zookeeperClusterPeer = new NetworkPolicyPeer();
        LabelSelector labelSelector2 = new LabelSelector();
        Map<String, String> expressions2 = new HashMap<>(1);
        expressions2.put(Labels.STRIMZI_NAME_LABEL, zookeeperClusterName(cluster));
        labelSelector2.setMatchLabels(expressions2);
        zookeeperClusterPeer.setPodSelector(labelSelector2);

        // Zookeeper only ports - 2888 & 3888 which need to be accessed by the Zookeeper cluster members only
        NetworkPolicyIngressRule zookeeperClusteringIngressRule = new NetworkPolicyIngressRuleBuilder()
                .withPorts(clusteringPort, leaderElectionPort)
                .withFrom(zookeeperClusterPeer)
                .build();

        rules.add(zookeeperClusteringIngressRule);

        // Clients port - needs to be access from outside the Zookeeper cluster as well
        NetworkPolicyIngressRule clientsIngressRule = new NetworkPolicyIngressRuleBuilder()
                .withPorts(clientsPort)
                .withFrom()
                .build();

        NetworkPolicyPeer kafkaClusterPeer = new NetworkPolicyPeer();
        LabelSelector labelSelector = new LabelSelector();
        Map<String, String> expressions = new HashMap<>(1);
        expressions.put(Labels.STRIMZI_NAME_LABEL, KafkaCluster.kafkaClusterName(cluster));
        labelSelector.setMatchLabels(expressions);
        kafkaClusterPeer.setPodSelector(labelSelector);

        NetworkPolicyPeer entityOperatorPeer = new NetworkPolicyPeer();
        LabelSelector labelSelector3 = new LabelSelector();
        Map<String, String> expressions3 = new HashMap<>(1);
        expressions3.put(Labels.STRIMZI_NAME_LABEL, EntityOperator.entityOperatorName(cluster));
        labelSelector3.setMatchLabels(expressions3);
        entityOperatorPeer.setPodSelector(labelSelector3);

        NetworkPolicyPeer clusterOperatorPeer = new NetworkPolicyPeer();
        LabelSelector labelSelector4 = new LabelSelector();
        Map<String, String> expressions4 = new HashMap<>(1);
        expressions4.put(Labels.STRIMZI_KIND_LABEL, "cluster-operator");
        labelSelector4.setMatchLabels(expressions4);
        clusterOperatorPeer.setPodSelector(labelSelector4);
        ModelUtils.setClusterOperatorNetworkPolicyNamespaceSelector(clusterOperatorPeer, namespace, operatorNamespace, operatorNamespaceLabels);

        NetworkPolicyPeer cruiseControlPeer = new NetworkPolicyPeer();
        LabelSelector labelSelector5 = new LabelSelector();
        Map<String, String> expressions5 = new HashMap<>(1);
        expressions5.put(Labels.STRIMZI_NAME_LABEL, CruiseControl.cruiseControlName(cluster));
        labelSelector5.setMatchLabels(expressions5);
        cruiseControlPeer.setPodSelector(labelSelector5);
            
        // This is a hack because we have no guarantee that the CO namespace has some particular labels
        List<NetworkPolicyPeer> clientsPortPeers = new ArrayList<>(4);
        clientsPortPeers.add(kafkaClusterPeer);
        clientsPortPeers.add(zookeeperClusterPeer);
        clientsPortPeers.add(entityOperatorPeer);
        clientsPortPeers.add(clusterOperatorPeer);
        clientsPortPeers.add(cruiseControlPeer);
        clientsIngressRule.setFrom(clientsPortPeers);

        rules.add(clientsIngressRule);

        if (isMetricsEnabled) {
            NetworkPolicyPort metricsPort = new NetworkPolicyPort();
            metricsPort.setPort(new IntOrString(METRICS_PORT));

            NetworkPolicyIngressRule metricsRule = new NetworkPolicyIngressRuleBuilder()
                    .withPorts(metricsPort)
                    .withFrom()
                    .build();

            rules.add(metricsRule);
        }

        NetworkPolicy networkPolicy = new NetworkPolicyBuilder()
                .withNewMetadata()
                    .withName(policyName(cluster))
                    .withNamespace(namespace)
                    .withLabels(labels.toMap())
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withNewSpec()
                    .withPodSelector(labelSelector2)
                    .withIngress(rules)
                .endSpec()
                .build();

        log.trace("Created network policy {}", networkPolicy);
        return networkPolicy;
    }

    public Service generateHeadlessService() {
        return createHeadlessService(getServicePortList());
    }

    public StatefulSet generateStatefulSet(boolean isOpenShift, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets) {

        return createStatefulSet(
                Collections.singletonMap(ANNO_STRIMZI_IO_STORAGE, ModelUtils.encodeStorageToJson(storage)),
                Collections.emptyMap(),
                getVolumes(isOpenShift),
                getVolumeClaims(),
                getMergedAffinity(),
                getInitContainers(imagePullPolicy),
                getContainers(imagePullPolicy),
                imagePullSecrets,
                isOpenShift);
    }

    /**
     * Generates the ZooKeeper nodes certificates
     *
     * @param kafka The Kafka custom resource
     * @param clusterCa The CA for cluster certificates
     * @param isMaintenanceTimeWindowsSatisfied Indicates whether we are in the maintenance window or not.
     *                                          This is used for certificate renewals
     */
    public void generateCertificates(Kafka kafka, ClusterCa clusterCa, boolean isMaintenanceTimeWindowsSatisfied) {
        log.debug("Generating certificates");
        try {
            nodeCerts = clusterCa.generateZkCerts(kafka, isMaintenanceTimeWindowsSatisfied);
        } catch (IOException e) {
            log.warn("Error while generating certificates", e);
        }
        log.debug("End generating certificates");
    }

    /**
     * Generate the Secret containing the Zookeeper nodes certificates signed by the cluster CA certificate used for TLS based
     * internal communication with Kafka.
     * It also contains the related Zookeeper nodes private keys.
     *
     * @return The generated Secret.
     */
    public Secret generateNodesSecret() {

        Map<String, String> data = new HashMap<>(replicas * 4);
        for (int i = 0; i < replicas; i++) {
            CertAndKey cert = nodeCerts.get(ZookeeperCluster.zookeeperPodName(cluster, i));
            data.put(ZookeeperCluster.zookeeperPodName(cluster, i) + ".key", cert.keyAsBase64String());
            data.put(ZookeeperCluster.zookeeperPodName(cluster, i) + ".crt", cert.certAsBase64String());
            data.put(ZookeeperCluster.zookeeperPodName(cluster, i) + ".p12", cert.keyStoreAsBase64String());
            data.put(ZookeeperCluster.zookeeperPodName(cluster, i) + ".password", cert.storePasswordAsBase64String());
        }
        return createSecret(ZookeeperCluster.nodesSecretName(cluster), data);
    }

    @Override
    protected List<Container> getContainers(ImagePullPolicy imagePullPolicy) {

        List<Container> containers = new ArrayList<>(1);

        Container container = new ContainerBuilder()
                .withName(ZOOKEEPER_NAME)
                .withImage(getImage())
                .withCommand("/opt/kafka/zookeeper_run.sh")
                .withEnv(getEnvVars())
                .withVolumeMounts(getVolumeMounts())
                .withPorts(getContainerPortList())
                .withLivenessProbe(ProbeGenerator.execProbe(livenessProbeOptions, Collections.singletonList(livenessPath)))
                .withReadinessProbe(ProbeGenerator.execProbe(readinessProbeOptions, Collections.singletonList(readinessPath)))
                .withResources(getResources())
                .withImagePullPolicy(determineImagePullPolicy(imagePullPolicy, getImage()))
                .withSecurityContext(templateZookeeperContainerSecurityContext)
                .build();

        containers.add(container);

        return containers;
    }

    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(buildEnvVar(ENV_VAR_ZOOKEEPER_METRICS_ENABLED, String.valueOf(isMetricsEnabled)));
        varList.add(buildEnvVar(ENV_VAR_ZOOKEEPER_SNAPSHOT_CHECK_ENABLED, String.valueOf(isSnapshotCheckEnabled)));
        varList.add(buildEnvVar(ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED, String.valueOf(gcLoggingEnabled)));
        if (javaSystemProperties != null) {
            varList.add(buildEnvVar(ENV_VAR_STRIMZI_JAVA_SYSTEM_PROPERTIES, ModelUtils.getJavaSystemPropertiesToString(javaSystemProperties)));
        }

        heapOptions(varList, 0.75, 2L * 1024L * 1024L * 1024L);
        jvmPerformanceOptions(varList);
        varList.add(buildEnvVar(ENV_VAR_ZOOKEEPER_CONFIGURATION, configuration.getConfiguration()));

        // Add shared environment variables used for all containers
        varList.addAll(getRequiredEnvVars());

        addContainerEnvsToExistingEnvs(varList, templateZookeeperContainerEnvVars);

        return varList;
    }

    private List<ServicePort> getServicePortList() {
        List<ServicePort> portList = new ArrayList<>(3);
        portList.add(createServicePort(CLIENT_TLS_PORT_NAME, CLIENT_TLS_PORT, CLIENT_TLS_PORT, "TCP"));
        portList.add(createServicePort(CLUSTERING_PORT_NAME, CLUSTERING_PORT, CLUSTERING_PORT, "TCP"));
        portList.add(createServicePort(LEADER_ELECTION_PORT_NAME, LEADER_ELECTION_PORT, LEADER_ELECTION_PORT, "TCP"));

        return portList;
    }

    private List<ContainerPort> getContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>(4);

        portList.add(createContainerPort(CLUSTERING_PORT_NAME, CLUSTERING_PORT, "TCP"));
        portList.add(createContainerPort(LEADER_ELECTION_PORT_NAME, LEADER_ELECTION_PORT, "TCP"));
        portList.add(createContainerPort(CLIENT_TLS_PORT_NAME, CLIENT_TLS_PORT, "TCP"));

        if (isMetricsEnabled) {
            portList.add(createContainerPort(METRICS_PORT_NAME, METRICS_PORT, "TCP"));
        }

        return portList;
    }

    private List<Volume> getVolumes(boolean isOpenShift) {
        List<Volume> volumeList = new ArrayList<>(5);

        if (storage instanceof EphemeralStorage) {
            String sizeLimit = ((EphemeralStorage) storage).getSizeLimit();
            volumeList.add(VolumeUtils.createEmptyDirVolume(VOLUME_NAME, sizeLimit));
        }

        volumeList.add(createTempDirVolume());
        volumeList.add(VolumeUtils.createConfigMapVolume(logAndMetricsConfigVolumeName, ancillaryConfigMapName));
        volumeList.add(VolumeUtils.createSecretVolume(ZOOKEEPER_NODE_CERTIFICATES_VOLUME_NAME, ZookeeperCluster.nodesSecretName(cluster), isOpenShift));
        volumeList.add(VolumeUtils.createSecretVolume(ZOOKEEPER_CLUSTER_CA_VOLUME_NAME, AbstractModel.clusterCaCertSecretName(cluster), isOpenShift));

        return volumeList;
    }

    /* test */ List<PersistentVolumeClaim> getVolumeClaims() {
        List<PersistentVolumeClaim> pvcList = new ArrayList<>(1);
        if (storage instanceof PersistentClaimStorage) {
            pvcList.add(VolumeUtils.createPersistentVolumeClaimTemplate(VOLUME_NAME, (PersistentClaimStorage) storage));
        }
        return pvcList;
    }

    public List<PersistentVolumeClaim> generatePersistentVolumeClaims() {
        List<PersistentVolumeClaim> pvcList = new ArrayList<>(replicas);
        if (storage instanceof PersistentClaimStorage) {
            for (int i = 0; i < replicas; i++) {
                pvcList.add(createPersistentVolumeClaim(i, "data-" + name + "-" + i, (PersistentClaimStorage) storage));
            }
        }
        return pvcList;
    }

    private List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> volumeMountList = new ArrayList<>(5);

        volumeMountList.add(createTempDirVolumeMount());
        volumeMountList.add(VolumeUtils.createVolumeMount(VOLUME_NAME, mountPath));
        volumeMountList.add(VolumeUtils.createVolumeMount(logAndMetricsConfigVolumeName, logAndMetricsConfigMountPath));
        volumeMountList.add(VolumeUtils.createVolumeMount(ZOOKEEPER_NODE_CERTIFICATES_VOLUME_NAME, ZOOKEEPER_NODE_CERTIFICATES_VOLUME_MOUNT));
        volumeMountList.add(VolumeUtils.createVolumeMount(ZOOKEEPER_CLUSTER_CA_VOLUME_NAME, ZOOKEEPER_CLUSTER_CA_VOLUME_MOUNT));

        return volumeMountList;
    }

    /**
     * Generates the PodDisruptionBudget.
     *
     * @return The PodDisruptionBudget.
     */
    public PodDisruptionBudget generatePodDisruptionBudget() {
        return createPodDisruptionBudget();
    }

    @Override
    protected String getDefaultLogConfigFileName() {
        return "zookeeperDefaultLoggingProperties";
    }

    /**
     * Get the name of the zookeeper service account given the name of the {@code zookeeperResourceName}.
     * @param zookeeperResourceName The resource name.
     * @return The service account name.
     */
    public static String containerServiceAccountName(String zookeeperResourceName) {
        return zookeeperClusterName(zookeeperResourceName);
    }

    @Override
    protected String getServiceAccountName() {
        return containerServiceAccountName(cluster);
    }

    @Override
    public void setImage(String image) {
        super.setImage(image);
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return this.version;
    }

    /**
     * Generates a configuration ConfigMap with metrics and logging configurations and node count.
     *
     * @param metricsAndLogging    The ConfigMaps with original logging and metrics configurations.
     *
     * @return      The generated configuration ConfigMap.
     */
    public ConfigMap generateConfigurationConfigMap(MetricsAndLogging metricsAndLogging) {
        ConfigMap zkConfigMap = super.generateMetricsAndLogConfigMap(metricsAndLogging);
        zkConfigMap.getData().put(CONFIG_MAP_KEY_ZOOKEEPER_NODE_COUNT, Integer.toString(getReplicas()));
        return zkConfigMap;
    }
}
