/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyIngressRule;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeer;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.template.InternalServiceTemplate;
import io.strimzi.api.kafka.model.common.template.PodDisruptionBudgetTemplate;
import io.strimzi.api.kafka.model.common.template.PodTemplate;
import io.strimzi.api.kafka.model.common.template.ResourceTemplate;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaClusterSpec;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.zookeeper.ZookeeperClusterSpec;
import io.strimzi.api.kafka.model.zookeeper.ZookeeperClusterTemplate;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.cluster.model.jmx.JmxModel;
import io.strimzi.operator.cluster.model.jmx.SupportsJmx;
import io.strimzi.operator.cluster.model.logging.LoggingModel;
import io.strimzi.operator.cluster.model.logging.SupportsLogging;
import io.strimzi.operator.cluster.model.metrics.MetricsModel;
import io.strimzi.operator.cluster.model.metrics.SupportsMetrics;
import io.strimzi.operator.cluster.model.securityprofiles.ContainerSecurityProviderContextImpl;
import io.strimzi.operator.cluster.model.securityprofiles.PodSecurityProviderContextImpl;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.StatusUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.emptyMap;

/**
 * ZooKeeper cluster model
 */
@SuppressWarnings({"checkstyle:ClassFanOutComplexity"})
public class ZookeeperCluster extends AbstractModel implements SupportsMetrics, SupportsLogging, SupportsJmx {
    /**
     * Port for plaintext access for ZooKeeper clients (available inside the pod only)
     */
    public static final int CLIENT_PLAINTEXT_PORT = 12181; // This port is internal only, not exposed => no need for name

    /**
     * TLS port for ZooKeeper clients
     */
    public static final int CLIENT_TLS_PORT = 2181;

    /**
     * Port used for ZooKeeper clustering
     */
    public static final int CLUSTERING_PORT = 2888;

    /**
     * Port used for ZooKeeper leader election
     */
    public static final int LEADER_ELECTION_PORT = 3888;

    protected static final String COMPONENT_TYPE = "zookeeper";
    protected static final String CLIENT_TLS_PORT_NAME = "tcp-clients";
    protected static final String CLUSTERING_PORT_NAME = "tcp-clustering";
    protected static final String LEADER_ELECTION_PORT_NAME = "tcp-election";

    protected static final String ZOOKEEPER_NAME = "zookeeper";
    protected static final String ZOOKEEPER_NODE_CERTIFICATES_VOLUME_NAME = "zookeeper-nodes";
    protected static final String ZOOKEEPER_NODE_CERTIFICATES_VOLUME_MOUNT = "/opt/kafka/zookeeper-node-certs/";
    protected static final String ZOOKEEPER_CLUSTER_CA_VOLUME_NAME = "cluster-ca-certs";
    protected static final String ZOOKEEPER_CLUSTER_CA_VOLUME_MOUNT = "/opt/kafka/cluster-ca-certs/";
    private static final String DATA_VOLUME_MOUNT_PATH = "/var/lib/zookeeper";
    private static final String LOG_AND_METRICS_CONFIG_VOLUME_NAME = "zookeeper-metrics-and-logging";
    private static final String LOG_AND_METRICS_CONFIG_VOLUME_MOUNT = "/opt/kafka/custom-config/";

    // Zookeeper configuration
    private int replicas;
    private final boolean isSnapshotCheckEnabled;
    private JmxModel jmx;
    @SuppressFBWarnings({"UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR"}) // This field is initialized in the fromCrd method
    private MetricsModel metrics;
    private LoggingModel logging;
    /* test */ ZookeeperConfiguration configuration;

    /**
     * Storage configuration
     */
    protected Storage storage;

    /**
     * Warning conditions generated from the Custom Resource
     */
    protected List<Condition> warningConditions = new ArrayList<>(0);

    private static final boolean DEFAULT_ZOOKEEPER_SNAPSHOT_CHECK_ENABLED = true;

    // Zookeeper configuration keys (EnvVariables)
    protected static final String ENV_VAR_ZOOKEEPER_METRICS_ENABLED = "ZOOKEEPER_METRICS_ENABLED";
    protected static final String ENV_VAR_ZOOKEEPER_CONFIGURATION = "ZOOKEEPER_CONFIGURATION";
    private static final String ENV_VAR_ZOOKEEPER_SNAPSHOT_CHECK_ENABLED = "ZOOKEEPER_SNAPSHOT_CHECK_ENABLED";

    protected static final String CO_ENV_VAR_CUSTOM_ZOOKEEPER_POD_LABELS = "STRIMZI_CUSTOM_ZOOKEEPER_LABELS";

    // Config map keys
    private static final String CONFIG_MAP_KEY_ZOOKEEPER_NODE_COUNT = "zookeeper.node-count";

    // Templates
    private PodDisruptionBudgetTemplate templatePodDisruptionBudget;
    private ResourceTemplate templatePersistentVolumeClaims;
    private ResourceTemplate templatePodSet;
    private PodTemplate templatePod;
    private InternalServiceTemplate templateHeadlessService;
    private InternalServiceTemplate templateService;

    private static final Map<String, String> DEFAULT_POD_LABELS = new HashMap<>();
    static {
        String value = System.getenv(CO_ENV_VAR_CUSTOM_ZOOKEEPER_POD_LABELS);
        if (value != null) {
            DEFAULT_POD_LABELS.putAll(Util.parseMap(value));
        }
    }

    /**
     * Constructor
     *
     * @param reconciliation The reconciliation
     * @param resource Kubernetes resource with metadata containing the namespace and cluster name
     * @param sharedEnvironmentProvider Shared environment provider
     */
    private ZookeeperCluster(Reconciliation reconciliation, HasMetadata resource, SharedEnvironmentProvider sharedEnvironmentProvider) {
        super(reconciliation, resource, KafkaResources.zookeeperComponentName(resource.getMetadata().getName()), COMPONENT_TYPE, sharedEnvironmentProvider);

        this.image = null;
        this.isSnapshotCheckEnabled = DEFAULT_ZOOKEEPER_SNAPSHOT_CHECK_ENABLED;
    }

    /**
     * Creates ZooKeeper cluster model from the Kafka CR
     *
     * @param reconciliation    Reconciliation marker
     * @param kafkaAssembly     The Kafka CR
     * @param versions          Supported Kafka versions
     * @param sharedEnvironmentProvider Shared environment provider
     *
     * @return  New instance of the ZooKeeper cluster model
     */
    public static ZookeeperCluster fromCrd(Reconciliation reconciliation, Kafka kafkaAssembly, KafkaVersion.Lookup versions, SharedEnvironmentProvider sharedEnvironmentProvider) {
        return fromCrd(reconciliation, kafkaAssembly, versions, null, 0, sharedEnvironmentProvider);
    }

    /**
     * Creates ZooKeeper cluster model from the Kafka CR
     *
     * @param reconciliation    Reconciliation marker
     * @param kafkaAssembly     The Kafka CR
     * @param versions          Supported Kafka versions
     * @param oldStorage        Old storage configuration (based on the actual Kubernetes cluster)
     * @param oldReplicas       Current number of replicas (based on the actual Kubernetes cluster)
     * @param sharedEnvironmentProvider Shared environment provider
     *
     * @return  New instance of the ZooKeeper cluster model
     */
    @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    public static ZookeeperCluster fromCrd(Reconciliation reconciliation, Kafka kafkaAssembly, KafkaVersion.Lookup versions, Storage oldStorage, int oldReplicas, SharedEnvironmentProvider sharedEnvironmentProvider) {
        ZookeeperCluster result = new ZookeeperCluster(reconciliation, kafkaAssembly, sharedEnvironmentProvider);
        ZookeeperClusterSpec zookeeperClusterSpec = kafkaAssembly.getSpec().getZookeeper();

        int replicas = zookeeperClusterSpec.getReplicas();

        if (replicas == 1 && zookeeperClusterSpec.getStorage() != null && "ephemeral".equals(zookeeperClusterSpec.getStorage().getType())) {
            LOGGER.warnCr(reconciliation, "A ZooKeeper cluster with a single replica and ephemeral storage will be in a defective state after any restart or rolling update. It is recommended that a minimum of three replicas are used.");
        }
        result.replicas = replicas;

        ModelUtils.validateComputeResources(zookeeperClusterSpec.getResources(), ".spec.zookeeper.resources");

        String image = zookeeperClusterSpec.getImage();
        if (image == null) {
            KafkaClusterSpec kafkaClusterSpec = kafkaAssembly.getSpec().getKafka();
            image = versions.kafkaImage(kafkaClusterSpec != null ? kafkaClusterSpec.getImage() : null,
                    kafkaClusterSpec != null ? kafkaClusterSpec.getVersion() : null);
        }
        result.image = image;

        result.readinessProbeOptions = ProbeUtils.extractReadinessProbeOptionsOrDefault(zookeeperClusterSpec, ProbeUtils.DEFAULT_HEALTHCHECK_OPTIONS);
        result.livenessProbeOptions = ProbeUtils.extractLivenessProbeOptionsOrDefault(zookeeperClusterSpec, ProbeUtils.DEFAULT_HEALTHCHECK_OPTIONS);

        result.gcLoggingEnabled = zookeeperClusterSpec.getJvmOptions() == null ? JvmOptions.DEFAULT_GC_LOGGING_ENABLED : zookeeperClusterSpec.getJvmOptions().isGcLoggingEnabled();

        if (oldStorage != null) {
            Storage newStorage = zookeeperClusterSpec.getStorage();
            StorageUtils.validatePersistentStorage(newStorage, "Kafka.spec.zookeeper.storage");

            StorageDiff diff = new StorageDiff(
                    reconciliation,
                    oldStorage,
                    newStorage,
                    IntStream.range(0, oldReplicas).boxed().collect(Collectors.toUnmodifiableSet()),
                    IntStream.range(0, zookeeperClusterSpec.getReplicas()).boxed().collect(Collectors.toUnmodifiableSet())
            );

            if (!diff.isEmpty()) {
                LOGGER.warnCr(reconciliation, "Only the following changes to Zookeeper storage are allowed: " +
                        "changing the deleteClaim flag, " +
                        "changing overrides to nodes which do not exist yet " +
                        "and increasing size of persistent claim volumes (depending on the volume type and used storage class).");
                LOGGER.warnCr(reconciliation, "The desired ZooKeeper storage configuration in the custom resource {}/{} contains changes which are not allowed. As " +
                        "a result, all storage changes will be ignored. Use DEBUG level logging for more information " +
                        "about the detected changes.", kafkaAssembly.getMetadata().getNamespace(), kafkaAssembly.getMetadata().getName());

                Condition warning = StatusUtils.buildWarningCondition("ZooKeeperStorage",
                        "The desired ZooKeeper storage configuration contains changes which are not allowed. As a " +
                                "result, all storage changes will be ignored. Use DEBUG level logging for more information " +
                                "about the detected changes.");
                result.warningConditions.add(warning);

                result.setStorage(oldStorage);
            } else {
                result.setStorage(newStorage);
            }
        } else {
            result.setStorage(zookeeperClusterSpec.getStorage());
        }

        result.configuration = new ZookeeperConfiguration(reconciliation, zookeeperClusterSpec.getConfig().entrySet());

        result.resources = zookeeperClusterSpec.getResources();

        result.jvmOptions = zookeeperClusterSpec.getJvmOptions();
        result.metrics = new MetricsModel(zookeeperClusterSpec);
        result.logging = new LoggingModel(zookeeperClusterSpec, result.getClass().getSimpleName(), false, false);
        result.jmx = new JmxModel(
                reconciliation.namespace(),
                KafkaResources.zookeeperJmxSecretName(result.cluster),
                result.labels,
                result.ownerReference,
                zookeeperClusterSpec
        );

        if (zookeeperClusterSpec.getTemplate() != null) {
            ZookeeperClusterTemplate template = zookeeperClusterSpec.getTemplate();

            result.templatePodDisruptionBudget = template.getPodDisruptionBudget();
            result.templatePersistentVolumeClaims = template.getPersistentVolumeClaim();
            result.templatePodSet = template.getPodSet();
            result.templatePod = template.getPod();
            result.templateService = template.getClientService();
            result.templateHeadlessService = template.getNodesService();
            result.templateServiceAccount = template.getServiceAccount();
            result.templateContainer = template.getZookeeperContainer();
        }

        // Should run at the end when everything is set
        ZooKeeperSpecChecker specChecker = new ZooKeeperSpecChecker(result);
        result.warningConditions.addAll(specChecker.run());

        return result;
    }

    /**
     * @return The storage.
     */
    public Storage getStorage() {
        return storage;
    }

    /**
     * Set the Storage
     *
     * @param storage Persistent Storage configuration
     */
    protected void setStorage(Storage storage) {
        StorageUtils.validatePersistentStorage(storage, "Kafka.spec.zookeeper.storage");
        this.storage = storage;
    }

    /**
     * Returns a list of warning conditions set by the model. Returns an empty list if no warning conditions were set.
     *
     * @return  List of warning conditions.
     */
    public List<Condition> getWarningConditions() {
        return warningConditions;
    }

    /**
     * @return  Generates a ZooKeeper service
     */
    public Service generateService() {
        return ServiceUtils.createClusterIpService(
                KafkaResources.zookeeperServiceName(cluster),
                namespace,
                labels,
                ownerReference,
                templateService,
                List.of(ServiceUtils.createServicePort(CLIENT_TLS_PORT_NAME, CLIENT_TLS_PORT, CLIENT_TLS_PORT, "TCP"))
        );
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
        // Internal peers => Strimzi components which need access
        NetworkPolicyPeer clusterOperatorPeer = NetworkPolicyUtils.createPeer(Map.of(Labels.STRIMZI_KIND_LABEL, "cluster-operator"), NetworkPolicyUtils.clusterOperatorNamespaceSelector(namespace, operatorNamespace, operatorNamespaceLabels));
        NetworkPolicyPeer zookeeperClusterPeer = NetworkPolicyUtils.createPeer(labels.strimziSelectorLabels().toMap());
        NetworkPolicyPeer kafkaClusterPeer = NetworkPolicyUtils.createPeer(Map.of(Labels.STRIMZI_NAME_LABEL, KafkaResources.kafkaComponentName(cluster)));
        NetworkPolicyPeer entityOperatorPeer = NetworkPolicyUtils.createPeer(Map.of(Labels.STRIMZI_NAME_LABEL, KafkaResources.entityOperatorDeploymentName(cluster)));

        // List of network policy rules for all ports
        List<NetworkPolicyIngressRule> rules = new ArrayList<>();

        // Zookeeper only ports - 2888 & 3888 which need to be accessed by the Zookeeper cluster members only
        rules.add(NetworkPolicyUtils.createIngressRule(CLUSTERING_PORT, List.of(zookeeperClusterPeer)));
        rules.add(NetworkPolicyUtils.createIngressRule(LEADER_ELECTION_PORT, List.of(zookeeperClusterPeer)));

        // Clients port - needs to be access from outside the Zookeeper cluster as well
        rules.add(NetworkPolicyUtils.createIngressRule(CLIENT_TLS_PORT, List.of(kafkaClusterPeer, zookeeperClusterPeer, entityOperatorPeer, clusterOperatorPeer)));

        // The Metrics port (if enabled) is opened to all by default
        if (metrics.isEnabled()) {
            rules.add(NetworkPolicyUtils.createIngressRule(MetricsModel.METRICS_PORT, List.of()));
        }

        // The JMX port (if enabled) is opened to all by default
        rules.addAll(jmx.networkPolicyIngresRules());

        // Build the final network policy with all rules covering all the ports
        return NetworkPolicyUtils.createNetworkPolicy(
                KafkaResources.zookeeperNetworkPolicyName(cluster),
                namespace,
                labels,
                ownerReference,
                rules
        );
    }

    /**
     * @return  Generates the headless ZooKeeper service
     */
    public Service generateHeadlessService() {
        return ServiceUtils.createHeadlessService(
                KafkaResources.zookeeperHeadlessServiceName(cluster),
                namespace,
                labels,
                ownerReference,
                templateHeadlessService,
                getServicePortList()
        );
    }

    /**
     * Generates the StrimziPodSet for the ZooKeeper cluster.
     *
     * @param replicas                  Number of replicas the StrimziPodSet should have. During scale-ups or scale-downs,
     *                                  node sets with different numbers of pods are generated.
     * @param isOpenShift               Flags whether we are on OpenShift or not
     * @param imagePullPolicy           Image pull policy which will be used by the pods
     * @param imagePullSecrets          List of image pull secrets
     * @param podAnnotationsProvider    Function which provides the annotations for the given pod based on its index.
     *                                  The annotations for each pod are different due to different certificates. So they
     *                                  need to be dynamically generated though this function instead of just
     *                                  passed as Map.
     *
     * @return                  Generated StrimziPodSet with ZooKeeper pods
     */
    public StrimziPodSet generatePodSet(int replicas,
                                        boolean isOpenShift,
                                        ImagePullPolicy imagePullPolicy,
                                        List<LocalObjectReference> imagePullSecrets,
                                        Function<Integer, Map<String, String>> podAnnotationsProvider) {
        return WorkloadUtils.createPodSet(
                componentName,
                namespace,
                labels,
                ownerReference,
                templatePodSet,
                replicas,
                Map.of(Annotations.ANNO_STRIMZI_IO_STORAGE, ModelUtils.encodeStorageToJson(storage)),
                labels.strimziSelectorLabels(),
                podNum -> WorkloadUtils.createStatefulPod(
                        reconciliation,
                        getPodName(podNum),
                        namespace,
                        labels,
                        componentName,
                        componentName,
                        templatePod,
                        DEFAULT_POD_LABELS,
                        podAnnotationsProvider.apply(podNum),
                        KafkaResources.zookeeperHeadlessServiceName(cluster),
                        templatePod != null ? templatePod.getAffinity() : null,
                        null,
                        List.of(createContainer(imagePullPolicy)),
                        getPodSetVolumes(getPodName(podNum), isOpenShift),
                        imagePullSecrets,
                        securityProvider.zooKeeperPodSecurityContext(new PodSecurityProviderContextImpl(storage, templatePod))
                )
        );
    }

    /**
     * Generate the Secret containing the Zookeeper nodes certificates signed by the cluster CA certificate used for TLS
     * based internal communication with Kafka. It contains both the public and private keys.
     *
     * @param clusterCa                         The CA for cluster certificates
     * @param isMaintenanceTimeWindowsSatisfied Indicates whether we are in the maintenance window or not.
     *
     * @return The generated Secret with the ZooKeeper node certificates
     */
    public Secret generateCertificatesSecret(ClusterCa clusterCa, boolean isMaintenanceTimeWindowsSatisfied) {
        Map<String, CertAndKey> certs;

        try {
            certs = clusterCa.generateZkCerts(namespace, cluster, nodes(), isMaintenanceTimeWindowsSatisfied);
        } catch (IOException e) {
            LOGGER.warnCr(reconciliation, "Error while generating certificates", e);
            throw new RuntimeException("Failed to prepare ZooKeeper certificates", e);
        }

        return ModelUtils.createSecret(KafkaResources.zookeeperSecretName(cluster), namespace, labels, ownerReference,
                CertUtils.buildSecretData(certs), Map.ofEntries(clusterCa.caCertGenerationFullAnnotation()), emptyMap());
    }

    /* test */ Container createContainer(ImagePullPolicy imagePullPolicy) {
        return ContainerUtils.createContainer(
                ZOOKEEPER_NAME,
                image,
                List.of("/opt/kafka/zookeeper_run.sh"),
                securityProvider.zooKeeperContainerSecurityContext(new ContainerSecurityProviderContextImpl(storage, templateContainer)),
                resources,
                getEnvVars(),
                getContainerPortList(),
                getVolumeMounts(),
                ProbeUtils.execProbe(livenessProbeOptions, List.of("/opt/kafka/zookeeper_healthcheck.sh")),
                ProbeUtils.execProbe(readinessProbeOptions, List.of("/opt/kafka/zookeeper_healthcheck.sh")),
                imagePullPolicy
        );
    }

    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = new ArrayList<>();
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_ZOOKEEPER_METRICS_ENABLED, String.valueOf(metrics.isEnabled())));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_ZOOKEEPER_SNAPSHOT_CHECK_ENABLED, String.valueOf(isSnapshotCheckEnabled)));
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_STRIMZI_KAFKA_GC_LOG_ENABLED, String.valueOf(gcLoggingEnabled)));

        varList.addAll(jmx.envVars());

        JvmOptionUtils.heapOptions(varList, 75, 2L * 1024L * 1024L * 1024L, jvmOptions, resources);
        JvmOptionUtils.jvmPerformanceOptions(varList, jvmOptions);
        JvmOptionUtils.jvmSystemProperties(varList, jvmOptions);
        varList.add(ContainerUtils.createEnvVar(ENV_VAR_ZOOKEEPER_CONFIGURATION, configuration.getConfiguration()));

        // Add shared environment variables used for all containers
        varList.addAll(sharedEnvironmentProvider.variables());

        ContainerUtils.addContainerEnvsToExistingEnvs(reconciliation, varList, templateContainer);

        return varList;
    }

    private List<ServicePort> getServicePortList() {
        List<ServicePort> portList = new ArrayList<>(4);
        portList.add(ServiceUtils.createServicePort(CLIENT_TLS_PORT_NAME, CLIENT_TLS_PORT, CLIENT_TLS_PORT, "TCP"));
        portList.add(ServiceUtils.createServicePort(CLUSTERING_PORT_NAME, CLUSTERING_PORT, CLUSTERING_PORT, "TCP"));
        portList.add(ServiceUtils.createServicePort(LEADER_ELECTION_PORT_NAME, LEADER_ELECTION_PORT, LEADER_ELECTION_PORT, "TCP"));

        portList.addAll(jmx.servicePorts());

        return portList;
    }

    private List<ContainerPort> getContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>(4);

        portList.add(ContainerUtils.createContainerPort(CLUSTERING_PORT_NAME, CLUSTERING_PORT));
        portList.add(ContainerUtils.createContainerPort(LEADER_ELECTION_PORT_NAME, LEADER_ELECTION_PORT));
        portList.add(ContainerUtils.createContainerPort(CLIENT_TLS_PORT_NAME, CLIENT_TLS_PORT));

        if (metrics.isEnabled()) {
            portList.add(ContainerUtils.createContainerPort(MetricsModel.METRICS_PORT_NAME, MetricsModel.METRICS_PORT));
        }

        portList.addAll(jmx.containerPorts());

        return portList;
    }

    /**
     * Generates a list of volumes used by PodSets. For StrimziPodSet, it needs to include also all persistent claim
     * volumes which StatefulSet would generate on its own.
     *
     * @param podName       Name of the pod used to name the volumes
     * @param isOpenShift   Flag whether we are on OpenShift or not
     *
     * @return              List of volumes to be included in the StrimziPodSet pod
     */
    private List<Volume> getPodSetVolumes(String podName, boolean isOpenShift) {
        List<Volume> volumeList = new ArrayList<>(5);

        volumeList.add(VolumeUtils.createTempDirVolume(templatePod));
        volumeList.add(VolumeUtils.createConfigMapVolume(LOG_AND_METRICS_CONFIG_VOLUME_NAME, KafkaResources.zookeeperMetricsAndLogConfigMapName(cluster)));
        volumeList.add(VolumeUtils.createSecretVolume(ZOOKEEPER_NODE_CERTIFICATES_VOLUME_NAME, KafkaResources.zookeeperSecretName(cluster), isOpenShift));
        volumeList.add(VolumeUtils.createSecretVolume(ZOOKEEPER_CLUSTER_CA_VOLUME_NAME, AbstractModel.clusterCaCertSecretName(cluster), isOpenShift));
        volumeList.addAll(VolumeUtils.createPodSetVolumes(podName, storage, false));

        return volumeList;
    }

    /**
     * @return  Generates list of ZooKeeper PVCs
     */
    public List<PersistentVolumeClaim> generatePersistentVolumeClaims() {
        return PersistentVolumeClaimUtils
                .createPersistentVolumeClaims(
                        namespace,
                        nodes(),
                        storage,
                        false,
                        labels,
                        ownerReference,
                        templatePersistentVolumeClaims
                );
    }

    private List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> volumeMountList = new ArrayList<>(5);

        volumeMountList.add(VolumeUtils.createTempDirVolumeMount());
        // ZooKeeper uses mount path which is different from the one used by Kafka.
        // As a result it cannot use VolumeUtils.getVolumeMounts and creates the volume mount directly
        volumeMountList.add(VolumeUtils.createVolumeMount(VolumeUtils.DATA_VOLUME_NAME, DATA_VOLUME_MOUNT_PATH));
        volumeMountList.add(VolumeUtils.createVolumeMount(LOG_AND_METRICS_CONFIG_VOLUME_NAME, LOG_AND_METRICS_CONFIG_VOLUME_MOUNT));
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
        return PodDisruptionBudgetUtils.createCustomControllerPodDisruptionBudget(componentName, namespace, labels, ownerReference, templatePodDisruptionBudget, replicas);
    }

    /**
     * Generates a configuration ConfigMap with metrics and logging configurations and node count.
     *
     * @param metricsAndLogging    The ConfigMaps with original logging and metrics configurations.
     *
     * @return      The generated configuration ConfigMap.
     */
    public ConfigMap generateConfigurationConfigMap(MetricsAndLogging metricsAndLogging) {
        Map<String, String> data = ConfigMapUtils.generateMetricsAndLogConfigMapData(reconciliation, this, metricsAndLogging);
        data.put(CONFIG_MAP_KEY_ZOOKEEPER_NODE_COUNT, Integer.toString(replicas));

        return ConfigMapUtils
                .createConfigMap(
                        KafkaResources.zookeeperMetricsAndLogConfigMapName(cluster),
                        namespace,
                        labels,
                        ownerReference,
                        data
                );
    }

    /**
     * @return The number of replicas
     */
    public int getReplicas() {
        return replicas;
    }

    /**
     * @return  JMX Model instance for configuring JMX access
     */
    public JmxModel jmx()   {
        return jmx;
    }

    /**
     * @return  Metrics Model instance for configuring Prometheus metrics
     */
    public MetricsModel metrics()   {
        return metrics;
    }

    /**
     * @return  Logging Model instance for configuring logging
     */
    public LoggingModel logging()   {
        return logging;
    }

    /**
     * @return  Set of node references for this ZooKeeper cluster
     */
    public Set<NodeRef> nodes()   {
        Set<NodeRef> nodes = new LinkedHashSet<>();

        for (int i = 0; i < replicas; i++)  {
            nodes.add(new NodeRef(getPodName(i), i, null, false, false));
        }

        return nodes;
    }
}
