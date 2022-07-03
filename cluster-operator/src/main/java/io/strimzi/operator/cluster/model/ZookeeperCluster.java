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
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.KafkaJmxAuthenticationPassword;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.Logging;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.ZookeeperClusterSpec;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.api.kafka.model.template.ZookeeperClusterTemplate;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.cluster.model.securityprofiles.ContainerSecurityProviderContextImpl;
import io.strimzi.operator.cluster.model.securityprofiles.PodSecurityProviderContextImpl;
import io.strimzi.operator.common.MetricsAndLogging;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.StatusUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"checkstyle:ClassFanOutComplexity"})
public class ZookeeperCluster extends AbstractModel {
    public static final String APPLICATION_NAME = "zookeeper";

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

    // Env vars for JMX service
    protected static final String ENV_VAR_ZOOKEEPER_JMX_ENABLED = "ZOOKEEPER_JMX_ENABLED";
    private static final String SECRET_JMX_USERNAME_KEY = "jmx-username";
    private static final String SECRET_JMX_PASSWORD_KEY = "jmx-password";
    private static final String ENV_VAR_ZOOKEEPER_JMX_USERNAME = "ZOOKEEPER_JMX_USERNAME";
    private static final String ENV_VAR_ZOOKEEPER_JMX_PASSWORD = "ZOOKEEPER_JMX_PASSWORD";

    // Zookeeper configuration
    private final boolean isSnapshotCheckEnabled;
    private boolean isJmxEnabled = false;
    private boolean isJmxAuthenticated = false;

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

    protected static final String CO_ENV_VAR_CUSTOM_ZOOKEEPER_POD_LABELS = "STRIMZI_CUSTOM_ZOOKEEPER_LABELS";

    // Config map keys
    public static final String CONFIG_MAP_KEY_ZOOKEEPER_NODE_COUNT = "zookeeper.node-count";

    // Templates
    protected List<ContainerEnvVar> templateZookeeperContainerEnvVars;
    protected SecurityContext templateZookeeperContainerSecurityContext;

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
     */
    private ZookeeperCluster(Reconciliation reconciliation, HasMetadata resource) {
        super(reconciliation, resource, APPLICATION_NAME);
        this.name = KafkaResources.zookeeperStatefulSetName(cluster);
        this.serviceName = KafkaResources.zookeeperServiceName(cluster);
        this.headlessServiceName = KafkaResources.zookeeperHeadlessServiceName(cluster);
        this.ancillaryConfigMapName = KafkaResources.zookeeperMetricsAndLogConfigMapName(cluster);
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

    public static ZookeeperCluster fromCrd(Reconciliation reconciliation, Kafka kafkaAssembly, KafkaVersion.Lookup versions) {
        return fromCrd(reconciliation, kafkaAssembly, versions, null, 0);
    }

    @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    public static ZookeeperCluster fromCrd(Reconciliation reconciliation, Kafka kafkaAssembly, KafkaVersion.Lookup versions, Storage oldStorage, int oldReplicas) {
        ZookeeperCluster zk = new ZookeeperCluster(reconciliation, kafkaAssembly);
        zk.setOwnerReference(kafkaAssembly);
        ZookeeperClusterSpec zookeeperClusterSpec = kafkaAssembly.getSpec().getZookeeper();

        int replicas = zookeeperClusterSpec.getReplicas();
        if (replicas <= 0) {
            replicas = ZookeeperClusterSpec.DEFAULT_REPLICAS;
        }
        if (replicas == 1 && zookeeperClusterSpec.getStorage() != null && "ephemeral".equals(zookeeperClusterSpec.getStorage().getType())) {
            LOGGER.warnCr(reconciliation, "A ZooKeeper cluster with a single replica and ephemeral storage will be in a defective state after any restart or rolling update. It is recommended that a minimum of three replicas are used.");
        }
        zk.setReplicas(replicas);

        String image = zookeeperClusterSpec.getImage();
        if (image == null) {
            KafkaClusterSpec kafkaClusterSpec = kafkaAssembly.getSpec().getKafka();
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

        // Parse different types of metrics configurations
        ModelUtils.parseMetrics(zk, zookeeperClusterSpec);

        if (oldStorage != null) {
            Storage newStorage = zookeeperClusterSpec.getStorage();
            AbstractModel.validatePersistentStorage(newStorage);

            StorageDiff diff = new StorageDiff(reconciliation, oldStorage, newStorage, oldReplicas, zookeeperClusterSpec.getReplicas());

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
                zk.addWarningCondition(warning);

                zk.setStorage(oldStorage);
            } else {
                zk.setStorage(newStorage);
            }
        } else {
            zk.setStorage(zookeeperClusterSpec.getStorage());
        }

        zk.setConfiguration(new ZookeeperConfiguration(reconciliation, zookeeperClusterSpec.getConfig().entrySet()));

        zk.setResources(zookeeperClusterSpec.getResources());

        zk.setJvmOptions(zookeeperClusterSpec.getJvmOptions());

        if (zookeeperClusterSpec.getJmxOptions() != null) {
            zk.isJmxEnabled = true;

            if (zookeeperClusterSpec.getJmxOptions().getAuthentication() != null)   {
                zk.isJmxAuthenticated = zookeeperClusterSpec.getJmxOptions().getAuthentication() instanceof KafkaJmxAuthenticationPassword;
            }
        }

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

            if (template.getPodSet() != null && template.getPodSet().getMetadata() != null) {
                zk.templatePodSetLabels = template.getPodSet().getMetadata().getLabels();
                zk.templatePodSetAnnotations = template.getPodSet().getMetadata().getAnnotations();
            }

            ModelUtils.parsePodTemplate(zk, template.getPod());
            ModelUtils.parseInternalServiceTemplate(zk, template.getClientService());
            ModelUtils.parseInternalHeadlessServiceTemplate(zk, template.getNodesService());

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

            if (template.getServiceAccount() != null && template.getServiceAccount().getMetadata() != null) {
                zk.templateServiceAccountLabels = template.getServiceAccount().getMetadata().getLabels();
                zk.templateServiceAccountAnnotations = template.getServiceAccount().getMetadata().getAnnotations();
            }

            if (template.getJmxSecret() != null && template.getJmxSecret().getMetadata() != null) {
                zk.templateJmxSecretLabels = template.getJmxSecret().getMetadata().getLabels();
                zk.templateJmxSecretAnnotations = template.getJmxSecret().getMetadata().getAnnotations();
            }

            ModelUtils.parsePodDisruptionBudgetTemplate(zk, template.getPodDisruptionBudget());
        }

        zk.templatePodLabels = Util.mergeLabelsOrAnnotations(zk.templatePodLabels, DEFAULT_POD_LABELS);

        // Should run at the end when everything is set
        ZooKeeperSpecChecker specChecker = new ZooKeeperSpecChecker(zk);
        zk.warningConditions.addAll(specChecker.run());

        return zk;
    }

    public Service generateService() {
        List<ServicePort> ports = new ArrayList<>(1);
        ports.add(createServicePort(CLIENT_TLS_PORT_NAME, CLIENT_TLS_PORT, CLIENT_TLS_PORT, "TCP"));

        return createService("ClusterIP", ports, templateServiceAnnotations);
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
        clientsPort.setProtocol("TCP");

        NetworkPolicyPort clusteringPort = new NetworkPolicyPort();
        clusteringPort.setPort(new IntOrString(CLUSTERING_PORT));
        clusteringPort.setProtocol("TCP");

        NetworkPolicyPort leaderElectionPort = new NetworkPolicyPort();
        leaderElectionPort.setPort(new IntOrString(LEADER_ELECTION_PORT));
        leaderElectionPort.setProtocol("TCP");

        NetworkPolicyPeer zookeeperClusterPeer = new NetworkPolicyPeer();
        LabelSelector labelSelector2 = new LabelSelector();
        Map<String, String> expressions2 = new HashMap<>(1);
        expressions2.put(Labels.STRIMZI_NAME_LABEL, KafkaResources.zookeeperStatefulSetName(cluster));
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
        expressions.put(Labels.STRIMZI_NAME_LABEL, KafkaResources.kafkaStatefulSetName(cluster));
        labelSelector.setMatchLabels(expressions);
        kafkaClusterPeer.setPodSelector(labelSelector);

        NetworkPolicyPeer entityOperatorPeer = new NetworkPolicyPeer();
        LabelSelector labelSelector3 = new LabelSelector();
        Map<String, String> expressions3 = new HashMap<>(1);
        expressions3.put(Labels.STRIMZI_NAME_LABEL, KafkaResources.entityOperatorDeploymentName(cluster));
        labelSelector3.setMatchLabels(expressions3);
        entityOperatorPeer.setPodSelector(labelSelector3);

        NetworkPolicyPeer clusterOperatorPeer = new NetworkPolicyPeer();
        LabelSelector labelSelector4 = new LabelSelector();
        Map<String, String> expressions4 = new HashMap<>(1);
        expressions4.put(Labels.STRIMZI_KIND_LABEL, "cluster-operator");
        labelSelector4.setMatchLabels(expressions4);
        clusterOperatorPeer.setPodSelector(labelSelector4);
        ModelUtils.setClusterOperatorNetworkPolicyNamespaceSelector(clusterOperatorPeer, namespace, operatorNamespace, operatorNamespaceLabels);

        // This is a hack because we have no guarantee that the CO namespace has some particular labels
        List<NetworkPolicyPeer> clientsPortPeers = new ArrayList<>(4);
        clientsPortPeers.add(kafkaClusterPeer);
        clientsPortPeers.add(zookeeperClusterPeer);
        clientsPortPeers.add(entityOperatorPeer);
        clientsPortPeers.add(clusterOperatorPeer);
        clientsIngressRule.setFrom(clientsPortPeers);

        rules.add(clientsIngressRule);

        if (isMetricsEnabled) {
            NetworkPolicyIngressRule metricsRule = new NetworkPolicyIngressRuleBuilder()
                    .addNewPort()
                        .withNewPort(METRICS_PORT)
                        .withProtocol("TCP")
                    .endPort()
                    .withFrom()
                    .build();

            rules.add(metricsRule);
        }

        if (isJmxEnabled) {
            NetworkPolicyPort jmxPort = new NetworkPolicyPort();
            jmxPort.setPort(new IntOrString(JMX_PORT));

            NetworkPolicyIngressRule jmxRule = new NetworkPolicyIngressRuleBuilder()
                    .withPorts(jmxPort)
                    .withFrom()
                    .build();

            rules.add(jmxRule);
        }

        NetworkPolicy networkPolicy = new NetworkPolicyBuilder()
                .withNewMetadata()
                    .withName(KafkaResources.zookeeperNetworkPolicyName(cluster))
                    .withNamespace(namespace)
                    .withLabels(labels.toMap())
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withNewSpec()
                    .withPodSelector(labelSelector2)
                    .withIngress(rules)
                .endSpec()
                .build();

        LOGGER.traceCr(reconciliation, "Created network policy {}", networkPolicy);
        return networkPolicy;
    }

    public Service generateHeadlessService() {
        return createHeadlessService(getServicePortList());
    }

    public StatefulSet generateStatefulSet(boolean isOpenShift, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets) {
        return createStatefulSet(
                Collections.singletonMap(ANNO_STRIMZI_IO_STORAGE, ModelUtils.encodeStorageToJson(storage)),
                Collections.emptyMap(),
                getStatefulSetVolumes(isOpenShift),
                getPersistentVolumeClaimTemplates(),
                getMergedAffinity(),
                getInitContainers(imagePullPolicy),
                getContainers(imagePullPolicy),
                imagePullSecrets,
                securityProvider.zooKeeperPodSecurityContext(new PodSecurityProviderContextImpl(storage, templateSecurityContext)));
    }

    /**
     * Generates the StrimziPodSet for the ZooKeeper cluster. This is used when the UseStrimziPodSets feature gate is
     * enabled.
     *
     * @param replicas          Number of replicas the StrimziPodSet should have. During scale-ups or scale-downs, node
     *                          sets with different numbers of pods are generated.
     * @param isOpenShift       Flags whether we are on OpenShift or not
     * @param imagePullPolicy   Image pull policy which will be used by the pods
     * @param imagePullSecrets  List of image pull secrets
     * @param podAnnotations    List of custom pod annotations
     *
     * @return                  Generated StrimziPodSet with ZooKeeper pods
     */
    public StrimziPodSet generatePodSet(int replicas,
                                        boolean isOpenShift,
                                        ImagePullPolicy imagePullPolicy,
                                        List<LocalObjectReference> imagePullSecrets,
                                        Map<String, String> podAnnotations) {
        return createPodSet(
            replicas,
            Collections.singletonMap(ANNO_STRIMZI_IO_STORAGE, ModelUtils.encodeStorageToJson(storage)),
            (brokerId) -> podAnnotations,
            podName -> getPodSetVolumes(podName, isOpenShift),
            getMergedAffinity(),
            getInitContainers(imagePullPolicy),
            getContainers(imagePullPolicy),
            imagePullSecrets,
            securityProvider.zooKeeperPodSecurityContext(new PodSecurityProviderContextImpl(storage, templateSecurityContext)));
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
        Map<String, String> secretData = new HashMap<>(replicas * 4);
        Map<String, CertAndKey> certs;

        try {
            certs = clusterCa.generateZkCerts(namespace, cluster, replicas, isMaintenanceTimeWindowsSatisfied);
        } catch (IOException e) {
            LOGGER.warnCr(reconciliation, "Error while generating certificates", e);
            throw new RuntimeException("Failed to prepare ZooKeeper certificates", e);
        }

        for (int i = 0; i < replicas; i++) {
            CertAndKey cert = certs.get(KafkaResources.zookeeperPodName(cluster, i));
            secretData.put(KafkaResources.zookeeperPodName(cluster, i) + ".key", cert.keyAsBase64String());
            secretData.put(KafkaResources.zookeeperPodName(cluster, i) + ".crt", cert.certAsBase64String());
            secretData.put(KafkaResources.zookeeperPodName(cluster, i) + ".p12", cert.keyStoreAsBase64String());
            secretData.put(KafkaResources.zookeeperPodName(cluster, i) + ".password", cert.storePasswordAsBase64String());
        }

        return createSecret(KafkaResources.zookeeperSecretName(cluster), secretData,
                Collections.singletonMap(clusterCa.caCertGenerationAnnotation(), String.valueOf(clusterCa.certGeneration())));
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
                .withSecurityContext(securityProvider.zooKeeperContainerSecurityContext(new ContainerSecurityProviderContextImpl(storage, templateZookeeperContainerSecurityContext)))
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

        if (isJmxEnabled) {
            varList.add(buildEnvVar(ENV_VAR_ZOOKEEPER_JMX_ENABLED, "true"));
            if (isJmxAuthenticated) {
                varList.add(buildEnvVarFromSecret(ENV_VAR_ZOOKEEPER_JMX_USERNAME, KafkaResources.zookeeperJmxSecretName(cluster), SECRET_JMX_USERNAME_KEY));
                varList.add(buildEnvVarFromSecret(ENV_VAR_ZOOKEEPER_JMX_PASSWORD, KafkaResources.zookeeperJmxSecretName(cluster), SECRET_JMX_PASSWORD_KEY));
            }
        }

        ModelUtils.heapOptions(varList, 75, 2L * 1024L * 1024L * 1024L, getJvmOptions(), getResources());
        ModelUtils.jvmPerformanceOptions(varList, getJvmOptions());
        ModelUtils.jvmSystemProperties(varList, getJvmOptions());
        varList.add(buildEnvVar(ENV_VAR_ZOOKEEPER_CONFIGURATION, configuration.getConfiguration()));

        // Add shared environment variables used for all containers
        varList.addAll(getRequiredEnvVars());

        addContainerEnvsToExistingEnvs(varList, templateZookeeperContainerEnvVars);

        return varList;
    }

    private List<ServicePort> getServicePortList() {
        List<ServicePort> portList = new ArrayList<>(4);
        portList.add(createServicePort(CLIENT_TLS_PORT_NAME, CLIENT_TLS_PORT, CLIENT_TLS_PORT, "TCP"));
        portList.add(createServicePort(CLUSTERING_PORT_NAME, CLUSTERING_PORT, CLUSTERING_PORT, "TCP"));
        portList.add(createServicePort(LEADER_ELECTION_PORT_NAME, LEADER_ELECTION_PORT, LEADER_ELECTION_PORT, "TCP"));

        if (isJmxEnabled) {
            portList.add(createServicePort(JMX_PORT_NAME, JMX_PORT, JMX_PORT, "TCP"));
        }

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

    /**
     * Generates list of non-data volumes used by ZooKeeper Pods. This includes tmp volumes, mounted secrets and config
     * maps.
     *
     * @param isOpenShift   Indicates whether we are on OpenShift or not
     *
     * @return              List of nondata volumes used by the ZooKeeper pods
     */
    private List<Volume> getNonDataVolumes(boolean isOpenShift) {
        List<Volume> volumeList = new ArrayList<>(4);

        volumeList.add(createTempDirVolume());
        volumeList.add(VolumeUtils.createConfigMapVolume(logAndMetricsConfigVolumeName, ancillaryConfigMapName));
        volumeList.add(VolumeUtils.createSecretVolume(ZOOKEEPER_NODE_CERTIFICATES_VOLUME_NAME, KafkaResources.zookeeperSecretName(cluster), isOpenShift));
        volumeList.add(VolumeUtils.createSecretVolume(ZOOKEEPER_CLUSTER_CA_VOLUME_NAME, AbstractModel.clusterCaCertSecretName(cluster), isOpenShift));

        return volumeList;
    }

    /**
     * Generates a list of volumes used by StatefulSet. For StatefulSet, it needs to include only ephemeral data
     * volumes. Persistent claim volumes are generated directly by StatefulSet.
     *
     * @param isOpenShift   Flag whether we are on OpenShift or not
     *
     * @return              List of volumes to be included in the StatefulSet pod template
     */
    private List<Volume> getStatefulSetVolumes(boolean isOpenShift) {
        List<Volume> volumeList = new ArrayList<>(5);

        volumeList.addAll(VolumeUtils.createStatefulSetVolumes(storage, false));
        volumeList.addAll(getNonDataVolumes(isOpenShift));

        return volumeList;
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

        volumeList.addAll(VolumeUtils.createPodSetVolumes(podName, storage, false));
        volumeList.addAll(getNonDataVolumes(isOpenShift));

        return volumeList;
    }

    /**
     * Creates a list of Persistent Volume Claim templates for use in StatefulSets
     *
     * @return  List of Persistent Volume Claim Templates
     */
    /* test */ List<PersistentVolumeClaim> getPersistentVolumeClaimTemplates() {
        return VolumeUtils.createPersistentVolumeClaimTemplates(storage, false);
    }

    public List<PersistentVolumeClaim> generatePersistentVolumeClaims() {
        return createPersistentVolumeClaims(storage, false);
    }

    private List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> volumeMountList = new ArrayList<>(5);

        volumeMountList.add(createTempDirVolumeMount());
        // ZooKeeper uses mount path which is different from the one used by Kafka.
        // As a result it cannot use VolumeUtils.getVolumeMounts and creates the volume mount directly
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

    /**
     * Generates the PodDisruptionBudgetV1Beta1.
     *
     * @return The PodDisruptionBudgetV1Beta1.
     */
    public io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudget generatePodDisruptionBudgetV1Beta1() {
        return createPodDisruptionBudgetV1Beta1();
    }

    /**
     * Generates the PodDisruptionBudget for operator managed pods.
     *
     * @return The PodDisruptionBudget.
     */
    public PodDisruptionBudget generateCustomControllerPodDisruptionBudget() {
        return createCustomControllerPodDisruptionBudget();
    }

    /**
     * Generates the PodDisruptionBudget V1Beta1 for operator managed pods.
     *
     * @return The PodDisruptionBudget v1beta1.
     */
    public io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudget generateCustomControllerPodDisruptionBudgetV1Beta1() {
        return createCustomControllerPodDisruptionBudgetV1Beta1();
    }

    @Override
    protected String getDefaultLogConfigFileName() {
        return "zookeeperDefaultLoggingProperties";
    }

    /**
     * Sets the image used for ZooKeeper. This method is called from outside the ZooKeeper model. It overrides the
     * method from the super class to make it public.
     *
     * @param image Image which should be used by ZooKeeper cluster
     */
    @Override
    public void setImage(String image) {
        super.setImage(image);
    }

    @Override
    public String getServiceAccountName() {
        return KafkaResources.zookeeperStatefulSetName(cluster);
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

    /**
     * Generate the Secret containing the username and password to secure the jmx port on the zookeeper nodes
     *
     * @param currentSecret The existing Secret with the current JMX credentials. Null if no secret exists yet.
     *
     * @return The generated Secret
     */
    public Secret generateJmxSecret(Secret currentSecret) {
        if (isJmxAuthenticated) {
            PasswordGenerator passwordGenerator = new PasswordGenerator(16);
            Map<String, String> data = new HashMap<>(2);

            if (currentSecret != null && currentSecret.getData() != null)  {
                data.put(SECRET_JMX_USERNAME_KEY, currentSecret.getData().computeIfAbsent(SECRET_JMX_USERNAME_KEY, (key) -> Util.encodeToBase64(passwordGenerator.generate())));
                data.put(SECRET_JMX_PASSWORD_KEY, currentSecret.getData().computeIfAbsent(SECRET_JMX_PASSWORD_KEY, (key) -> Util.encodeToBase64(passwordGenerator.generate())));
            } else {
                data.put(SECRET_JMX_USERNAME_KEY, Util.encodeToBase64(passwordGenerator.generate()));
                data.put(SECRET_JMX_PASSWORD_KEY, Util.encodeToBase64(passwordGenerator.generate()));
            }

            return createJmxSecret(KafkaResources.zookeeperJmxSecretName(cluster), data);
        } else {
            return null;
        }
    }
}
