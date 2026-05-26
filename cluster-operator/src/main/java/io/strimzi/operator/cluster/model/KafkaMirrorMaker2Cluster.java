/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.strimzi.api.kafka.model.common.ClientTls;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.connect.KafkaConnectSpec;
import io.strimzi.api.kafka.model.connect.KafkaConnectSpecBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2MirrorSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Spec;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Kafka Mirror Maker 2 model
 */
public class KafkaMirrorMaker2Cluster extends KafkaConnectCluster {
    protected static final String COMPONENT_TYPE = "kafka-mirror-maker-2";

    // Kafka MirrorMaker 2 connector configuration keys (EnvVariables)
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_2_CLUSTERS = "KAFKA_MIRRORMAKER_2_CLUSTERS";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_2_TRUSTED_CERTS_CLUSTERS = "KAFKA_MIRRORMAKER_2_TRUSTED_CERTS_CLUSTERS";
    protected static final String CO_ENV_VAR_CUSTOM_MIRROR_MAKER2_POD_LABELS = "STRIMZI_CUSTOM_KAFKA_MIRROR_MAKER2_LABELS";

    protected static final String MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT = "/opt/kafka/mm2-certs/";
    protected static final String MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT = "/opt/kafka/mm2-password/";

    private static final Map<String, String> DEFAULT_POD_LABELS = new HashMap<>();
    static {
        String value = System.getenv(CO_ENV_VAR_CUSTOM_MIRROR_MAKER2_POD_LABELS);
        if (value != null) {
            DEFAULT_POD_LABELS.putAll(Util.parseMap(value));
        }
    }

    /**
     * Default Strimzi Metrics Reporter allowlist for MirrorMaker 2.
     * Inherits from Kafka Connect metrics and adds MM2-specific metrics.
     * Check example dashboard compatibility in case of changes to existing regexes.
     */
    private static final List<String> DEFAULT_METRICS_ALLOW_LIST;
    static {
        List<String> list = new ArrayList<>(KafkaConnectCluster.DEFAULT_METRICS_ALLOW_LIST);
        list.add("kafka_connect_mirror_mirrorcheckpointconnector.*");
        list.add("kafka_connect_mirror_mirrorsourceconnector.*");
        DEFAULT_METRICS_ALLOW_LIST = Collections.unmodifiableList(list);
    }

    private KafkaMirrorMaker2Connectors connectors;
    private Collection<KafkaMirrorMaker2ClusterSpec> clusters;

    /**
     * Constructor
     *
     * @param reconciliation The reconciliation
     * @param resource Kubernetes resource with metadata containing the namespace and cluster name
     * @param sharedEnvironmentProvider Shared environment provider
     */
    private KafkaMirrorMaker2Cluster(Reconciliation reconciliation, KafkaMirrorMaker2 resource, SharedEnvironmentProvider sharedEnvironmentProvider) {
        super(reconciliation, resource, KafkaMirrorMaker2Resources.componentName(resource.getMetadata().getName()), COMPONENT_TYPE, sharedEnvironmentProvider);

        this.serviceName = KafkaMirrorMaker2Resources.serviceName(cluster);
        this.connectConfigMapName = KafkaMirrorMaker2Resources.configMapName(cluster);
    }

    /**
     * Creates an instance of KafkaMirrorMaker2Cluster from CRD definition.
     *
     * @param reconciliation    The reconciliation
     * @param kafkaMirrorMaker2 The Custom Resource based on which the cluster model should be created.
     * @param versions The image versions for MirrorMaker 2 clusters.
     * @param sharedEnvironmentProvider Shared environment provider.
     * @return The MirrorMaker 2 cluster model.
     */
    public static KafkaMirrorMaker2Cluster fromCrd(Reconciliation reconciliation,
                                                   KafkaMirrorMaker2 kafkaMirrorMaker2,
                                                   KafkaVersion.Lookup versions,
                                                   SharedEnvironmentProvider sharedEnvironmentProvider) {
        ModelUtils.validateComputeResources(kafkaMirrorMaker2.getSpec().getResources(), "KafkaMirrorMaker2.spec.resources");
        KafkaMirrorMaker2Cluster result = new KafkaMirrorMaker2Cluster(reconciliation, kafkaMirrorMaker2, sharedEnvironmentProvider);

        result.clusters = clusters(kafkaMirrorMaker2);
        result.connectors = KafkaMirrorMaker2Connectors.fromCrd(reconciliation, kafkaMirrorMaker2);
        result.configuration = new KafkaMirrorMaker2Configuration(reconciliation, kafkaMirrorMaker2.getSpec().getTarget().getConfig().entrySet());
        // Image needs to be set here to properly use the default MM2 container image if needed
        result.image = versions.kafkaMirrorMaker2Version(kafkaMirrorMaker2.getSpec().getImage(), kafkaMirrorMaker2.getSpec().getVersion());

        return fromSpec(reconciliation, buildKafkaConnectSpec(kafkaMirrorMaker2.getSpec()), versions, result);
    }

    /**
     * Returns a list of target and source clusters. The list is later used to generate the volumes, volume mounts, and so on.
     * These are created the same way for source and target clusters, so we can afford to just mix them all into one
     * list in this method.
     *
     * @param kafkaMirrorMaker2 The KafkaMirrorMaker2 resource using the new API layout
     *
     * @return  List of source and target clusters
     */
    private static List<KafkaMirrorMaker2ClusterSpec> clusters(KafkaMirrorMaker2 kafkaMirrorMaker2)    {
        // The resource is already converted to the new API, so we do not need to check both APIs
        List<KafkaMirrorMaker2ClusterSpec> clusters = new ArrayList<>();

        // We add the target cluster
        clusters.add(kafkaMirrorMaker2.getSpec().getTarget());
        clusters.addAll(kafkaMirrorMaker2.getSpec().getMirrors().stream().map(KafkaMirrorMaker2MirrorSpec::getSource).toList());

        return clusters;
    }

    /**
     * Builds the KafkaConnectSpec instance out of the KafkaMirrorMaker2Spec. This is later used to deploy the
     * underlying Kafka Connect cluster.
     *
     * @param spec              KafkaMirrorMaker2Spec instance
     *
     * @return  KafkaConnectSpec built out of the KafkaMirrorMaker2Spec instance
     */
    private static KafkaConnectSpec buildKafkaConnectSpec(KafkaMirrorMaker2Spec spec) {
        return new KafkaConnectSpecBuilder()
                // Target cluster defined fields from .spec.target
                .withBootstrapServers(spec.getTarget().getBootstrapServers())
                .withGroupId(spec.getTarget().getGroupId())
                .withConfigStorageTopic(spec.getTarget().getConfigStorageTopic())
                .withOffsetStorageTopic(spec.getTarget().getOffsetStorageTopic())
                .withStatusStorageTopic(spec.getTarget().getStatusStorageTopic())
                .withTls(spec.getTarget().getTls())
                .withAuthentication(spec.getTarget().getAuthentication())
                .withConfig(spec.getTarget().getConfig())
                // Regular KafkaMirrorMaker2 fields fom its .spec
                .withLogging(spec.getLogging())
                .withReplicas(spec.getReplicas())
                .withVersion(spec.getVersion())
                .withImage(spec.getImage())
                .withResources(spec.getResources())
                .withLivenessProbe(spec.getLivenessProbe())
                .withReadinessProbe(spec.getReadinessProbe())
                .withJvmOptions(spec.getJvmOptions())
                .withJmxOptions(spec.getJmxOptions())
                .withMetricsConfig(spec.getMetricsConfig())
                .withClientRackInitImage(spec.getClientRackInitImage())
                .withRack(spec.getRack())
                .withTracing(spec.getTracing())
                .withTemplate(spec.getTemplate())
                .build();
    }

    /**
     * Gets the name of the ClusterRoleBinding for the init container.
     *
     * @return  Name of the ClusterRoleBinding
     */
    @Override
    public String getInitContainerClusterRoleBindingName() {
        return KafkaMirrorMaker2Resources.initContainerClusterRoleBindingName(cluster, namespace);
    }

    /**
     * Gets the name of the RoleBinding for Mirror Maker 2.
     *
     * @return  Name of the RoleBinding
     */
    @Override
    public String getRoleBindingName() {
        return KafkaMirrorMaker2Resources.mm2RoleBindingName(getCluster());
    }

    /**
     * Gets the list of Secrets the Pods need to access through the Kubernetes API.
     * Includes Connect authentication secrets and per-cluster authentication secrets.
     *
     * @return  The list of Secrets the Pods will need access to through Kubernetes API
     */
    @Override
    public Set<String> secretsToAllowAccessTo() {
        Set<String> secretNames = super.secretsToAllowAccessTo();

        // Add per-cluster secrets
        for (KafkaMirrorMaker2ClusterSpec mirrorMaker2Cluster : clusters) {

            ClientTls clusterTls = mirrorMaker2Cluster.getTls();
            KafkaClientAuthentication clusterAuth = mirrorMaker2Cluster.getAuthentication();

            secretNames.addAll(AuthenticationUtils.getAuthenticationSecretsToAccess(
                    clusterTls,
                    clusterAuth,
                    KafkaConnectResources.internalTlsTrustedCertsSecretName(cluster)));
        }

        return secretNames;
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = super.getEnvVars();

        final StringBuilder clusterAliases = new StringBuilder();

        for (KafkaMirrorMaker2ClusterSpec mirrorMaker2Cluster : clusters) {
            String clusterAlias = mirrorMaker2Cluster.getAlias();

            if (!clusterAliases.isEmpty()) {
                clusterAliases.append(";");
            }
            clusterAliases.append(clusterAlias);
        }

        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_CLUSTERS, clusterAliases.toString()));

        JvmOptionUtils.jvmSystemProperties(varList, jvmOptions);

        return varList;
    }

    /**
     * The command for running Connect has to be passed through a method so that we can handle different run commands
     * for Connect and Mirror Maker 2 (which inherits from this class) without duplicating the whole container creation.
     *
     * @return  Command for starting the Kafka Mirror Maker 2 container
     */
    @Override
    protected String getCommand() {
        return "/opt/kafka/kafka_connect_run.sh";
    }

    /**
     * The default labels Connect pod uses have to be passed through a method so that we can handle different labels for
     * Connect and Mirror Maker 2 (which inherits from this class) without duplicating the whole pod creation.
     *
     * @return Default Pod Labels for Kafka Mirror Maker 2
     */
    @Override
    protected Map<String, String> defaultPodLabels() {
        return DEFAULT_POD_LABELS;
    }

    /**
     * Gets the Mirror Maker 2 Connectors model.
     *
     * @return  Returns the Mirror Maker 2 Connectors model
     */
    public KafkaMirrorMaker2Connectors connectors() {
        return connectors;
    }

    /**
     * Gets the collection of Mirror Maker 2 cluster specifications.
     *
     * @return  Returns the Mirror Maker 2 Clusters
     */
    public Collection<KafkaMirrorMaker2ClusterSpec> clusters() {
        return clusters;
    }

    /**
     * Override the default metrics allow list to include MM2-specific metrics.
     * MirrorMaker 2 needs both Kafka Connect metrics and MM2-specific metrics.
     *
     * @return List of default metrics allow list patterns for MirrorMaker 2
     */
    @Override
    protected List<String> getDefaultMetricsAllowList() {
        return DEFAULT_METRICS_ALLOW_LIST;
    }
}
