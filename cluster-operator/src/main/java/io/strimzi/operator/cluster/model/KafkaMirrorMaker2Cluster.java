/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.ClientTls;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationOAuth;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationTls;
import io.strimzi.api.kafka.model.connect.KafkaConnectSpec;
import io.strimzi.api.kafka.model.connect.KafkaConnectSpecBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2MirrorSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Spec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2TargetClusterSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2TargetClusterSpecBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.InvalidResourceException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Kafka Mirror Maker 2 model
 */
public class KafkaMirrorMaker2Cluster extends KafkaConnectCluster {
    protected static final String COMPONENT_TYPE = "kafka-mirror-maker-2";

    // Kafka MirrorMaker 2 connector configuration keys (EnvVariables)
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_2_CLUSTERS = "KAFKA_MIRRORMAKER_2_CLUSTERS";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_2_TRUSTED_CERTS_CLUSTERS = "KAFKA_MIRRORMAKER_2_TRUSTED_CERTS_CLUSTERS";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_2_TLS_AUTH_CLUSTERS = "KAFKA_MIRRORMAKER_2_TLS_AUTH_CLUSTERS";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_2_TLS_AUTH_CERTS_CLUSTERS = "KAFKA_MIRRORMAKER_2_TLS_AUTH_CERTS_CLUSTERS";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_2_TLS_AUTH_KEYS_CLUSTERS = "KAFKA_MIRRORMAKER_2_TLS_AUTH_KEYS_CLUSTERS";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_2_OAUTH_TRUSTED_CERTS_CLUSTERS = "KAFKA_MIRRORMAKER_2_OAUTH_TRUSTED_CERTS_CLUSTERS";
    protected static final String CO_ENV_VAR_CUSTOM_MIRROR_MAKER2_POD_LABELS = "STRIMZI_CUSTOM_KAFKA_MIRROR_MAKER2_LABELS";

    protected static final String MIRRORMAKER_2_OAUTH_SECRETS_BASE_VOLUME_MOUNT = "/opt/kafka/mm2-oauth/";
    protected static final String MIRRORMAKER_2_OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT = "/opt/kafka/mm2-oauth-certs/";
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
        KafkaMirrorMaker2 updatedMirrorMaker2 = validateAndUpdateToNewAPI(kafkaMirrorMaker2);

        KafkaMirrorMaker2Cluster result = new KafkaMirrorMaker2Cluster(reconciliation, updatedMirrorMaker2, sharedEnvironmentProvider);

        result.clusters = clusters(updatedMirrorMaker2);
        result.connectors = KafkaMirrorMaker2Connectors.fromCrd(reconciliation, updatedMirrorMaker2);
        result.configuration = new KafkaMirrorMaker2Configuration(reconciliation, updatedMirrorMaker2.getSpec().getTarget().getConfig().entrySet());
        // Image needs to be set here to properly use the default MM2 container image if needed
        result.image = versions.kafkaMirrorMaker2Version(updatedMirrorMaker2.getSpec().getImage(), updatedMirrorMaker2.getSpec().getVersion());

        return fromSpec(reconciliation, buildKafkaConnectSpec(updatedMirrorMaker2.getSpec()), versions, result);
    }

    /**
     * Validates the KafkaMirrorMaker2 custom resource and converts it into the new API layout. It creates and updates
     * a copy of the resource to avoid changing the structure of the actual Kubernetes resource.
     *
     * @param kafkaMirrorMaker2     The KafkaMirrorMaker2 resource to validate and rebuild
     *
     * @return  Updated copy of the KafkaMirrorMaker2 resource with the new API layout
     */
    @SuppressWarnings("deprecation") // The clusters, sourceCluster, and targetCluster fields are deprecated
    /* test */ static KafkaMirrorMaker2 validateAndUpdateToNewAPI(KafkaMirrorMaker2 kafkaMirrorMaker2) {
        // We have to create a copy of the CR so that we can refactor it to the new API layout
        // That allows us to have a cleaner code in the model classes without breaking the assembly operator
        KafkaMirrorMaker2 updated = new KafkaMirrorMaker2Builder(kafkaMirrorMaker2).build();

        if (updated.getSpec() == null)    {
            throw new InvalidResourceException(".spec section is required for KafkaMirrorMaker2 resource");
        } else if (updated.getSpec().getMirrors() == null || updated.getSpec().getMirrors().isEmpty())  {
            throw new InvalidResourceException(".spec.mirrors section is required in KafkaMirrorMaker2 resource");
        } else {
            if (updated.getSpec().getTarget() == null) {
                // The old .spec.connectCluster and .spec.clusters fields are used -> this path will be removed when we drop v1beta2 API
                if (updated.getSpec().getConnectCluster() != null
                        && updated.getSpec().getClusters() != null
                        && !updated.getSpec().getClusters().isEmpty()) {
                    KafkaMirrorMaker2ClusterSpec connectCluster = updated.getSpec().getClusters().stream()
                            .filter(clustersListItem -> updated.getSpec().getConnectCluster().equals(clustersListItem.getAlias()))
                            .findFirst()
                            .orElseThrow(() -> new InvalidResourceException("connectCluster with alias " + updated.getSpec().getConnectCluster() + " cannot be found in the list of clusters at .spec.clusters"));

                    KafkaMirrorMaker2TargetClusterSpec target = new KafkaMirrorMaker2TargetClusterSpecBuilder()
                            .withAlias(connectCluster.getAlias())
                            .withBootstrapServers(connectCluster.getBootstrapServers())
                            .withGroupId(extractAndRemoveValueFromConfig(connectCluster.getConfig(), "group.id", "mirrormaker2-cluster"))
                            .withConfigStorageTopic(extractAndRemoveValueFromConfig(connectCluster.getConfig(), "config.storage.topic", "mirrormaker2-cluster-configs"))
                            .withOffsetStorageTopic(extractAndRemoveValueFromConfig(connectCluster.getConfig(), "offset.storage.topic", "mirrormaker2-cluster-offsets"))
                            .withStatusStorageTopic(extractAndRemoveValueFromConfig(connectCluster.getConfig(), "status.storage.topic", "mirrormaker2-cluster-status"))
                            .withTls(connectCluster.getTls())
                            .withAuthentication(connectCluster.getAuthentication())
                            .withConfig(connectCluster.getConfig())
                            .build();
                    updated.getSpec().setTarget(target);
                } else {
                    throw new InvalidResourceException("Either .spec.target or .spec.connectCluster and .spec.clusters have to be specified");
                }
            }

            Map<String, KafkaMirrorMaker2ClusterSpec> clusters = updated.getSpec().getClusters() != null ? updated.getSpec().getClusters().stream().collect(Collectors.toMap(KafkaMirrorMaker2ClusterSpec::getAlias, Function.identity())) : Map.of();
            Set<String> aliases = new HashSet<>();
            aliases.add(updated.getSpec().getTarget().getAlias()); // We add the target cluster alias to the set of aliases to check for duplicates

            updated.getSpec().getMirrors().forEach(mirror -> {
                if (mirror.getSource() == null && mirror.getSourceCluster() == null) {
                    throw new InvalidResourceException("Both .spec.mirrors[].source and .spec.mirrors[].sourceCluster are missing. Cannot determine source cluster.");
                } else if (mirror.getTargetCluster() != null && !mirror.getTargetCluster().equals(updated.getSpec().getTarget().getAlias()))    {
                    throw new InvalidResourceException("The .spec.mirrors[].targetCluster alias is not the same as the actual target cluster alias.");
                } else if (mirror.getSource() == null) {
                    KafkaMirrorMaker2ClusterSpec source = clusters.get(mirror.getSourceCluster());
                    if (source == null) {
                        throw new InvalidResourceException(".spec.mirrors[].sourceCluster is set to an non-existent alias. Cannot determine source cluster.");
                    } else {
                        mirror.setSource(source);
                    }
                }

                // We check for duplicate aliases here
                aliases.add(mirror.getSource().getAlias());

                // Reset the old fields
                mirror.setSourceCluster(null);
                mirror.setTargetCluster(null);
            });

            // We check for any duplicate aliases used
            if (aliases.size() != updated.getSpec().getMirrors().size() + 1)    {
                throw new InvalidResourceException("The target and source cluster aliases must be unique.");
            }

            updated.getSpec().setClusters(null);
            updated.getSpec().setConnectCluster(null);

            return updated;
        }
    }

    /**
     * Utility method to help with backward compatibility between the old configuration and a new configuration. This
     * should be removed once v1beta2 API is dropped, and we use only v1.
     *
     * @param configuration     Configuration
     * @param configKey         Configuration key
     * @param defaultConfig     Default configuration value
     *
     * @return  String with the value that should be used.
     */
    private static String extractAndRemoveValueFromConfig(Map<String, Object> configuration, String configKey, String defaultConfig) {
        if (configuration != null) {
            String config = (String) configuration.getOrDefault(configKey, defaultConfig);
            configuration.remove(configKey);
            return config;
        } else {
            return defaultConfig;
        }
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
    @SuppressWarnings("deprecation") // External Configuration, connectCluster, and clusters are deprecated
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
                .withExternalConfiguration(spec.getExternalConfiguration())
                .build();
    }

    /**
     * @return  Name of the ClusterRoleBinding
     */
    @Override
    public String getInitContainerClusterRoleBindingName() {
        return KafkaMirrorMaker2Resources.initContainerClusterRoleBindingName(cluster, namespace);
    }

    /**
     * @return  Name of the RoleBinding
     */
    @Override
    public String getRoleBindingName() {
        return KafkaMirrorMaker2Resources.mm2RoleBindingName(getCluster());
    }

    @Override
    protected List<Volume> getVolumes(boolean isOpenShift) {
        List<Volume> volumeList = super.getVolumes(isOpenShift);

        for (KafkaMirrorMaker2ClusterSpec mirrorMaker2Cluster: clusters) {
            String alias = mirrorMaker2Cluster.getAlias();
            ClientTls tls = mirrorMaker2Cluster.getTls();

            if (tls != null) {
                CertUtils.createTrustedCertificatesVolumes(volumeList, tls.getTrustedCertificates(), isOpenShift, alias);
            }

            AuthenticationUtils.configurePKCS12ClientAuthenticationVolumes(mirrorMaker2Cluster.getAuthentication(), volumeList, mirrorMaker2Cluster.getAlias() + "-oauth-certs", isOpenShift, mirrorMaker2Cluster.getAlias() + '-',  true);
        }
        return volumeList;
    }

    @Override
    protected List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> volumeMountList = super.getVolumeMounts();

        for (KafkaMirrorMaker2ClusterSpec mirrorMaker2Cluster: clusters) {
            String alias = mirrorMaker2Cluster.getAlias();
            String tlsVolumeMountPath =  buildClusterVolumeMountPath(MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT, alias);

            ClientTls kafkaMirrorMaker2Tls = mirrorMaker2Cluster.getTls();
            if (kafkaMirrorMaker2Tls != null) {
                CertUtils.createTrustedCertificatesVolumeMounts(volumeMountList, kafkaMirrorMaker2Tls.getTrustedCertificates(), tlsVolumeMountPath, alias);
            }
            String passwordVolumeMountPath =  buildClusterVolumeMountPath(MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT, alias);
            String oauthTlsVolumeMountPath =  buildClusterVolumeMountPath(MIRRORMAKER_2_OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT, alias);
            String oauthVolumeMountPath =  buildClusterVolumeMountPath(MIRRORMAKER_2_OAUTH_SECRETS_BASE_VOLUME_MOUNT, alias);
            AuthenticationUtils.configurePKCS12ClientAuthenticationVolumeMounts(mirrorMaker2Cluster.getAuthentication(), volumeMountList, tlsVolumeMountPath, passwordVolumeMountPath, oauthTlsVolumeMountPath, mirrorMaker2Cluster.getAlias() + "-oauth-certs", mirrorMaker2Cluster.getAlias() + '-', true, oauthVolumeMountPath);
        }

        return volumeMountList;
    }

    private static String buildClusterVolumeMountPath(final String baseVolumeMount,  final String path) {
        return baseVolumeMount + path + "/";
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity", "deprecation"}) // OAuth authentication is deprecated
    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = super.getEnvVars();

        final StringBuilder clusterAliases = new StringBuilder();
        final StringBuilder clustersTrustedCerts = new StringBuilder();
        final StringBuilder clustersTlsAuthCerts = new StringBuilder();
        final StringBuilder clustersTlsAuthKeys = new StringBuilder();
        final StringBuilder clustersOauthTrustedCerts = new StringBuilder();

        for (KafkaMirrorMaker2ClusterSpec mirrorMaker2Cluster : clusters) {
            String clusterAlias = mirrorMaker2Cluster.getAlias();

            if (!clusterAliases.isEmpty()) {
                clusterAliases.append(";");
            }
            clusterAliases.append(clusterAlias);

            getClusterTrustedCerts(clustersTrustedCerts, mirrorMaker2Cluster, clusterAlias);

            KafkaClientAuthentication authentication = mirrorMaker2Cluster.getAuthentication();
            if (authentication != null) {
                if (authentication instanceof KafkaClientAuthenticationTls tlsAuth) {
                    if (tlsAuth.getCertificateAndKey() != null) {
                        appendCluster(clustersTlsAuthCerts, clusterAlias, () -> tlsAuth.getCertificateAndKey().getSecretName() + "/" + tlsAuth.getCertificateAndKey().getCertificate());
                        appendCluster(clustersTlsAuthKeys, clusterAlias, () -> tlsAuth.getCertificateAndKey().getSecretName() + "/" + tlsAuth.getCertificateAndKey().getKey());
                    }
                } else if (authentication instanceof KafkaClientAuthenticationOAuth oauth) {
                    if (oauth.getTlsTrustedCertificates() != null && !oauth.getTlsTrustedCertificates().isEmpty()) {
                        appendClusterOAuthTrustedCerts(clustersOauthTrustedCerts, clusterAlias, oauth.getTlsTrustedCertificates());
                    }
                }
            }
        }

        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_CLUSTERS, clusterAliases.toString()));

        if (!clustersTrustedCerts.isEmpty()) {
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_TRUSTED_CERTS_CLUSTERS, clustersTrustedCerts.toString()));
        }

        if (!clustersTlsAuthCerts.isEmpty() || !clustersTlsAuthKeys.isEmpty()) {
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_TLS_AUTH_CLUSTERS, "true"));
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_TLS_AUTH_CERTS_CLUSTERS, clustersTlsAuthCerts.toString()));
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_TLS_AUTH_KEYS_CLUSTERS, clustersTlsAuthKeys.toString()));
        }

        if (!clustersOauthTrustedCerts.isEmpty()) {
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_OAUTH_TRUSTED_CERTS_CLUSTERS, clustersOauthTrustedCerts.toString()));
        }

        JvmOptionUtils.jvmSystemProperties(varList, jvmOptions);

        return varList;
    }

    private static void getClusterTrustedCerts(final StringBuilder clustersTrustedCerts, KafkaMirrorMaker2ClusterSpec mirrorMaker2Cluster, String clusterAlias) {
        ClientTls tls = mirrorMaker2Cluster.getTls();
        if (tls != null) {
            List<CertSecretSource> trustedCertificates = tls.getTrustedCertificates();
   
            if (trustedCertificates != null && !trustedCertificates.isEmpty()) {
                if (!clustersTrustedCerts.isEmpty()) {
                    clustersTrustedCerts.append("\n");
                }
                clustersTrustedCerts.append(clusterAlias);
                clustersTrustedCerts.append("=");
                clustersTrustedCerts.append(CertUtils.trustedCertsEnvVar(trustedCertificates));
            }
        }
    }

    private static void appendClusterOAuthTrustedCerts(final StringBuilder clusters, String clusterAlias, List<CertSecretSource> trustedCerts) {
        appendCluster(clusters, clusterAlias, () -> CertUtils.trustedCertsEnvVar(trustedCerts));
    }

    private static void appendCluster(final StringBuilder clusters, String clusterAlias, Supplier<String> function) {
        if (!clusters.isEmpty()) {
            clusters.append("\n");
        }                   
        clusters.append(clusterAlias);
        clusters.append("=");
        clusters.append(function.get());
    }

    /**
     * The command for running Connect has to be passed through a method so that we can handle different run commands
     * for Connect and Mirror Maker 2 (which inherits from this class) without duplicating the whole container creation.
     *
     * @return  Command for starting the Kafka Mirror Maker 2 container
     */
    @Override
    protected String getCommand() {
        return "/opt/kafka/kafka_mirror_maker_2_run.sh";
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
     * @return  Returns the Mirror Maker 2 Connectors model
     */
    public KafkaMirrorMaker2Connectors connectors() {
        return connectors;
    }

    /**
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
