/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.rbac.PolicyRule;
import io.fabric8.kubernetes.api.model.rbac.PolicyRuleBuilder;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.strimzi.api.kafka.model.common.CertSecretSource;
import io.strimzi.api.kafka.model.common.ClientTls;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationOAuth;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationTls;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.connect.KafkaConnectSpec;
import io.strimzi.api.kafka.model.connect.KafkaConnectSpecBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Spec;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.InvalidResourceException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Kafka Mirror Maker 2 model
 */
public class KafkaMirrorMaker2Cluster extends KafkaConnectCluster {
    protected static final String COMPONENT_TYPE = "kafka-mirror-maker-2";

    // Kafka MirrorMaker 2 connector configuration keys (EnvVariables)
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

    private final KafkaMirrorMaker2Connectors connectors;

    private List<KafkaMirrorMaker2ClusterSpec> clusters;

    /**
     * Constructor
     *
     * @param reconciliation The reconciliation
     * @param resource Kubernetes resource with metadata containing the namespace and cluster name
     * @param sharedEnvironmentProvider Shared environment provider
     */
    private KafkaMirrorMaker2Cluster(Reconciliation reconciliation, KafkaMirrorMaker2 resource, SharedEnvironmentProvider sharedEnvironmentProvider) {
        super(reconciliation, resource, KafkaMirrorMaker2Resources.componentName(resource.getMetadata().getName()), COMPONENT_TYPE, sharedEnvironmentProvider);

        this.connectors = KafkaMirrorMaker2Connectors.fromCrd(reconciliation, resource);
        this.serviceName = KafkaMirrorMaker2Resources.serviceName(cluster);
        this.connectConfigMapName = KafkaMirrorMaker2Resources.configMapName(cluster);
    }

    /**
     * Creates instance of KafkaMirrorMaker2Cluster from CRD definition.
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
        KafkaMirrorMaker2Cluster result = new KafkaMirrorMaker2Cluster(reconciliation, kafkaMirrorMaker2, sharedEnvironmentProvider);
        KafkaMirrorMaker2Spec spec = kafkaMirrorMaker2.getSpec();

        result.image = versions.kafkaMirrorMaker2Version(spec.getImage(), spec.getVersion());

        List<KafkaMirrorMaker2ClusterSpec> clustersList = ModelUtils.asListOrEmptyList(spec.getClusters());
        result.setClusters(clustersList);

        KafkaMirrorMaker2ClusterSpec connectCluster = new KafkaMirrorMaker2ClusterSpecBuilder().build();
        String connectClusterAlias = spec.getConnectCluster();        
        if (connectClusterAlias != null) {
            connectCluster = clustersList.stream()
                    .filter(clustersListItem -> spec.getConnectCluster().equals(clustersListItem.getAlias()))
                    .findFirst()
                    .orElseThrow(() -> new InvalidResourceException("connectCluster with alias " + connectClusterAlias + " cannot be found in the list of clusters at spec.clusters"));
        }        
        result.configuration = new KafkaMirrorMaker2Configuration(reconciliation, connectCluster.getConfig().entrySet());

        return fromSpec(reconciliation, buildKafkaConnectSpec(spec, connectCluster), versions, result);
    }

    @SuppressWarnings("deprecation") // External Configuration is deprecated
    private static KafkaConnectSpec buildKafkaConnectSpec(KafkaMirrorMaker2Spec spec, KafkaMirrorMaker2ClusterSpec connectCluster) {

        ClientTls connectTls = null;
        ClientTls mirrorMaker2ConnectClusterTls = connectCluster.getTls();
        if (mirrorMaker2ConnectClusterTls != null) {
            connectTls = new ClientTls();
            connectTls.setTrustedCertificates(mirrorMaker2ConnectClusterTls.getTrustedCertificates());
            for (Entry<String, Object> entry : mirrorMaker2ConnectClusterTls.getAdditionalProperties().entrySet()) {
                connectTls.setAdditionalProperty(entry.getKey(), entry.getValue());
            }
        }

        return new KafkaConnectSpecBuilder()
                .withBootstrapServers(connectCluster.getBootstrapServers())
                .withTls(connectTls)
                .withAuthentication(connectCluster.getAuthentication())
                .withConfig(connectCluster.getConfig())
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

    @Override
    protected List<Volume> getVolumes(boolean isOpenShift) {
        List<Volume> volumeList = super.getVolumes(isOpenShift);

        for (KafkaMirrorMaker2ClusterSpec mirrorMaker2Cluster: clusters) {
            AuthenticationUtils.configureClientAuthenticationVolumes(mirrorMaker2Cluster.getAuthentication(), volumeList, KafkaConnectResources.internalOauthTrustedCertsSecretName(cluster), isOpenShift, mirrorMaker2Cluster.getAlias() + "-", true);
        }
        return volumeList;
    }

    @Override
    protected List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> volumeMountList = super.getVolumeMounts();

        for (KafkaMirrorMaker2ClusterSpec mirrorMaker2Cluster: clusters) {
            String alias = mirrorMaker2Cluster.getAlias();
            String tlsVolumeMountPath =  buildClusterVolumeMountPath(MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT, alias);
            String passwordVolumeMountPath =  buildClusterVolumeMountPath(MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT, alias);
            String oauthTlsVolumeMountPath =  buildClusterVolumeMountPath(MIRRORMAKER_2_OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT, alias);
            String oauthVolumeMountPath =  buildClusterVolumeMountPath(MIRRORMAKER_2_OAUTH_SECRETS_BASE_VOLUME_MOUNT, alias);
            AuthenticationUtils.configureClientAuthenticationVolumeMounts(mirrorMaker2Cluster.getAuthentication(), volumeMountList, tlsVolumeMountPath, passwordVolumeMountPath, oauthTlsVolumeMountPath, KafkaConnectResources.internalOauthTrustedCertsSecretName(cluster), mirrorMaker2Cluster.getAlias() + '-', true, oauthVolumeMountPath);
        }

        return volumeMountList;
    }
    @Override
    public Role generateRole() {
        List<String> certSecretNames = new ArrayList<>();
        for (KafkaMirrorMaker2ClusterSpec mirrorMaker2Cluster: clusters) {
            ClientTls mirrorTls = mirrorMaker2Cluster.getTls();
            if (mirrorTls != null && mirrorTls.getTrustedCertificates() != null && !mirrorTls.getTrustedCertificates().isEmpty()) {
                certSecretNames.add(KafkaConnectResources.internalTlsTrustedCertsSecretName(cluster));
            }

            KafkaClientAuthentication mirrorAuthentication = mirrorMaker2Cluster.getAuthentication();

            if (mirrorAuthentication instanceof KafkaClientAuthenticationTls tlsAuth && tlsAuth.getCertificateAndKey() != null) {
                certSecretNames.add(tlsAuth.getCertificateAndKey().getSecretName());
            } else if (mirrorAuthentication instanceof KafkaClientAuthenticationOAuth oauth && oauth.getTlsTrustedCertificates() != null
                    && !oauth.getTlsTrustedCertificates().isEmpty()) {
                certSecretNames.add(KafkaConnectResources.internalOauthTrustedCertsSecretName(cluster));
            }
        }

        List<PolicyRule> rules = List.of(new PolicyRuleBuilder()
                .withApiGroups("")
                .withResources("secrets")
                .withVerbs("get")
                .withResourceNames(certSecretNames)
                .build());

        return RbacUtils.createRole(componentName, namespace, rules, labels, ownerReference, null);
    }

    private String buildClusterVolumeMountPath(final String baseVolumeMount,  final String path) {
        return baseVolumeMount + path + "/";
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = super.getEnvVars();

        JvmOptionUtils.jvmSystemProperties(varList, jvmOptions);

        return varList;
    }

    @Override
    public Set<String> getTlsCertsSecrets() {
        Set<String> secretsToCopy = new HashSet<>();
        for (KafkaMirrorMaker2ClusterSpec mirrorMaker2Cluster: clusters) {
            ClientTls tls = mirrorMaker2Cluster.getTls();
            if (tls != null && tls.getTrustedCertificates() != null) {
                secretsToCopy.addAll(tls.getTrustedCertificates().stream().map(CertSecretSource::getSecretName).toList());
            }
        }
        return secretsToCopy;
    }

    @Override
    public Set<String> getOauthTlsCertsSecrets() {
        Set<String> secretsToCopy = new HashSet<>();
        for (KafkaMirrorMaker2ClusterSpec mirrorMaker2Cluster: clusters) {
            KafkaClientAuthentication mm2Auth = mirrorMaker2Cluster.getAuthentication();
            if (mm2Auth instanceof KafkaClientAuthenticationOAuth oauth && oauth.getTlsTrustedCertificates() != null) {
                secretsToCopy.addAll(oauth.getTlsTrustedCertificates().stream().map(CertSecretSource::getSecretName).toList());
            }
        }
        return secretsToCopy;
    }

    /**
     * Sets the configured clusters for mirroring
     *
     * @param clusters The list of cluster configurations
     */
    protected void setClusters(List<KafkaMirrorMaker2ClusterSpec> clusters) {
        this.clusters = clusters;
    }

    /**
     * The default labels Connect pod has to be passed through a method so that we can handle different labels for
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
}
