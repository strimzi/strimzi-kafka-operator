/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.GenericSecretSource;
import io.strimzi.api.kafka.model.KafkaConnectSpec;
import io.strimzi.api.kafka.model.KafkaConnectSpecBuilder;
import io.strimzi.api.kafka.model.ClientTls;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Spec;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationOAuth;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationPlain;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationScram;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationTls;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;

import static io.strimzi.operator.cluster.ClusterOperatorConfig.STRIMZI_DEFAULT_KAFKA_INIT_IMAGE;

/**
 * Kafka Mirror Maker 2 model
 */
public class KafkaMirrorMaker2Cluster extends KafkaConnectCluster {
    protected static final String COMPONENT_TYPE = "kafka-mirror-maker-2";

    // Kafka MirrorMaker 2.0 connector configuration keys (EnvVariables)
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_2_CLUSTERS = "KAFKA_MIRRORMAKER_2_CLUSTERS";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_2_TLS_CLUSTERS = "KAFKA_MIRRORMAKER_2_TLS_CLUSTERS";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_2_TRUSTED_CERTS_CLUSTERS = "KAFKA_MIRRORMAKER_2_TRUSTED_CERTS_CLUSTERS";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_2_TLS_AUTH_CLUSTERS = "KAFKA_MIRRORMAKER_2_TLS_AUTH_CLUSTERS";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_2_TLS_AUTH_CERTS_CLUSTERS = "KAFKA_MIRRORMAKER_2_TLS_AUTH_CERTS_CLUSTERS";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_2_TLS_AUTH_KEYS_CLUSTERS = "KAFKA_MIRRORMAKER_2_TLS_AUTH_KEYS_CLUSTERS";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_2_SASL_PASSWORD_FILES_CLUSTERS = "KAFKA_MIRRORMAKER_2_SASL_PASSWORD_FILES_CLUSTERS";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_2_OAUTH_TRUSTED_CERTS = "KAFKA_MIRRORMAKER_2_OAUTH_TRUSTED_CERTS";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_2_OAUTH_CLIENT_SECRETS_CLUSTERS = "KAFKA_MIRRORMAKER_2_OAUTH_CLIENT_SECRETS_CLUSTERS";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_2_OAUTH_ACCESS_TOKENS_CLUSTERS = "KAFKA_MIRRORMAKER_2_OAUTH_OAUTH_ACCESS_TOKENS_CLUSTERS";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_2_OAUTH_REFRESH_TOKENS_CLUSTERS = "KAFKA_MIRRORMAKER_2_OAUTH_REFRESH_TOKENS_CLUSTERS";
    protected static final String ENV_VAR_KAFKA_MIRRORMAKER_2_OAUTH_PASSWORDS_CLUSTERS = "KAFKA_MIRRORMAKER_2_OAUTH_PASSWORDS_CLUSTERS";
    protected static final String CO_ENV_VAR_CUSTOM_MIRROR_MAKER2_POD_LABELS = "STRIMZI_CUSTOM_KAFKA_MIRROR_MAKER2_LABELS";

    protected static final String MIRRORMAKER_2_OAUTH_SECRETS_BASE_VOLUME_MOUNT = "/opt/kafka/mm2-oauth/";
    protected static final String MIRRORMAKER_2_OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT = "/opt/kafka/mm2-oauth-certs/";
    protected static final String MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT = "/opt/kafka/mm2-certs/";
    protected static final String MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT = "/opt/kafka/mm2-password/";

    private List<KafkaMirrorMaker2ClusterSpec> clusters;

    private static final Map<String, String> DEFAULT_POD_LABELS = new HashMap<>();
    static {
        String value = System.getenv(CO_ENV_VAR_CUSTOM_MIRROR_MAKER2_POD_LABELS);
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
    private KafkaMirrorMaker2Cluster(Reconciliation reconciliation, HasMetadata resource) {
        super(reconciliation, resource, KafkaMirrorMaker2Resources.deploymentName(resource.getMetadata().getName()), COMPONENT_TYPE);

        this.serviceName = KafkaMirrorMaker2Resources.serviceName(cluster);
        this.ancillaryConfigMapName = KafkaMirrorMaker2Resources.metricsAndLogConfigMapName(cluster);
    }

    /**
     * Creates instance of KafkaMirrorMaker2Cluster from CRD definition.
     *
     * @param reconciliation    The reconciliation
     * @param kafkaMirrorMaker2 The Custom Resource based on which the cluster model should be created.
     * @param versions The image versions for MirrorMaker 2.0 clusters.
     * @return The MirrorMaker 2.0 cluster model.
     */
    public static KafkaMirrorMaker2Cluster fromCrd(Reconciliation reconciliation,
                                                   KafkaMirrorMaker2 kafkaMirrorMaker2,
                                                   KafkaVersion.Lookup versions) {
        KafkaMirrorMaker2Cluster cluster = new KafkaMirrorMaker2Cluster(reconciliation, kafkaMirrorMaker2);
        KafkaMirrorMaker2Spec spec = kafkaMirrorMaker2.getSpec();

        cluster.image = versions.kafkaMirrorMaker2Version(spec.getImage(), spec.getVersion());

        List<KafkaMirrorMaker2ClusterSpec> clustersList = ModelUtils.asListOrEmptyList(spec.getClusters());
        cluster.setClusters(clustersList);

        KafkaMirrorMaker2ClusterSpec connectCluster = new KafkaMirrorMaker2ClusterSpecBuilder().build();
        String connectClusterAlias = spec.getConnectCluster();        
        if (connectClusterAlias != null) {
            connectCluster = clustersList.stream()
                    .filter(clustersListItem -> spec.getConnectCluster().equals(clustersListItem.getAlias()))
                    .findFirst()
                    .orElseThrow(() -> new InvalidResourceException("connectCluster with alias " + connectClusterAlias + " cannot be found in the list of clusters at spec.clusters"));
        }        
        cluster.setConfiguration(new KafkaMirrorMaker2Configuration(reconciliation, connectCluster.getConfig().entrySet()));
        KafkaMirrorMaker2Cluster mm2 = fromSpec(reconciliation, buildKafkaConnectSpec(spec, connectCluster), versions, cluster);

        return mm2;
    }

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
                .withClientRackInitImage(System.getenv().getOrDefault(STRIMZI_DEFAULT_KAFKA_INIT_IMAGE, "quay.io/strimzi/operator:latest"))
                .withRack(spec.getRack())
                .withTracing(spec.getTracing())
                .withTemplate(spec.getTemplate())
                .withExternalConfiguration(spec.getExternalConfiguration())
                .build();

    }

    public String getInitContainerClusterRoleBindingName() {
        return KafkaMirrorMaker2Resources.initContainerClusterRoleBindingName(cluster, namespace);
    }

    @Override
    protected List<Volume> getVolumes(boolean isOpenShift) {
        List<Volume> volumeList = super.getVolumes(isOpenShift);

        for (KafkaMirrorMaker2ClusterSpec mirrorMaker2Cluster: clusters) {
            String alias = mirrorMaker2Cluster.getAlias();
            ClientTls tls = mirrorMaker2Cluster.getTls();

            if (tls != null) {
                VolumeUtils.createSecretVolume(volumeList, tls.getTrustedCertificates(), isOpenShift, alias);
            }
            AuthenticationUtils.configureClientAuthenticationVolumes(mirrorMaker2Cluster.getAuthentication(), volumeList, mirrorMaker2Cluster.getAlias() + "-oauth-certs", isOpenShift, mirrorMaker2Cluster.getAlias() + '-',  true);
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
                VolumeUtils.createSecretVolumeMount(volumeMountList, kafkaMirrorMaker2Tls.getTrustedCertificates(), tlsVolumeMountPath, alias);
            }
            String passwordVolumeMountPath =  buildClusterVolumeMountPath(MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT, alias);
            String oauthTlsVolumeMountPath =  buildClusterVolumeMountPath(MIRRORMAKER_2_OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT, alias);
            String oauthVolumeMountPath =  buildClusterVolumeMountPath(MIRRORMAKER_2_OAUTH_SECRETS_BASE_VOLUME_MOUNT, alias);
            AuthenticationUtils.configureClientAuthenticationVolumeMounts(mirrorMaker2Cluster.getAuthentication(), volumeMountList, tlsVolumeMountPath, passwordVolumeMountPath, oauthTlsVolumeMountPath, mirrorMaker2Cluster.getAlias() + "-oauth-certs", mirrorMaker2Cluster.getAlias() + '-', true, oauthVolumeMountPath);

        }
        return volumeMountList;
    }

    private String buildClusterVolumeMountPath(final String baseVolumeMount,  final String path) {
        return baseVolumeMount + path + "/";
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    @Override
    protected List<EnvVar> getEnvVars() {
        List<EnvVar> varList = super.getEnvVars();

        final StringBuilder clusterAliases = new StringBuilder();
        final StringBuilder clustersTrustedCerts = new StringBuilder();
        boolean hasClusterWithTls = false;
        final StringBuilder clustersTlsAuthCerts = new StringBuilder();
        final StringBuilder clustersTlsAuthKeys = new StringBuilder();
        final StringBuilder clustersSaslPasswordFiles = new StringBuilder();
        boolean hasClusterOauthTrustedCerts = false;
        final StringBuilder clustersOauthClientSecrets = new StringBuilder();
        final StringBuilder clustersOauthAccessTokens = new StringBuilder();
        final StringBuilder clustersOauthRefreshTokens = new StringBuilder();
        final StringBuilder clustersOauthPasswords = new StringBuilder();

        for (KafkaMirrorMaker2ClusterSpec mirrorMaker2Cluster : clusters) {
            String clusterAlias = mirrorMaker2Cluster.getAlias();

            if (clusterAliases.length() > 0) {
                clusterAliases.append(";");
            }
            clusterAliases.append(clusterAlias);

            if (mirrorMaker2Cluster.getTls() != null)   {
                hasClusterWithTls = true;
            }

            getClusterTrustedCerts(clustersTrustedCerts, mirrorMaker2Cluster, clusterAlias);

            KafkaClientAuthentication authentication = mirrorMaker2Cluster.getAuthentication();
            if (authentication != null) {
                if (authentication instanceof KafkaClientAuthenticationTls) {
                    KafkaClientAuthenticationTls tlsAuth = (KafkaClientAuthenticationTls) authentication;
                    if (tlsAuth.getCertificateAndKey() != null) {
                        appendCluster(clustersTlsAuthCerts, clusterAlias, () -> tlsAuth.getCertificateAndKey().getSecretName() + "/" + tlsAuth.getCertificateAndKey().getCertificate());
                        appendCluster(clustersTlsAuthKeys, clusterAlias, () -> tlsAuth.getCertificateAndKey().getSecretName() + "/" + tlsAuth.getCertificateAndKey().getKey());
                    }
                } else if (authentication instanceof KafkaClientAuthenticationPlain) {
                    KafkaClientAuthenticationPlain passwordAuth = (KafkaClientAuthenticationPlain) authentication;
                    appendClusterPasswordSecretSource(clustersSaslPasswordFiles, clusterAlias, passwordAuth.getPasswordSecret());
                } else if (authentication instanceof KafkaClientAuthenticationScram) {
                    KafkaClientAuthenticationScram passwordAuth = (KafkaClientAuthenticationScram) authentication;
                    appendClusterPasswordSecretSource(clustersSaslPasswordFiles, clusterAlias, passwordAuth.getPasswordSecret());
                } else if (authentication instanceof KafkaClientAuthenticationOAuth) {
                    KafkaClientAuthenticationOAuth oauth = (KafkaClientAuthenticationOAuth) authentication;

                    if (oauth.getTlsTrustedCertificates() != null && !oauth.getTlsTrustedCertificates().isEmpty()) {
                        hasClusterOauthTrustedCerts = true;
                    }
                    appendClusterOAuthSecretSource(clustersOauthClientSecrets, clusterAlias, oauth.getClientSecret());
                    appendClusterOAuthSecretSource(clustersOauthAccessTokens, clusterAlias, oauth.getAccessToken());
                    appendClusterOAuthSecretSource(clustersOauthRefreshTokens, clusterAlias, oauth.getRefreshToken());
                    appendClusterPasswordSecretSource(clustersOauthPasswords, clusterAlias, oauth.getPasswordSecret());
                }
            }
        }

        varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_CLUSTERS, clusterAliases.toString()));

        if (hasClusterWithTls) {
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_TLS_CLUSTERS, "true"));
        }

        if (clustersTrustedCerts.length() > 0) {
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_TRUSTED_CERTS_CLUSTERS, clustersTrustedCerts.toString()));
        }

        if (clustersTlsAuthCerts.length() > 0 || clustersTlsAuthKeys.length() > 0) {
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_TLS_AUTH_CLUSTERS, "true"));
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_TLS_AUTH_CERTS_CLUSTERS, clustersTlsAuthCerts.toString()));
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_TLS_AUTH_KEYS_CLUSTERS, clustersTlsAuthKeys.toString()));
        }

        if (clustersSaslPasswordFiles.length() > 0) {
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_SASL_PASSWORD_FILES_CLUSTERS, clustersSaslPasswordFiles.toString()));
        }

        if (hasClusterOauthTrustedCerts) {
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_OAUTH_TRUSTED_CERTS, "true"));
        }

        if (clustersOauthClientSecrets.length() > 0) {
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_OAUTH_CLIENT_SECRETS_CLUSTERS, clustersOauthClientSecrets.toString()));
        }

        if (clustersOauthAccessTokens.length() > 0) {
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_OAUTH_ACCESS_TOKENS_CLUSTERS, clustersOauthAccessTokens.toString()));
        }

        if (clustersOauthRefreshTokens.length() > 0) {
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_OAUTH_REFRESH_TOKENS_CLUSTERS, clustersOauthRefreshTokens.toString()));
        }

        if (clustersOauthPasswords.length() > 0) {
            varList.add(ContainerUtils.createEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_OAUTH_PASSWORDS_CLUSTERS, clustersOauthPasswords.toString()));
        }

        ModelUtils.jvmSystemProperties(varList, jvmOptions);

        return varList;
    }

    private void getClusterTrustedCerts(final StringBuilder clustersTrustedCerts, KafkaMirrorMaker2ClusterSpec mirrorMaker2Cluster, String clusterAlias) {
        ClientTls tls = mirrorMaker2Cluster.getTls();
        if (tls != null) {
            List<CertSecretSource> trustedCertificates = tls.getTrustedCertificates();
   
            if (trustedCertificates != null && trustedCertificates.size() > 0) {
                if (clustersTrustedCerts.length() > 0) {
                    clustersTrustedCerts.append("\n");
                }
                clustersTrustedCerts.append(clusterAlias);
                clustersTrustedCerts.append("=");
   
                boolean separator = false;
                for (CertSecretSource certSecretSource : trustedCertificates) {
                    if (separator) {
                        clustersTrustedCerts.append(";");
                    }
                    clustersTrustedCerts.append(certSecretSource.getSecretName());
                    clustersTrustedCerts.append("/");
                    clustersTrustedCerts.append(certSecretSource.getCertificate());
                    separator = true;
                }
            }
        }
    }

    private void appendClusterPasswordSecretSource(final StringBuilder clusters, String clusterAlias, PasswordSecretSource passwordSecretSource) {
        if (passwordSecretSource != null) {
            appendCluster(clusters, clusterAlias, () -> passwordSecretSource.getSecretName() + "/" + passwordSecretSource.getPassword());
        }
    }

    private void appendClusterOAuthSecretSource(final StringBuilder clusters, String clusterAlias, GenericSecretSource secretSource) {
        if (secretSource != null) {
            appendCluster(clusters, clusterAlias, () -> secretSource.getSecretName() + "/" + secretSource.getKey());
        }
    }

    private void appendCluster(final StringBuilder clusters, String clusterAlias, Supplier<String> function) {
        if (clusters.length() > 0) {
            clusters.append("\n");
        }                   
        clusters.append(clusterAlias);
        clusters.append("=");
        clusters.append(function.get());
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
     * The command for running Connect has to be passed through a method so that we can handle different run commands
     * for Connect and Mirror Maker 2 (which inherits from this class) without duplicating the whole container creation.
     *
     * @return  Command for starting Kafka Mirror Maker 2 container
     */
    @Override
    protected String getCommand() {
        return "/opt/kafka/kafka_mirror_maker_2_run.sh";
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
}
