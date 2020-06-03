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
import io.strimzi.api.kafka.model.KafkaConnectTls;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Spec;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Tls;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationOAuth;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationPlain;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationScramSha512;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationTls;

import java.util.List;
import java.util.Map.Entry;
import java.util.function.Supplier;

public class KafkaMirrorMaker2Cluster extends KafkaConnectCluster {
    protected static final String APPLICATION_NAME = "kafka-mirror-maker-2";

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

    protected static final String MIRRORMAKER_2_OAUTH_SECRETS_BASE_VOLUME_MOUNT = "/opt/kafka/mm2-oauth/";
    protected static final String MIRRORMAKER_2_OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT = "/opt/kafka/mm2-oauth-certs/";
    protected static final String MIRRORMAKER_2_TLS_CERTS_BASE_VOLUME_MOUNT = "/opt/kafka/mm2-certs/";
    protected static final String MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT = "/opt/kafka/mm2-password/";

    private List<KafkaMirrorMaker2ClusterSpec> clusters;

    /**
     * Constructor
     *
     * @param resource Kubernetes resource with metadata containing the namespace and cluster name
     */
    private KafkaMirrorMaker2Cluster(HasMetadata resource) {
        super(resource, APPLICATION_NAME);
        this.name = KafkaMirrorMaker2Resources.deploymentName(cluster);
        this.serviceName = KafkaMirrorMaker2Resources.serviceName(cluster);
        this.ancillaryConfigMapName = KafkaMirrorMaker2Resources.metricsAndLogConfigMapName(cluster);
    }

    /**
     * Creates instance of KafkaMirrorMaker2Cluster from CRD definition.
     *
     * @param kafkaMirrorMaker2 The Custom Resource based on which the cluster model should be created.
     * @param versions The image versions for MirrorMaker 2.0 clusters.
     * @return The MirrorMaker 2.0 cluster model.
     */
    public static KafkaMirrorMaker2Cluster fromCrd(KafkaMirrorMaker2 kafkaMirrorMaker2, 
                                                   KafkaVersion.Lookup versions) {
        KafkaMirrorMaker2Cluster cluster = new KafkaMirrorMaker2Cluster(kafkaMirrorMaker2);
        KafkaMirrorMaker2Spec spec = kafkaMirrorMaker2.getSpec();
        cluster.setOwnerReference(kafkaMirrorMaker2);

        String specVersion = spec.getVersion();
        if (versions.version(specVersion).compareVersion("2.4.0") < 0) {
            throw new InvalidResourceException("Kafka MirrorMaker 2.0 is not available in the version at spec.version (" + specVersion + "). Kafka MirrorMaker 2.0 is available in Kafka version 2.4.0 and later.");
        } 
        cluster.setImage(versions.kafkaMirrorMaker2Version(spec.getImage(), specVersion));

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
        cluster.setConfiguration(new KafkaMirrorMaker2Configuration(connectCluster.getConfig().entrySet()));
        return fromSpec(buildKafkaConnectSpec(spec, connectCluster), versions, cluster);
    }

    @SuppressWarnings("deprecation")
    private static KafkaConnectSpec buildKafkaConnectSpec(KafkaMirrorMaker2Spec spec, KafkaMirrorMaker2ClusterSpec connectCluster) {
        
        KafkaConnectTls connectTls = null;
        KafkaMirrorMaker2Tls mirrorMaker2ConnectClusterTls = connectCluster.getTls();
        if (mirrorMaker2ConnectClusterTls != null) {
            connectTls = new KafkaConnectTls();
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
                .withMetrics(spec.getMetrics())
                .withTracing(spec.getTracing())
                .withAffinity(spec.getAffinity())
                .withTolerations(spec.getTolerations())
                .withTemplate(spec.getTemplate())
                .withExternalConfiguration(spec.getExternalConfiguration())
                .build();

    }

    @Override
    protected String getDefaultLogConfigFileName() {
        return "kafkaMirrorMaker2DefaultLoggingProperties";
    }

    @Override
    protected String getServiceAccountName() {
        return KafkaMirrorMaker2Resources.serviceAccountName(cluster);
    }

    @Override
    protected List<Volume> getVolumes(boolean isOpenShift) {
        List<Volume> volumeList = super.getVolumes(isOpenShift);

        for (KafkaMirrorMaker2ClusterSpec mirrorMaker2Cluster: clusters) {
            KafkaMirrorMaker2Tls tls = mirrorMaker2Cluster.getTls();

            if (tls != null) {
                List<CertSecretSource> trustedCertificates = tls.getTrustedCertificates();
    
                if (trustedCertificates != null && trustedCertificates.size() > 0) {
                    for (CertSecretSource certSecretSource : trustedCertificates) {
                        String volumeName = mirrorMaker2Cluster.getAlias() + '-' + certSecretSource.getSecretName();
                        // skipping if a volume with same Secret name was already added
                        if (!volumeList.stream().anyMatch(v -> v.getName().equals(volumeName))) {
                            volumeList.add(VolumeUtils.createSecretVolume(volumeName, certSecretSource.getSecretName(), isOpenShift));
                        }
                    }
                }
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

            KafkaMirrorMaker2Tls tls = mirrorMaker2Cluster.getTls();
            if (tls != null) {
                List<CertSecretSource> trustedCertificates = tls.getTrustedCertificates();
    
                if (trustedCertificates != null && trustedCertificates.size() > 0) {
                    for (CertSecretSource certSecretSource : trustedCertificates) {
                        String volumeMountName = alias + '-' + certSecretSource.getSecretName();
                        // skipping if a volume mount with same Secret name was already added
                        if (!volumeMountList.stream().anyMatch(vm -> vm.getName().equals(volumeMountName))) {
                            volumeMountList.add(VolumeUtils.createVolumeMount(volumeMountName,
                                tlsVolumeMountPath + certSecretSource.getSecretName()));
                        }
                    }
                }
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
        final StringBuilder clustersTlsAuthCerts = new StringBuilder();
        final StringBuilder clustersTlsAuthKeys = new StringBuilder();
        final StringBuilder clustersSaslPasswordFiles = new StringBuilder();
        boolean hasClusterOauthTrustedCerts = false;
        final StringBuilder clustersOauthClientSecrets = new StringBuilder();
        final StringBuilder clustersOauthAccessTokens = new StringBuilder();
        final StringBuilder clustersOauthRefreshTokens = new StringBuilder();

        for (KafkaMirrorMaker2ClusterSpec mirrorMaker2Cluster : clusters) {
            String clusterAlias = mirrorMaker2Cluster.getAlias();

            if (clusterAliases.length() > 0) {
                clusterAliases.append(";");
            }
            clusterAliases.append(clusterAlias);

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
                } else if (authentication instanceof KafkaClientAuthenticationScramSha512) {
                    KafkaClientAuthenticationScramSha512 passwordAuth = (KafkaClientAuthenticationScramSha512) authentication;
                    appendClusterPasswordSecretSource(clustersSaslPasswordFiles, clusterAlias, passwordAuth.getPasswordSecret());
                } else if (authentication instanceof KafkaClientAuthenticationOAuth) {
                    KafkaClientAuthenticationOAuth oauth = (KafkaClientAuthenticationOAuth) authentication;

                    if (oauth.getTlsTrustedCertificates() != null && !oauth.getTlsTrustedCertificates().isEmpty()) {
                        hasClusterOauthTrustedCerts = true;
                    }
                    appendClusterOAuthSecretSource(clustersOauthClientSecrets, clusterAlias, oauth.getClientSecret());
                    appendClusterOAuthSecretSource(clustersOauthAccessTokens, clusterAlias, oauth.getAccessToken());
                    appendClusterOAuthSecretSource(clustersOauthRefreshTokens, clusterAlias, oauth.getRefreshToken());
                }
            }
        }

        varList.add(buildEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_CLUSTERS, clusterAliases.toString()));

        if (clustersTrustedCerts.length() > 0) {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_TLS_CLUSTERS, "true"));
            varList.add(buildEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_TRUSTED_CERTS_CLUSTERS, clustersTrustedCerts.toString()));
        }

        if (clustersTlsAuthCerts.length() > 0 || clustersTlsAuthKeys.length() > 0) {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_TLS_AUTH_CLUSTERS, "true"));
            varList.add(buildEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_TLS_AUTH_CERTS_CLUSTERS, clustersTlsAuthCerts.toString()));
            varList.add(buildEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_TLS_AUTH_KEYS_CLUSTERS, clustersTlsAuthKeys.toString()));
        }

        if (clustersSaslPasswordFiles.length() > 0) {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_SASL_PASSWORD_FILES_CLUSTERS, clustersSaslPasswordFiles.toString()));
        }

        if (hasClusterOauthTrustedCerts) {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_OAUTH_TRUSTED_CERTS, "true"));
        }

        if (clustersOauthClientSecrets.length() > 0) {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_OAUTH_CLIENT_SECRETS_CLUSTERS, clustersOauthClientSecrets.toString()));
        }

        if (clustersOauthAccessTokens.length() > 0) {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_OAUTH_ACCESS_TOKENS_CLUSTERS, clustersOauthAccessTokens.toString()));
        }

        if (clustersOauthRefreshTokens.length() > 0) {
            varList.add(buildEnvVar(ENV_VAR_KAFKA_MIRRORMAKER_2_OAUTH_REFRESH_TOKENS_CLUSTERS, clustersOauthRefreshTokens.toString()));
        }

        if (javaSystemProperties != null) {
            varList.add(buildEnvVar(ENV_VAR_STRIMZI_JAVA_SYSTEM_PROPERTIES, ModelUtils.getJavaSystemPropertiesToString(javaSystemProperties)));
        }

        return varList;
    }

    private void getClusterTrustedCerts(final StringBuilder clustersTrustedCerts, KafkaMirrorMaker2ClusterSpec mirrorMaker2Cluster, String clusterAlias) {
        KafkaMirrorMaker2Tls tls = mirrorMaker2Cluster.getTls();
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

    @Override
    protected String getCommand() {
        return "/opt/kafka/kafka_mirror_maker_2_run.sh";
    }
}
