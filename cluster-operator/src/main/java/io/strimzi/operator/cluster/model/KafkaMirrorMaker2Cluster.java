/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.GenericSecretSource;
import io.strimzi.api.kafka.model.KafkaConnectTls;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Spec;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Tls;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationOAuth;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationPlain;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationScramSha512;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationTls;
import io.strimzi.operator.common.model.Labels;

public class KafkaMirrorMaker2Cluster extends KafkaConnectCluster {

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

    protected static final String OAUTH_SECRETS_BASE_VOLUME_MOUNT = "/opt/kafka/oauth/";

    private List<KafkaMirrorMaker2ClusterSpec> clusters;
    private KafkaMirrorMaker2ClusterSpec connectCluster;

    /**
     * Constructor
     *
     * @param namespace Kubernetes/OpenShift namespace where Kafka Connect cluster
     *                  resources are going to be created
     * @param cluster   overall cluster name
     */
    private KafkaMirrorMaker2Cluster(String namespace, String cluster, Labels labels) {
        super(namespace, cluster, labels);
        this.name = KafkaMirrorMaker2Resources.deploymentName(cluster);
        this.serviceName = KafkaMirrorMaker2Resources.serviceName(cluster);
        this.ancillaryConfigName = KafkaMirrorMaker2Resources.metricsAndLogConfigMapName(cluster);
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
        KafkaMirrorMaker2Cluster cluster = new KafkaMirrorMaker2Cluster(kafkaMirrorMaker2.getMetadata().getNamespace(),
                kafkaMirrorMaker2.getMetadata().getName(),
                Labels.fromResource(kafkaMirrorMaker2).withKind(kafkaMirrorMaker2.getKind()));
        KafkaMirrorMaker2Spec spec = kafkaMirrorMaker2.getSpec();
        cluster.setOwnerReference(kafkaMirrorMaker2);
        cluster.setImage(versions.kafkaMirrorMaker2Version(spec.getImage(), spec.getVersion()));
        cluster.setConfiguration(new KafkaMirrorMaker2Configuration(spec.getConfig().entrySet())); 

        List<KafkaMirrorMaker2ClusterSpec> clustersList = Optional.ofNullable(spec.getClusters())
                .orElse(Collections.emptyList());
        cluster.setClusters(clustersList);

        String connectClusterAlias = spec.getConnectCluster();
        if (connectClusterAlias != null) {
            KafkaMirrorMaker2ClusterSpec connectCluster = clustersList.stream()
                    .filter(clustersListItem -> connectClusterAlias.equals(clustersListItem.getAlias()))
                    .findFirst()
                    .orElseThrow(() -> new InvalidResourceException("connectCluster with alias " + connectClusterAlias + " cannot be found in the list of clusters at spec.clusters"));     
            cluster.setConnectCluster(connectCluster);

            spec.setBootstrapServers(connectCluster.getBootstrapServers());
            spec.setAuthentication(connectCluster.getAuthentication());

            KafkaMirrorMaker2Tls mirrorMaker2ConnectClusterTls = connectCluster.getTls();
            if (mirrorMaker2ConnectClusterTls != null) {
                KafkaConnectTls connectTls = new KafkaConnectTls();
                connectTls.setTrustedCertificates(mirrorMaker2ConnectClusterTls.getTrustedCertificates());
                mirrorMaker2ConnectClusterTls.getAdditionalProperties().entrySet().stream()
                        .forEach(entry -> connectTls.setAdditionalProperty(entry.getKey(), entry.getValue()));
                spec.setTls(connectTls);
            }
        }

        return fromSpec(spec, versions, cluster);
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

        clusters.stream()
            .forEach(mirrorMaker2Cluster -> {
                KafkaMirrorMaker2Tls tls = mirrorMaker2Cluster.getTls();

                if (tls != null) {
                    List<CertSecretSource> trustedCertificates = tls.getTrustedCertificates();
        
                    if (trustedCertificates != null && trustedCertificates.size() > 0) {
                        for (CertSecretSource certSecretSource : trustedCertificates) {
                            // skipping if a volume with same Secret name was already added
                            if (!volumeList.stream().anyMatch(v -> v.getName().equals(certSecretSource.getSecretName()))) {
                                volumeList.add(createSecretVolume(certSecretSource.getSecretName(), certSecretSource.getSecretName(), isOpenShift));
                            }
                        }
                    }
                }
        
                AuthenticationUtils.configureClientAuthenticationVolumes(mirrorMaker2Cluster.getAuthentication(), volumeList, mirrorMaker2Cluster.getAlias() + "-oauth-certs", isOpenShift, true);
            });
        return volumeList;
    }

    @Override
    protected List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> volumeMountList = super.getVolumeMounts();

        clusters.stream()
            .forEach(mirrorMaker2Cluster -> {
                KafkaMirrorMaker2Tls tls = mirrorMaker2Cluster.getTls();

                if (tls != null) {
                    List<CertSecretSource> trustedCertificates = tls.getTrustedCertificates();
        
                    if (trustedCertificates != null && trustedCertificates.size() > 0) {
                        for (CertSecretSource certSecretSource : trustedCertificates) {
                            // skipping if a volume mount with same Secret name was already added
                            if (!volumeMountList.stream().anyMatch(vm -> vm.getName().equals(certSecretSource.getSecretName()))) {
                                volumeMountList.add(createVolumeMount(certSecretSource.getSecretName(),
                                        TLS_CERTS_BASE_VOLUME_MOUNT + certSecretSource.getSecretName()));
                            }
                        }
                    }
                }
        
                AuthenticationUtils.configureClientAuthenticationVolumeMounts(mirrorMaker2Cluster.getAuthentication(), volumeMountList, TLS_CERTS_BASE_VOLUME_MOUNT, PASSWORD_VOLUME_MOUNT, OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT + mirrorMaker2Cluster.getAlias() + "/", mirrorMaker2Cluster.getAlias() + "-oauth-certs", true, OAUTH_SECRETS_BASE_VOLUME_MOUNT);
            });
        return volumeMountList;
    }

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
                    clustersTrustedCerts.append(certSecretSource.getSecretName() + "/" + certSecretSource.getCertificate());
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
     * Sets the configured cluster for Kafka Connect
     *
     * @param connectCluster The cluster configuration used for Kafka Connect
     */
    protected void setConnectCluster(KafkaMirrorMaker2ClusterSpec connectCluster) {
        this.connectCluster = connectCluster;
    }

    @Override
    protected String getCommand() {
        return "/opt/kafka/kafka_mirror_maker_2_run.sh";
    }
}
