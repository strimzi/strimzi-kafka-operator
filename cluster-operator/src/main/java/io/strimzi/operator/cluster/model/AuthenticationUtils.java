/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.KafkaJmxAuthentication;
import io.strimzi.api.kafka.model.KafkaJmxAuthenticationPassword;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationOAuth;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationPlain;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationScramSha512;
import io.strimzi.api.kafka.model.authentication.KafkaClientAuthenticationTls;
import io.strimzi.kafka.oauth.client.ClientConfig;
import io.strimzi.kafka.oauth.server.ServerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class AuthenticationUtils {
    protected static final Logger log = LogManager.getLogger(AuthenticationUtils.class.getName());

    /**
     * Validates Kafka client authentication for all components based on Apache Kafka clients.
     *
     * @param authentication    The authentication object from CRD
     * @param tls   Indicates whether TLS is enabled or not
     */
    @SuppressWarnings("BooleanExpressionComplexity")
    public static void validateClientAuthentication(KafkaClientAuthentication authentication, boolean tls)    {
        if (authentication != null)   {
            if (authentication instanceof KafkaClientAuthenticationTls) {
                KafkaClientAuthenticationTls auth = (KafkaClientAuthenticationTls) authentication;
                if (auth.getCertificateAndKey() != null) {
                    if (!tls) {
                        log.warn("TLS configuration missing: related TLS client authentication will not work properly");
                    }
                } else {
                    log.warn("TLS Client authentication selected, but no certificate and key configured.");
                    throw new InvalidResourceException("TLS Client authentication selected, but no certificate and key configured.");
                }
            } else if (authentication instanceof KafkaClientAuthenticationScramSha512)    {
                KafkaClientAuthenticationScramSha512 auth = (KafkaClientAuthenticationScramSha512) authentication;
                if (auth.getUsername() == null || auth.getPasswordSecret() == null) {
                    log.warn("SCRAM-SHA-512 authentication selected, but username or password configuration is missing.");
                    throw new InvalidResourceException("SCRAM-SHA-512 authentication selected, but username or password configuration is missing.");
                }
            } else if (authentication instanceof KafkaClientAuthenticationPlain) {
                KafkaClientAuthenticationPlain auth = (KafkaClientAuthenticationPlain) authentication;
                if (auth.getUsername() == null || auth.getPasswordSecret() == null) {
                    log.warn("PLAIN authentication selected, but username or password configuration is missing.");
                    throw new InvalidResourceException("PLAIN authentication selected, but username or password configuration is missing.");
                }
            } else if (authentication instanceof KafkaClientAuthenticationOAuth) {
                KafkaClientAuthenticationOAuth auth = (KafkaClientAuthenticationOAuth) authentication;

                if ((auth.getAccessToken() != null)
                        || (auth.getTokenEndpointUri() != null && auth.getClientId() != null && auth.getRefreshToken() != null)
                        || (auth.getTokenEndpointUri() != null && auth.getClientId() != null && auth.getClientSecret() != null))    {
                    // Valid options, lets just pass it through.
                    // This way the condition is easier to read and understand.
                } else {
                    log.warn("OAUTH authentication selected, but some options are missing. You have to specify one of the following commbinations: [accessToken], [tokenEndpointUri, clientId, refreshToken], [tokenEndpointUri, clientId, clientsecret].");
                    throw new InvalidResourceException("OAUTH authentication selected, but some options are missing. You have to specify one of the following commbinations: [accessToken], [tokenEndpointUri, clientId, refreshToken], [tokenEndpointUri, clientId, clientsecret].");
                }
            }
        }
    }

    /**
     * Creates the Volumes used for authentication of Kafka client based components
     *
     * @param authentication    Authentication object from CRD
     * @param volumeList    List where the volumes will be added
     * @param oauthVolumeNamePrefix Prefix used for OAuth volumes
     * @param isOpenShift   Indicates whether we run on OpenShift or not
     */
    public static void configureClientAuthenticationVolumes(KafkaClientAuthentication authentication, List<Volume> volumeList, String oauthVolumeNamePrefix, boolean isOpenShift)   {
        if (authentication != null) {
            if (authentication instanceof KafkaClientAuthenticationTls) {
                KafkaClientAuthenticationTls tlsAuth = (KafkaClientAuthenticationTls) authentication;

                // skipping if a volume with same Secret name was already added
                if (!volumeList.stream().anyMatch(v -> v.getName().equals(tlsAuth.getCertificateAndKey().getSecretName()))) {
                    volumeList.add(AbstractModel.createSecretVolume(tlsAuth.getCertificateAndKey().getSecretName(), tlsAuth.getCertificateAndKey().getSecretName(), isOpenShift));
                }
            } else if (authentication instanceof KafkaClientAuthenticationPlain) {
                KafkaClientAuthenticationPlain passwordAuth = (KafkaClientAuthenticationPlain) authentication;
                volumeList.add(AbstractModel.createSecretVolume(passwordAuth.getPasswordSecret().getSecretName(), passwordAuth.getPasswordSecret().getSecretName(), isOpenShift));
            } else if (authentication instanceof KafkaClientAuthenticationScramSha512) {
                KafkaClientAuthenticationScramSha512 passwordAuth = (KafkaClientAuthenticationScramSha512) authentication;
                volumeList.add(AbstractModel.createSecretVolume(passwordAuth.getPasswordSecret().getSecretName(), passwordAuth.getPasswordSecret().getSecretName(), isOpenShift));
            } else if (authentication instanceof KafkaClientAuthenticationOAuth) {
                KafkaClientAuthenticationOAuth oauth = (KafkaClientAuthenticationOAuth) authentication;
                volumeList.addAll(configureOauthCertificateVolumes(oauthVolumeNamePrefix, oauth.getTlsTrustedCertificates(), isOpenShift));
            }
        }
    }

    /**
     * Creates the VolumeMounts used for authentication of Kafka client based components
     *
     * @param authentication    Authentication object from CRD
     * @param volumeMountList    List where the volumes will be added
     * @param tlsVolumeMount    Path where the TLS certs should be mounted
     * @param passwordVolumeMount   Path where passwords should be mounted
     * @param oauthVolumeMount      Path where the OAuth certificates would be mounted
     * @param oauthVolumeNamePrefix Prefix used for OAuth volume names
     */
    public static void configureClientAuthenticationVolumeMounts(KafkaClientAuthentication authentication, List<VolumeMount> volumeMountList, String tlsVolumeMount, String passwordVolumeMount, String oauthVolumeMount, String oauthVolumeNamePrefix)   {
        if (authentication != null) {
            if (authentication instanceof KafkaClientAuthenticationTls) {
                KafkaClientAuthenticationTls tlsAuth = (KafkaClientAuthenticationTls) authentication;

                // skipping if a volume mount with same Secret name was already added
                if (!volumeMountList.stream().anyMatch(vm -> vm.getName().equals(tlsAuth.getCertificateAndKey().getSecretName()))) {
                    volumeMountList.add(AbstractModel.createVolumeMount(tlsAuth.getCertificateAndKey().getSecretName(),
                            tlsVolumeMount + tlsAuth.getCertificateAndKey().getSecretName()));
                }
            } else if (authentication instanceof KafkaClientAuthenticationPlain) {
                KafkaClientAuthenticationPlain passwordAuth = (KafkaClientAuthenticationPlain) authentication;
                volumeMountList.add(AbstractModel.createVolumeMount(passwordAuth.getPasswordSecret().getSecretName(), passwordVolumeMount + passwordAuth.getPasswordSecret().getSecretName()));
            } else if (authentication instanceof KafkaClientAuthenticationScramSha512) {
                KafkaClientAuthenticationScramSha512 passwordAuth = (KafkaClientAuthenticationScramSha512) authentication;
                volumeMountList.add(AbstractModel.createVolumeMount(passwordAuth.getPasswordSecret().getSecretName(), passwordVolumeMount + passwordAuth.getPasswordSecret().getSecretName()));
            } else if (authentication instanceof KafkaClientAuthenticationOAuth) {
                KafkaClientAuthenticationOAuth oauth = (KafkaClientAuthenticationOAuth) authentication;
                volumeMountList.addAll(configureOauthCertificateVolumeMounts(oauthVolumeNamePrefix, oauth.getTlsTrustedCertificates(), oauthVolumeMount));
            }
        }
    }

    /**
     * Configured environment variables related to authentication in Kafka clients
     *
     * @param authentication    Authentication object with auth configuration
     * @param varList   List where the new environment variables should be added
     * @param envVarNamer   Function for naming the environment variables (ech components is using different names)
     */
    public static void configureClientAuthenticationEnvVars(KafkaClientAuthentication authentication, List<EnvVar> varList, Function<String, String> envVarNamer)   {
        if (authentication != null) {
            if (authentication instanceof KafkaClientAuthenticationTls) {
                KafkaClientAuthenticationTls tlsAuth = (KafkaClientAuthenticationTls) authentication;
                varList.add(AbstractModel.buildEnvVar(envVarNamer.apply("TLS_AUTH_CERT"), String.format("%s/%s", tlsAuth.getCertificateAndKey().getSecretName(), tlsAuth.getCertificateAndKey().getCertificate())));
                varList.add(AbstractModel.buildEnvVar(envVarNamer.apply("TLS_AUTH_KEY"), String.format("%s/%s", tlsAuth.getCertificateAndKey().getSecretName(), tlsAuth.getCertificateAndKey().getKey())));
            } else if (authentication instanceof KafkaClientAuthenticationPlain) {
                KafkaClientAuthenticationPlain passwordAuth = (KafkaClientAuthenticationPlain) authentication;
                varList.add(AbstractModel.buildEnvVar(envVarNamer.apply("SASL_USERNAME"), passwordAuth.getUsername()));
                varList.add(AbstractModel.buildEnvVar(envVarNamer.apply("SASL_PASSWORD_FILE"), String.format("%s/%s", passwordAuth.getPasswordSecret().getSecretName(), passwordAuth.getPasswordSecret().getPassword())));
                varList.add(AbstractModel.buildEnvVar(envVarNamer.apply("SASL_MECHANISM"), "plain"));
            } else if (authentication instanceof KafkaClientAuthenticationScramSha512) {
                KafkaClientAuthenticationScramSha512 passwordAuth = (KafkaClientAuthenticationScramSha512) authentication;
                varList.add(AbstractModel.buildEnvVar(envVarNamer.apply("SASL_USERNAME"), passwordAuth.getUsername()));
                varList.add(AbstractModel.buildEnvVar(envVarNamer.apply("SASL_PASSWORD_FILE"), String.format("%s/%s", passwordAuth.getPasswordSecret().getSecretName(), passwordAuth.getPasswordSecret().getPassword())));
                varList.add(AbstractModel.buildEnvVar(envVarNamer.apply("SASL_MECHANISM"), "scram-sha-512"));
            } else if (authentication instanceof KafkaClientAuthenticationOAuth) {
                KafkaClientAuthenticationOAuth oauth = (KafkaClientAuthenticationOAuth) authentication;

                varList.add(AbstractModel.buildEnvVar(envVarNamer.apply("SASL_MECHANISM"), "oauth"));

                List<String> options = new ArrayList<>(2);
                if (oauth.getClientId() != null) options.add(String.format("%s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, oauth.getClientId()));
                if (oauth.getTokenEndpointUri() != null) options.add(String.format("%s=\"%s\"", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, oauth.getTokenEndpointUri()));
                if (oauth.isDisableTlsHostnameVerification()) options.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, ""));
                if (!oauth.isAccessTokenIsJwt()) options.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_TOKENS_NOT_JWT, true));
                if (oauth.getMaxTokenExpirySeconds() > 0) options.add(String.format("%s=\"%s\"", ClientConfig.OAUTH_MAX_TOKEN_EXPIRY_SECONDS, oauth.getMaxTokenExpirySeconds()));

                varList.add(AbstractModel.buildEnvVar(envVarNamer.apply("OAUTH_CONFIG"), String.join(" ", options)));

                if (oauth.getClientSecret() != null)    {
                    varList.add(AbstractModel.buildEnvVarFromSecret(envVarNamer.apply("OAUTH_CLIENT_SECRET"), oauth.getClientSecret().getSecretName(), oauth.getClientSecret().getKey()));
                }

                if (oauth.getAccessToken() != null)    {
                    varList.add(AbstractModel.buildEnvVarFromSecret(envVarNamer.apply("OAUTH_ACCESS_TOKEN"), oauth.getAccessToken().getSecretName(), oauth.getAccessToken().getKey()));
                }

                if (oauth.getRefreshToken() != null)    {
                    varList.add(AbstractModel.buildEnvVarFromSecret(envVarNamer.apply("OAUTH_REFRESH_TOKEN"), oauth.getRefreshToken().getSecretName(), oauth.getRefreshToken().getKey()));
                }
            }
        }
    }

    /**
     * Generates volumes needed for certificates needed to connect to OAuth server.
     * This is used in both OAuth servers and clients.
     *
     * @param volumeNamePrefix    Prefix for naming the secret volumes
     * @param trustedCertificates   List of certificates which should be mounted
     * @param isOpenShift   Flag whether we are on OpenShift or not
     *
     * @return List of new Volumes
     */
    public static List<Volume> configureOauthCertificateVolumes(String volumeNamePrefix, List<CertSecretSource> trustedCertificates, boolean isOpenShift)   {
        List<Volume> newVolumes = new ArrayList<>();

        if (trustedCertificates != null && trustedCertificates.size() > 0) {
            int i = 0;

            for (CertSecretSource certSecretSource : trustedCertificates) {
                Map<String, String> items = Collections.singletonMap(certSecretSource.getCertificate(), "tls.crt");
                String volumeName = String.format("%s-%d", volumeNamePrefix, i);

                Volume vol = AbstractModel.createSecretVolume(volumeName, certSecretSource.getSecretName(), items, isOpenShift);

                newVolumes.add(vol);
                i++;
            }
        }

        return newVolumes;
    }

    /**
     * Generates volume mounts needed for certificates needed to connect to OAuth server.
     * This is used in both OAuth servers and clients.
     *
     * @param volumeNamePrefix   Prefix which was used to name the secret volumes
     * @param trustedCertificates   List of certificates which should be mounted
     * @param baseVolumeMount   The Base volume into which the certificates should be mounted
     *
     * @return List of new VolumeMounts
     */
    public static List<VolumeMount> configureOauthCertificateVolumeMounts(String volumeNamePrefix, List<CertSecretSource> trustedCertificates, String baseVolumeMount)   {
        List<VolumeMount> newVolumeMounts = new ArrayList<>();

        if (trustedCertificates != null && trustedCertificates.size() > 0) {
            int i = 0;

            for (CertSecretSource certSecretSource : trustedCertificates) {
                String volumeName = String.format("%s-%d", volumeNamePrefix, i);
                newVolumeMounts.add(AbstractModel.createVolumeMount(volumeName, String.format("%s/%s-%d", baseVolumeMount, certSecretSource.getSecretName(), i)));
                i++;
            }
        }

        return newVolumeMounts;
    }

    /**
     * Generates the necessary resources that the Kafka Cluster needs to secure the Kafka Jmx Port
     *
     * @param authentication the Authentication Configuration for the Kafka Jmx Port
     * @param kafkaCluster the current state of the kafka Cluster CR
     */
    public static void configureKafkaJmxOptions(KafkaJmxAuthentication authentication, KafkaCluster kafkaCluster)   {
        if (authentication != null) {
            if (authentication instanceof KafkaJmxAuthenticationPassword) {
                kafkaCluster.setJmxAuthenticated(true);
            }
        } else {
            kafkaCluster.setJmxAuthenticated(false);
        }
    }

}
