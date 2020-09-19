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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

public class AuthenticationUtils {
    protected static final Logger log = LogManager.getLogger(AuthenticationUtils.class.getName());

    public static final String TLS_AUTH_CERT = "TLS_AUTH_CERT";
    public static final String TLS_AUTH_KEY = "TLS_AUTH_KEY";
    public static final String SASL_USERNAME = "SASL_USERNAME";
    public static final String SASL_PASSWORD_FILE = "SASL_PASSWORD_FILE";
    public static final String SASL_MECHANISM = "SASL_MECHANISM";
    public static final String OAUTH_CONFIG = "OAUTH_CONFIG";

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
        configureClientAuthenticationVolumes(authentication, volumeList, oauthVolumeNamePrefix, isOpenShift, "", false);
    }

    /**
     * Creates the Volumes used for authentication of Kafka client based components
     *
     * @param authentication    Authentication object from CRD
     * @param volumeList    List where the volumes will be added
     * @param oauthVolumeNamePrefix Prefix used for OAuth volumes
     * @param isOpenShift   Indicates whether we run on OpenShift or not
     * @param volumeNamePrefix Prefix used for volume names
     * @param createOAuthSecretVolumes   Indicates whether OAuth secret volumes will be added to the list
     */
    public static void configureClientAuthenticationVolumes(KafkaClientAuthentication authentication, List<Volume> volumeList, String oauthVolumeNamePrefix, boolean isOpenShift, String volumeNamePrefix, boolean createOAuthSecretVolumes)   {
        if (authentication != null) {
            if (authentication instanceof KafkaClientAuthenticationTls) {
                KafkaClientAuthenticationTls tlsAuth = (KafkaClientAuthenticationTls) authentication;
                addNewVolume(volumeList, volumeNamePrefix, tlsAuth.getCertificateAndKey().getSecretName(), isOpenShift);
            } else if (authentication instanceof KafkaClientAuthenticationPlain) {
                KafkaClientAuthenticationPlain passwordAuth = (KafkaClientAuthenticationPlain) authentication;
                addNewVolume(volumeList, volumeNamePrefix, passwordAuth.getPasswordSecret().getSecretName(), isOpenShift);
            } else if (authentication instanceof KafkaClientAuthenticationScramSha512) {
                KafkaClientAuthenticationScramSha512 passwordAuth = (KafkaClientAuthenticationScramSha512) authentication;
                addNewVolume(volumeList, volumeNamePrefix, passwordAuth.getPasswordSecret().getSecretName(), isOpenShift);
            } else if (authentication instanceof KafkaClientAuthenticationOAuth) {
                KafkaClientAuthenticationOAuth oauth = (KafkaClientAuthenticationOAuth) authentication;
                volumeList.addAll(configureOauthCertificateVolumes(oauthVolumeNamePrefix, oauth.getTlsTrustedCertificates(), isOpenShift));

                if (createOAuthSecretVolumes) {
                    if (oauth.getClientSecret() != null) {
                        addNewVolume(volumeList, volumeNamePrefix, oauth.getClientSecret().getSecretName(), isOpenShift);
                    }
                    if (oauth.getAccessToken() != null) {
                        addNewVolume(volumeList, volumeNamePrefix, oauth.getAccessToken().getSecretName(), isOpenShift);
                    }
                    if (oauth.getRefreshToken() != null) {
                        addNewVolume(volumeList, volumeNamePrefix, oauth.getRefreshToken().getSecretName(), isOpenShift);
                    }
                }
            }
        }
    }

    /**
     * Creates the Volumes used for authentication of Kafka client based components, checking that the named volume has not already been
     * created.
     */
    private static void addNewVolume(List<Volume> volumeList, String volumeNamePrefix, String secretName, boolean isOpenShift) {
        // skipping if a volume with same name was already added
        if (!volumeList.stream().anyMatch(v -> v.getName().equals(volumeNamePrefix + secretName))) {
            volumeList.add(VolumeUtils.createSecretVolume(volumeNamePrefix + secretName, secretName, isOpenShift));
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
    public static void configureClientAuthenticationVolumeMounts(KafkaClientAuthentication authentication, List<VolumeMount> volumeMountList, String tlsVolumeMount, String passwordVolumeMount, String oauthVolumeMount, String oauthVolumeNamePrefix) {
        configureClientAuthenticationVolumeMounts(authentication, volumeMountList, tlsVolumeMount, passwordVolumeMount, oauthVolumeMount, oauthVolumeNamePrefix, "", false, null);
    }

    /**
     * Creates the VolumeMounts used for authentication of Kafka client based components
     *
     * @param authentication    Authentication object from CRD
     * @param volumeMountList    List where the volume mounts will be added
     * @param tlsVolumeMount    Path where the TLS certs should be mounted
     * @param passwordVolumeMount   Path where passwords should be mounted
     * @param oauthCertsVolumeMount Path where the OAuth certificates would be mounted
     * @param oauthVolumeNamePrefix Prefix used for OAuth volume names
     * @param volumeNamePrefix Prefix used for volume mount names
     * @param mountOAuthSecretVolumes Indicates whether OAuth secret volume mounts will be added to the list
     * @param oauthSecretsVolumeMount Path where the OAuth secrets would be mounted
     */
    public static void configureClientAuthenticationVolumeMounts(KafkaClientAuthentication authentication, List<VolumeMount> volumeMountList, String tlsVolumeMount, String passwordVolumeMount, String oauthCertsVolumeMount, String oauthVolumeNamePrefix, String volumeNamePrefix, boolean mountOAuthSecretVolumes, String oauthSecretsVolumeMount) {
        if (authentication != null) {
            if (authentication instanceof KafkaClientAuthenticationTls) {
                KafkaClientAuthenticationTls tlsAuth = (KafkaClientAuthenticationTls) authentication;

                // skipping if a volume mount with same Secret name was already added
                if (!volumeMountList.stream().anyMatch(vm -> vm.getName().equals(volumeNamePrefix + tlsAuth.getCertificateAndKey().getSecretName()))) {
                    volumeMountList.add(VolumeUtils.createVolumeMount(volumeNamePrefix + tlsAuth.getCertificateAndKey().getSecretName(),
                            tlsVolumeMount + tlsAuth.getCertificateAndKey().getSecretName()));
                }
            } else if (authentication instanceof KafkaClientAuthenticationPlain) {
                KafkaClientAuthenticationPlain passwordAuth = (KafkaClientAuthenticationPlain) authentication;
                volumeMountList.add(VolumeUtils.createVolumeMount(volumeNamePrefix + passwordAuth.getPasswordSecret().getSecretName(), passwordVolumeMount + passwordAuth.getPasswordSecret().getSecretName()));
            } else if (authentication instanceof KafkaClientAuthenticationScramSha512) {
                KafkaClientAuthenticationScramSha512 passwordAuth = (KafkaClientAuthenticationScramSha512) authentication;
                volumeMountList.add(VolumeUtils.createVolumeMount(volumeNamePrefix + passwordAuth.getPasswordSecret().getSecretName(), passwordVolumeMount + passwordAuth.getPasswordSecret().getSecretName()));
            } else if (authentication instanceof KafkaClientAuthenticationOAuth) {
                KafkaClientAuthenticationOAuth oauth = (KafkaClientAuthenticationOAuth) authentication;
                volumeMountList.addAll(configureOauthCertificateVolumeMounts(oauthVolumeNamePrefix, oauth.getTlsTrustedCertificates(), oauthCertsVolumeMount));
            
                if (mountOAuthSecretVolumes) {
                    if (oauth.getClientSecret() != null) {
                        volumeMountList.add(VolumeUtils.createVolumeMount(volumeNamePrefix + oauth.getClientSecret().getSecretName(), oauthSecretsVolumeMount + oauth.getClientSecret().getSecretName()));
                    }
                    if (oauth.getAccessToken() != null) {
                        volumeMountList.add(VolumeUtils.createVolumeMount(volumeNamePrefix + oauth.getAccessToken().getSecretName(), oauthSecretsVolumeMount + oauth.getAccessToken().getSecretName()));
                    }
                    if (oauth.getRefreshToken() != null) {
                        volumeMountList.add(VolumeUtils.createVolumeMount(volumeNamePrefix + oauth.getRefreshToken().getSecretName(), oauthSecretsVolumeMount + oauth.getRefreshToken().getSecretName()));
                    }
                }
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

            for (Entry<String, String> entry: getClientAuthenticationProperties(authentication).entrySet()) {
                varList.add(AbstractModel.buildEnvVar(envVarNamer.apply(entry.getKey()), entry.getValue()));
            }
            
            if (authentication instanceof KafkaClientAuthenticationOAuth) {
                KafkaClientAuthenticationOAuth oauth = (KafkaClientAuthenticationOAuth) authentication;

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
     * Get a map of properties related to authentication in Kafka clients.
     *
     * @param authentication    Authentication object with auth configuration
     * @return Map of name/value pairs
     */
    public static Map<String, String> getClientAuthenticationProperties(KafkaClientAuthentication authentication) {
        Map<String, String> properties = new HashMap<>(3);
        
        if (authentication != null) {
            if (authentication instanceof KafkaClientAuthenticationTls) {
                KafkaClientAuthenticationTls tlsAuth = (KafkaClientAuthenticationTls) authentication;
                properties.put(TLS_AUTH_CERT, String.format("%s/%s", tlsAuth.getCertificateAndKey().getSecretName(), tlsAuth.getCertificateAndKey().getCertificate()));
                properties.put(TLS_AUTH_KEY, String.format("%s/%s", tlsAuth.getCertificateAndKey().getSecretName(), tlsAuth.getCertificateAndKey().getKey()));
            } else if (authentication instanceof KafkaClientAuthenticationPlain) {
                KafkaClientAuthenticationPlain passwordAuth = (KafkaClientAuthenticationPlain) authentication;
                properties.put(SASL_USERNAME, passwordAuth.getUsername());
                properties.put(SASL_PASSWORD_FILE, String.format("%s/%s", passwordAuth.getPasswordSecret().getSecretName(), passwordAuth.getPasswordSecret().getPassword()));
                properties.put(SASL_MECHANISM, KafkaClientAuthenticationPlain.TYPE_PLAIN);
            } else if (authentication instanceof KafkaClientAuthenticationScramSha512) {
                KafkaClientAuthenticationScramSha512 passwordAuth = (KafkaClientAuthenticationScramSha512) authentication;
                properties.put(SASL_USERNAME, passwordAuth.getUsername());
                properties.put(SASL_PASSWORD_FILE, String.format("%s/%s", passwordAuth.getPasswordSecret().getSecretName(), passwordAuth.getPasswordSecret().getPassword()));
                properties.put(SASL_MECHANISM, KafkaClientAuthenticationScramSha512.TYPE_SCRAM_SHA_512);
            } else if (authentication instanceof KafkaClientAuthenticationOAuth) {
                KafkaClientAuthenticationOAuth oauth = (KafkaClientAuthenticationOAuth) authentication;
                properties.put(SASL_MECHANISM, KafkaClientAuthenticationOAuth.TYPE_OAUTH);

                List<String> options = new ArrayList<>(2);
                if (oauth.getClientId() != null) options.add(String.format("%s=\"%s\"", ClientConfig.OAUTH_CLIENT_ID, oauth.getClientId()));
                if (oauth.getTokenEndpointUri() != null) options.add(String.format("%s=\"%s\"", ClientConfig.OAUTH_TOKEN_ENDPOINT_URI, oauth.getTokenEndpointUri()));
                if (oauth.isDisableTlsHostnameVerification()) options.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, ""));
                if (!oauth.isAccessTokenIsJwt()) options.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_ACCESS_TOKEN_IS_JWT, false));
                if (oauth.getMaxTokenExpirySeconds() > 0) options.add(String.format("%s=\"%s\"", ClientConfig.OAUTH_MAX_TOKEN_EXPIRY_SECONDS, oauth.getMaxTokenExpirySeconds()));

                properties.put(OAUTH_CONFIG, String.join(" ", options));
            }
        }
        
        return properties;
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

                Volume vol = VolumeUtils.createSecretVolume(volumeName, certSecretSource.getSecretName(), items, isOpenShift);

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
                newVolumeMounts.add(VolumeUtils.createVolumeMount(volumeName, String.format("%s/%s-%d", baseVolumeMount, certSecretSource.getSecretName(), i)));
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
