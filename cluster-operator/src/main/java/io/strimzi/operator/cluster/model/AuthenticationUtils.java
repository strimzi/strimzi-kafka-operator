/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationPlain;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScram;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationTls;
import io.strimzi.operator.common.model.InvalidResourceException;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Function;

/**
 * Utils for working with different authentication types
 */
public class AuthenticationUtils {
    /**
     * Key for a SASL username
     */
    public static final String SASL_USERNAME = "SASL_USERNAME";

    /**
     * Key for a SASL mechanism
     */
    public static final String SASL_MECHANISM = "SASL_MECHANISM";

    private static final String TLS_AUTH_CERT = "TLS_AUTH_CERT";
    private static final String TLS_AUTH_KEY = "TLS_AUTH_KEY";
    private static final String SASL_PASSWORD_FILE = "SASL_PASSWORD_FILE";

    private AuthenticationUtils() { }

    /**
     * Validates Kafka client authentication for all components based on Apache Kafka clients.
     *
     * @param authentication    The authentication object from CRD
     * @param tls   Indicates whether TLS is enabled or not
     *
     * @return  A warning message
     */
    @SuppressWarnings({"BooleanExpressionComplexity"})
    public static String validateClientAuthentication(KafkaClientAuthentication authentication, boolean tls) {
        String warnMsg = "";
        if (authentication != null)   {
            if (authentication instanceof KafkaClientAuthenticationTls auth) {
                if (auth.getCertificateAndKey() != null) {
                    if (!tls) {
                        warnMsg = "TLS configuration missing: related TLS client authentication will not work properly";
                    }
                } else {
                    throw new InvalidResourceException("TLS Client authentication selected, but no certificate and key configured.");
                }
            } else if (authentication instanceof KafkaClientAuthenticationScram auth)    {
                if (auth.getUsername() == null || auth.getPasswordSecret() == null) {
                    throw new InvalidResourceException(String.format("%s authentication selected, but username or password configuration is missing.", auth.getType().toUpperCase(Locale.ENGLISH)));
                }
            } else if (authentication instanceof KafkaClientAuthenticationPlain auth) {
                if (auth.getUsername() == null || auth.getPasswordSecret() == null) {
                    throw new InvalidResourceException("PLAIN authentication selected, but username or password configuration is missing.");
                }
            }
        }
        return warnMsg;
    }

    /**
     * Creates the Volumes used for authentication of Kafka client based components
     *
     * @param authentication    Authentication object from CRD
     * @param volumeList        List where the volumes will be added
     * @param isOpenShift       Indicates whether we run on OpenShift or not
     * @param volumeNamePrefix  Prefix used for volume names
     */
    public static void configureClientAuthenticationVolumes(KafkaClientAuthentication authentication, List<Volume> volumeList, boolean isOpenShift, String volumeNamePrefix)   {
        if (authentication != null) {
            if (authentication instanceof KafkaClientAuthenticationTls tlsAuth) {
                addNewVolume(volumeList, volumeNamePrefix, tlsAuth.getCertificateAndKey().getSecretName(), isOpenShift);
            } else if (authentication instanceof KafkaClientAuthenticationPlain passwordAuth) {
                addNewVolume(volumeList, volumeNamePrefix, passwordAuth.getPasswordSecret().getSecretName(), isOpenShift);
            } else if (authentication instanceof KafkaClientAuthenticationScram scramAuth) {
                addNewVolume(volumeList, volumeNamePrefix, scramAuth.getPasswordSecret().getSecretName(), isOpenShift);
            }
        }
    }

    /**
     * Creates the Volumes used for authentication of Kafka client based components, checking that the named volume has not already been
     * created.
     */
    private static void addNewVolume(List<Volume> volumeList, String volumeNamePrefix, String secretName, boolean isOpenShift) {
        // skipping if a volume with same name was already added
        if (volumeList.stream().noneMatch(v -> v.getName().equals(volumeNamePrefix + secretName))) {
            volumeList.add(VolumeUtils.createSecretVolume(volumeNamePrefix + secretName, secretName, isOpenShift));
        }
    }

    /**
     * Creates the VolumeMounts used for authentication of Kafka client based components
     *
     * @param authentication        Authentication object from CRD
     * @param volumeMountList       List where the volume mounts will be added
     * @param tlsVolumeMount        Path where the TLS certs should be mounted
     * @param passwordVolumeMount   Path where passwords should be mounted
     * @param volumeNamePrefix      Prefix used for volume mount names
     */
    public static void configureClientAuthenticationVolumeMounts(KafkaClientAuthentication authentication, List<VolumeMount> volumeMountList, String tlsVolumeMount, String passwordVolumeMount, String volumeNamePrefix) {
        if (authentication != null) {
            if (authentication instanceof KafkaClientAuthenticationTls tlsAuth) {
                // skipping if a volume mount with same Secret name was already added
                if (volumeMountList.stream().noneMatch(vm -> vm.getName().equals(volumeNamePrefix + tlsAuth.getCertificateAndKey().getSecretName()))) {
                    volumeMountList.add(VolumeUtils.createVolumeMount(volumeNamePrefix + tlsAuth.getCertificateAndKey().getSecretName(),
                            tlsVolumeMount + tlsAuth.getCertificateAndKey().getSecretName()));
                }
            } else if (authentication instanceof KafkaClientAuthenticationPlain passwordAuth) {
                volumeMountList.add(VolumeUtils.createVolumeMount(volumeNamePrefix + passwordAuth.getPasswordSecret().getSecretName(), passwordVolumeMount + passwordAuth.getPasswordSecret().getSecretName()));
            } else if (authentication instanceof KafkaClientAuthenticationScram scramAuth) {
                volumeMountList.add(VolumeUtils.createVolumeMount(volumeNamePrefix + scramAuth.getPasswordSecret().getSecretName(), passwordVolumeMount + scramAuth.getPasswordSecret().getSecretName()));
            }
        }
    }

    /**
     * Configured environment variables related to authentication in Kafka clients within Strimzi-based containers
     *
     * @param authentication    Authentication object with auth configuration
     * @param varList   List where the new environment variables should be added
     * @param envVarNamer   Function for naming the environment variables (ech components is using different names)
     */
    public static void configureClientAuthenticationEnvVars(KafkaClientAuthentication authentication, List<EnvVar> varList, Function<String, String> envVarNamer)   {
        if (authentication != null) {
            if (authentication instanceof KafkaClientAuthenticationTls tlsAuth) {
                varList.add(ContainerUtils.createEnvVar(envVarNamer.apply(TLS_AUTH_CERT), String.format("%s/%s", tlsAuth.getCertificateAndKey().getSecretName(), tlsAuth.getCertificateAndKey().getCertificate())));
                varList.add(ContainerUtils.createEnvVar(envVarNamer.apply(TLS_AUTH_KEY), String.format("%s/%s", tlsAuth.getCertificateAndKey().getSecretName(), tlsAuth.getCertificateAndKey().getKey())));
            } else if (authentication instanceof KafkaClientAuthenticationPlain passwordAuth) {
                varList.add(ContainerUtils.createEnvVar(envVarNamer.apply(SASL_USERNAME), passwordAuth.getUsername()));
                varList.add(ContainerUtils.createEnvVar(envVarNamer.apply(SASL_PASSWORD_FILE), String.format("%s/%s", passwordAuth.getPasswordSecret().getSecretName(), passwordAuth.getPasswordSecret().getPassword())));
                varList.add(ContainerUtils.createEnvVar(envVarNamer.apply(SASL_MECHANISM), KafkaClientAuthenticationPlain.TYPE_PLAIN));
            } else if (authentication instanceof KafkaClientAuthenticationScram scramAuth) {
                varList.add(ContainerUtils.createEnvVar(envVarNamer.apply(SASL_USERNAME), scramAuth.getUsername()));
                varList.add(ContainerUtils.createEnvVar(envVarNamer.apply(SASL_PASSWORD_FILE), String.format("%s/%s", scramAuth.getPasswordSecret().getSecretName(), scramAuth.getPasswordSecret().getPassword())));
                varList.add(ContainerUtils.createEnvVar(envVarNamer.apply(SASL_MECHANISM), scramAuth.getType()));
            }
        }
    }

    /**
     * Generates a JAAS configuration string based on the provided module name and options.
     * The flag will always be "required".
     *
     * @param moduleName The name of the JAAS module to be configured.
     * @param options A Map containing the options to be set for the JAAS module.
     *               The options are represented as key-value pairs, where both the key and
     *               the value must be non-null String objects.
     * @return The JAAS configuration.
     * @throws IllegalArgumentException If the moduleName is empty, or it contains '=' or ';',
     *                                  or if any key or value in the options map is empty,
     *                                  or they contain '=' or ';'.
     */
    public static String jaasConfig(String moduleName, Map<String, String> options) {
        StringJoiner joiner = new StringJoiner(" ");
        for (Entry<String, String> entry : options.entrySet()) {
            String key = Objects.requireNonNull(entry.getKey());
            String value = Objects.requireNonNull(entry.getValue());
            if (key.contains("=") || key.contains(";")) {
                throw new IllegalArgumentException("Keys must not contain '=' or ';'");
            }
            if (moduleName.isEmpty() || moduleName.contains(";") || moduleName.contains("=")) {
                throw new IllegalArgumentException("module name must be not empty and must not contain '=' or ';'");
            } else {
                joiner.add(key + "=\"" + value + "\"");
            }
        }
        return moduleName + " required " + joiner + ";";
    }
}
