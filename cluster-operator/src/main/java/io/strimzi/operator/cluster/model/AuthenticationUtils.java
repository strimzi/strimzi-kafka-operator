/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.rbac.PolicyRule;
import io.fabric8.kubernetes.api.model.rbac.PolicyRuleBuilder;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleRef;
import io.fabric8.kubernetes.api.model.rbac.RoleRefBuilder;
import io.fabric8.kubernetes.api.model.rbac.Subject;
import io.fabric8.kubernetes.api.model.rbac.SubjectBuilder;
import io.strimzi.api.kafka.model.common.ClientTls;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationPlain;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScram;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationTls;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

/**
 * Utils for working with different authentication types
 */
public class AuthenticationUtils {

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

    /**
     * Collects the names of Secrets that need to be accessible for authentication and TLS configuration.
     * This includes trusted certificates for TLS connections and authentication credentials.
     *
     * @param tls                           The TLS configuration containing trusted certificates
     * @param authentication                The authentication configuration (TLS, PLAIN, or SCRAM)
     * @param tlsTrustedCertsSecretName     The name of the secret containing TLS trusted certificates (component-specific)
     * @return List of secret names that need to be accessible
     */
    public static Set<String> getAuthenticationSecretsToAccess(ClientTls tls, KafkaClientAuthentication authentication, String tlsTrustedCertsSecretName) {
        Set<String> secretNames = new HashSet<>();

        // Add TLS trusted certificates secret if configured
        if (tls != null && tls.getTrustedCertificates() != null && !tls.getTrustedCertificates().isEmpty()) {
            secretNames.add(tlsTrustedCertsSecretName);
        }

        // Add authentication secrets based on authentication type
        if (authentication != null) {
            if (authentication instanceof KafkaClientAuthenticationTls tlsAuth && tlsAuth.getCertificateAndKey() != null) {
                secretNames.add(tlsAuth.getCertificateAndKey().getSecretName());
            } else if (authentication instanceof KafkaClientAuthenticationPlain plainAuth) {
                secretNames.add(plainAuth.getPasswordSecret().getSecretName());
            } else if (authentication instanceof KafkaClientAuthenticationScram scramAuth) {
                secretNames.add(scramAuth.getPasswordSecret().getSecretName());
            }
        }

        return secretNames;
    }

    /**
     * Generates a Kubernetes Role for accessing authentication secrets.
     * The Role grants "get" permission on specific secrets needed for authentication and TLS.
     * Returns null if no secrets need to be accessed.
     *
     * @param componentName     The name of the component (used as role name)
     * @param namespace         The namespace where the role will be created
     * @param secretNames       List of secret names that need to be accessible
     * @param labels            Labels to apply to the role
     * @param ownerReference    Owner reference for the role
     * @return Role resource or null if no secrets need to be accessed
     */
    public static Role generateAuthenticationSecretsRole(
            String componentName,
            String namespace,
            Set<String> secretNames,
            Labels labels,
            OwnerReference ownerReference) {
        if (secretNames.isEmpty()) {
            return null;
        } else {
            List<PolicyRule> rules = List.of(new PolicyRuleBuilder()
                    .withApiGroups("")
                    .withResources("secrets")
                    .withVerbs("get")
                    .withResourceNames(secretNames.stream().toList())
                    .build());

            return RbacUtils.createRole(componentName, namespace, rules, labels, ownerReference, null);
        }
    }

    /**
     * Generates a Kubernetes RoleBinding for the authentication secrets Role.
     * Binds the ServiceAccount to the Role created for the component.
     * Returns null if no secrets need to be accessed.
     *
     * @param roleBindingName   The name of the role binding
     * @param componentName     The name of the component (used as service account name)
     * @param namespace         The namespace where the role binding will be created
     * @param secretNames       List of secret names that need to be accessible
     * @param labels            Labels to apply to the role binding
     * @param ownerReference    Owner reference for the role binding
     * @return RoleBinding resource or null if no secrets need to be accessed
     */
    public static RoleBinding generateAuthenticationSecretsRoleBinding(
            String roleBindingName,
            String componentName,
            String namespace,
            Set<String> secretNames,
            Labels labels,
            OwnerReference ownerReference) {
        if (secretNames.isEmpty()) {
            return null;
        } else {
            Subject subject = new SubjectBuilder()
                    .withKind("ServiceAccount")
                    .withName(componentName)
                    .withNamespace(namespace)
                    .build();

            RoleRef roleRef = new RoleRefBuilder()
                    .withName(componentName)
                    .withApiGroup("rbac.authorization.k8s.io")
                    .withKind("Role")
                    .build();

            return RbacUtils.createRoleBinding(roleBindingName, namespace, roleRef, List.of(subject), labels, ownerReference, null);
        }
    }
}
