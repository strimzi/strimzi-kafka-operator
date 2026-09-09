/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.clustersecurity.kafka;

import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthentication;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthenticationType;
import io.strimzi.operator.common.model.InvalidResourceException;

/**
 * Interface for authentication configuration
 */
public interface AuthenticationConfiguration {
    /**
     * Returns the authentication configuration based on the authentication type
     *
     * @param namespace         Namespace of the Kafka cluster
     * @param clusterName       Name of the Kafka cluster
     * @param authentication    ClusterSecurityAuthentication from which the configuration is created
     *
     * @return  AuthenticationConfiguration instance
     */
    static AuthenticationConfiguration fromCrd(String namespace, String clusterName, ClusterSecurityAuthentication authentication)    {
        validate(authentication);

        return switch (authentication != null ? authentication.getType() : ClusterSecurityAuthenticationType.MTLS) {
            case MTLS -> new MtlsAuthenticationConfiguration();
            case NONE -> new NoneAuthenticationConfiguration();
            case SERVICE_ACCOUNT -> ServiceAccountAuthenticationConfiguration.fromCrd(namespace, clusterName, authentication);
        };
    }

    /**
     * Validates the authentication configuration and throws an InvalidResourceException if the authentication configuration is invalid.
     *
     * @param authentication    Authentication configuration to validate
     */
    private static void validate(ClusterSecurityAuthentication authentication) {
        if (authentication != null
                && authentication.getType() != null
                && !ClusterSecurityAuthenticationType.SERVICE_ACCOUNT.equals(authentication.getType())
                && authentication.getExpirationSeconds() != null) {
            throw new InvalidResourceException("The expirationSeconds option in Cluster Security configuration can be used only with service-account authentication type.");
        }
    }

    /**
     * Returns the authentication type
     *
     * @return  The authentication type
     */
    ClusterSecurityAuthenticationType getType();
}
