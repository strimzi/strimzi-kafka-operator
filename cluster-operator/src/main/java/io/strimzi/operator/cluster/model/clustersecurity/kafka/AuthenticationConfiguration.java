/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.clustersecurity.kafka;

import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthentication;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthenticationType;

/**
 * Interface for authentication configuration
 */
public interface AuthenticationConfiguration {
    /**
     * Returns the authentication configuration based on the authentication type
     *
     * @param clusterName       Name of the Kafka cluster
     * @param authentication    ClusterSecurityAuthentication from which the configuration is created
     *
     * @return  AuthenticationConfiguration instance
     */
    static AuthenticationConfiguration fromCrd(String clusterName, ClusterSecurityAuthentication authentication)    {
        if (authentication == null || authentication.getType() == null || ClusterSecurityAuthenticationType.MTLS.equals(authentication.getType())) {
            return new MtlsAuthenticationConfiguration();
        } else if (ClusterSecurityAuthenticationType.NONE.equals(authentication.getType())) {
            return new NoneAuthenticationConfiguration();
        } else if (ClusterSecurityAuthenticationType.SERVICE_ACCOUNT.equals(authentication.getType())) {
            return ServiceAccountAuthenticationConfiguration.fromCrd(clusterName, authentication);
        } else {
            throw new IllegalArgumentException("Unknown Cluster Security authentication type: " + authentication.getType());
        }
    }

    /**
     * Returns the authentication type
     *
     * @return  The authentication type
     */
    ClusterSecurityAuthenticationType getType();
}
