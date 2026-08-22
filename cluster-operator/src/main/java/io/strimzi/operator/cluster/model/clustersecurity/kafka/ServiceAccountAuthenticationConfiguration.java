/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.clustersecurity.kafka;

import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthentication;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthenticationType;

/**
 * Class for service account authentication configuration
 */
public class ServiceAccountAuthenticationConfiguration implements AuthenticationConfiguration {
    /**
     * The issuer of the token
     */
    public static final String ISSUER = "https://kubernetes.default.svc.cluster.local";

    /**
     * The JWKS URI of the token
     */
    public static final String JWKS_URI = "https://kubernetes.default.svc.cluster.local/openid/v1/jwks";

    private final Integer expirationSeconds;
    private final String audience;

    private ServiceAccountAuthenticationConfiguration(String clusterName, Integer expirationSeconds) {
        this.expirationSeconds = expirationSeconds != null ? expirationSeconds : 3600;
        this.audience = "strimzi.io/kafka/" + clusterName;
    }

    /**
     * Creates ServiceAccountAuthenticationConfiguration from ClusterSecurityAuthentication
     *
     * @param clusterName       Name of the Kafka cluster
     * @param authentication    ClusterSecurityAuthentication from which the configuration is created
     *
     * @return  ServiceAccountAuthenticationConfiguration instance
     */
    public static ServiceAccountAuthenticationConfiguration fromCrd(String clusterName, ClusterSecurityAuthentication authentication) {
        return new ServiceAccountAuthenticationConfiguration(clusterName, authentication.getExpirationSeconds());
    }

    /**
     * Returns the expiration time of the token in seconds
     *
     * @return  Expiration time in seconds
     */
    public Integer expirationSeconds() {
        return expirationSeconds;
    }

    /**
     * Returns the audience of the token
     *
     * @return  Audience of the token
     */
    public String audience() {
        return audience;
    }

    @Override
    public ClusterSecurityAuthenticationType getType() {
        return ClusterSecurityAuthenticationType.SERVICE_ACCOUNT;
    }
}
