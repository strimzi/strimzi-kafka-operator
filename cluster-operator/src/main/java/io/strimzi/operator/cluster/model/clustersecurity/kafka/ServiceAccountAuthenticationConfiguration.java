/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.clustersecurity.kafka;

import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthentication;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthenticationType;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;

/**
 * Class for service account authentication configuration
 */
public class ServiceAccountAuthenticationConfiguration implements AuthenticationConfiguration {
    /**
     * The default issuer of the token used when it was not detected from the Kubernetes OIDC discovery endpoint. It
     * uses the cluster DNS domain configured through the KUBERNETES_SERVICE_DNS_DOMAIN environment variable (defaults
     * to cluster.local).
     */
    public static final String ISSUER = "https://kubernetes.default.svc." + System.getenv().getOrDefault("KUBERNETES_SERVICE_DNS_DOMAIN", "cluster.local");

    /**
     * The default JWKS URI of the token used when it was not detected from the Kubernetes OIDC discovery endpoint
     */
    private static final String JWKS_URI = ISSUER + "/openid/v1/jwks";

    private final Integer expirationSeconds;
    private final String audience;
    private final String issuer;
    private final String jwksUri;

    private ServiceAccountAuthenticationConfiguration(String namespace, String clusterName, Integer expirationSeconds, PlatformFeaturesAvailability.OidcDiscovery oidcDiscovery) {
        this.expirationSeconds = expirationSeconds != null ? expirationSeconds : 3600;
        this.audience = "strimzi.io/kafka/" + namespace + "/" + clusterName;

        if (oidcDiscovery != null) {
            this.issuer = oidcDiscovery.issuer();
            this.jwksUri = oidcDiscovery.jwksUri();
        } else {
            this.issuer = ISSUER;
            this.jwksUri = JWKS_URI;
        }
    }

    /**
     * Creates ServiceAccountAuthenticationConfiguration from ClusterSecurityAuthentication
     *
     * @param namespace         Namespace of the Kafka cluster
     * @param clusterName       Name of the Kafka cluster
     * @param authentication    ClusterSecurityAuthentication from which the configuration is created
     * @param oidcDiscovery     OIDC discovery information detected from the Kubernetes API. When null, the default
     *                          issuer and JWKS URI are used.
     *
     * @return  ServiceAccountAuthenticationConfiguration instance
     */
    public static ServiceAccountAuthenticationConfiguration fromCrd(String namespace, String clusterName, ClusterSecurityAuthentication authentication, PlatformFeaturesAvailability.OidcDiscovery oidcDiscovery) {
        return new ServiceAccountAuthenticationConfiguration(namespace, clusterName, authentication.getExpirationSeconds(), oidcDiscovery);
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

    /**
     * Returns the issuer of the token
     *
     * @return  Issuer of the token
     */
    public String issuer() {
        return issuer;
    }

    /**
     * Returns the JWKS URI of the token
     *
     * @return  JWKS URI of the token
     */
    public String jwksUri() {
        return jwksUri;
    }

    @Override
    public ClusterSecurityAuthenticationType getType() {
        return ClusterSecurityAuthenticationType.SERVICE_ACCOUNT;
    }
}
