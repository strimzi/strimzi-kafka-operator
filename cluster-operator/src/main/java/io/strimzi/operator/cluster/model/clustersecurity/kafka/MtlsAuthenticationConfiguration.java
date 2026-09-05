/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.clustersecurity.kafka;

import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthenticationType;

/**
 * Class for mTLS authentication configuration
 */
public class MtlsAuthenticationConfiguration implements AuthenticationConfiguration {
    /**
     * Constructor for MtlsAuthenticationConfiguration
     */
    public MtlsAuthenticationConfiguration() { }

    @Override
    public ClusterSecurityAuthenticationType getType() {
        return ClusterSecurityAuthenticationType.MTLS;
    }
}
