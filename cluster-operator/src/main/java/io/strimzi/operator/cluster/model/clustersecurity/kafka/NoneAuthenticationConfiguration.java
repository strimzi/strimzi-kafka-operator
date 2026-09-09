/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.clustersecurity.kafka;

import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthenticationType;

/**
 * Class for none authentication configuration
 */
public class NoneAuthenticationConfiguration implements AuthenticationConfiguration {
    /**
     * Constructor for NoneAuthenticationConfiguration
     */
    public NoneAuthenticationConfiguration() { }

    @Override
    public ClusterSecurityAuthenticationType getType() {
        return ClusterSecurityAuthenticationType.NONE;
    }
}
