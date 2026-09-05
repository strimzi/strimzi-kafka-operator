/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.clustersecurity.kafka;

import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityEncryptionType;

/**
 * Class for TLS encryption configuration
 */
public class TlsEncryptionConfiguration implements EncryptionConfiguration {
    /**
     * Constructor for TlsEncryptionConfiguration
     */
    public TlsEncryptionConfiguration() { }

    @Override
    public ClusterSecurityEncryptionType getType() {
        return ClusterSecurityEncryptionType.TLS;
    }
}
