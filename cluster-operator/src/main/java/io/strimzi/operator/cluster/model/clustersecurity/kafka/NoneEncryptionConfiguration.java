/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.clustersecurity.kafka;

import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityEncryptionType;

/**
 * Class for none encryption configuration
 */
public class NoneEncryptionConfiguration implements EncryptionConfiguration {
    /**
     * Constructor for NoneEncryptionConfiguration
     */
    public NoneEncryptionConfiguration() { }

    @Override
    public ClusterSecurityEncryptionType getType() {
        return ClusterSecurityEncryptionType.NONE;
    }
}
