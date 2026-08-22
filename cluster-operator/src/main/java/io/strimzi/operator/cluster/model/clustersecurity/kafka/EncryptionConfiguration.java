/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.clustersecurity.kafka;

import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityEncryption;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityEncryptionType;

/**
 * Interface for encryption configuration
 */
public interface EncryptionConfiguration {
    /**
     * Returns the encryption configuration based on the encryption type
     *
     * @param encryption    The ClusterSecurityEncryption from which the configuration is created
     *
     * @return  EncryptionConfiguration instance
     */
    static EncryptionConfiguration fromCrd(ClusterSecurityEncryption encryption)    {
        if (encryption == null || encryption.getType() == null || ClusterSecurityEncryptionType.TLS.equals(encryption.getType())) {
            return new TlsEncryptionConfiguration();
        } else if (ClusterSecurityEncryptionType.NONE.equals(encryption.getType())) {
            return new NoneEncryptionConfiguration();
        } else  {
            throw new IllegalArgumentException("Unsupported Cluster Security encryption type: " + encryption.getType());
        }
    }

    /**
     * Returns the encryption type
     *
     * @return  The encryption type
     */
    ClusterSecurityEncryptionType getType();
}
