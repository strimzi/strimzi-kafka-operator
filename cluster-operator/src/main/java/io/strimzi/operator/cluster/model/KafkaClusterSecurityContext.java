/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthenticationType;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityEncryptionType;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityStatus;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityStatusBuilder;
import io.strimzi.operator.common.model.InvalidResourceException;

/**
 * Class that holds the security configuration of the Kafka cluster.
 */
public class KafkaClusterSecurityContext {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final ClusterSecurityEncryptionType encryptionType;
    private final ClusterSecurityAuthenticationType authenticationType;

    private KafkaClusterSecurityContext(ClusterSecurityEncryptionType encryptionType, ClusterSecurityAuthenticationType authenticationType) {
        this.encryptionType = encryptionType;
        this.authenticationType = authenticationType;
    }

    /**
     * Creates an instance of this class from the Kafka CR.
     *
     * @param kafka     The Kafka CR
     *
     * @return  KafkaClusterSecurityContext instance corresponding to the Kafka CR
     */
    public static KafkaClusterSecurityContext fromCrd(Kafka kafka) {
        if (kafka.getStatus() != null && kafka.getStatus().getClusterSecurity() != null) {
            // Cluster Security already exists in status. Today we just validate it and re-use it. In the future, this
            // will be used to validate the user-configured security from the CR and ensure that the security configuration
            // is not changed in a way that is not supported.
            ClusterSecurityStatus clusterSecurityStatus = deserializeStatus(kafka.getStatus().getClusterSecurity());
            return new KafkaClusterSecurityContext(clusterSecurityStatus.getEncryption().getType(), clusterSecurityStatus.getAuthentication().getType());
        } else {
            // Cluster Security does not exist in status. This is a new cluster or a cluster that follows the migration
            // procedure. Today we just create the context with the default configuration. In the future, this will take
            // over the user-configured settings.
            return new KafkaClusterSecurityContext(ClusterSecurityEncryptionType.STRIMZI_TLS, ClusterSecurityAuthenticationType.STRIMZI_MTLS);
        }
    }

    /**
     * Deserializes the ClusterSecurityStatus from the untyped object to the typed object.
     *
     * @param untypedClusterSecurityStatus  Untyped Cluster Security Status
     *
     * @return  Typed Cluster Security Status
     */
    public static ClusterSecurityStatus deserializeStatus(Object untypedClusterSecurityStatus) {
        if (untypedClusterSecurityStatus == null) {
            throw new InvalidResourceException("ClusterSecurityStatus is null and cannot be deserialized.");
        } else {
            try {
                ClusterSecurityStatus status = OBJECT_MAPPER.convertValue(untypedClusterSecurityStatus, ClusterSecurityStatus.class);

                if (status.getEncryption() == null || status.getEncryption().getType() == null || status.getAuthentication() == null || status.getAuthentication().getType() == null) {
                    throw new InvalidResourceException("Invalid ClusterSecurityStatus: encryption or authentication configuration is not set");
                }

                return status;
            } catch (IllegalArgumentException e) {
                throw new InvalidResourceException("Failed to deserialize ClusterSecurityStatus", e);
            }
        }
    }

    /**
     * Exports the Cluster Security Context for storing it in the `.status` section of the Kafka CR.
     *
     * @return  ClusterSecurityStatus instance corresponding to this context
     */
    public ClusterSecurityStatus toStatus() {
        return new ClusterSecurityStatusBuilder()
                .withNewEncryption()
                    .withType(encryptionType)
                .endEncryption()
                .withNewAuthentication()
                    .withType(authenticationType)
                .endAuthentication()
                .build();
    }
}
