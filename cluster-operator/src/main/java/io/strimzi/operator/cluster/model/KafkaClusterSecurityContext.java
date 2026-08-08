/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurity;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityAuthenticationType;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityEncryptionType;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityStatus;
import io.strimzi.api.kafka.model.kafka.clustersecurity.ClusterSecurityStatusBuilder;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.InvalidResourceException;

/**
 * Class that holds the security configuration of the Kafka cluster.
 */
public class KafkaClusterSecurityContext {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaClusterSecurityContext.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String INTERNAL_CLUSTER_SECURITY_ANNOTATION = "strimzi.io/internal-cluster-security";

    /**
     * The default Kafka Cluster Security Context configuration
     */
    public static final KafkaClusterSecurityContext DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT =  new KafkaClusterSecurityContext(ClusterSecurityEncryptionType.STRIMZI_TLS, ClusterSecurityAuthenticationType.STRIMZI_MTLS);

    private final ClusterSecurityEncryptionType encryptionType;
    private final ClusterSecurityAuthenticationType authenticationType;

    /* test */ KafkaClusterSecurityContext(ClusterSecurityEncryptionType encryptionType, ClusterSecurityAuthenticationType authenticationType) {
        this.encryptionType = encryptionType;
        this.authenticationType = authenticationType;

        validateDesiredConfiguration();
    }

    /**
     * Creates an instance of this class from the Kafka CR.
     *
     * @param kafka     The Kafka CR
     *
     * @return  KafkaClusterSecurityContext instance corresponding to the Kafka CR
     */
    public static KafkaClusterSecurityContext fromCrd(Kafka kafka) {
        ClusterSecurity clusterSecurity = Annotations.hasAnnotation(kafka, INTERNAL_CLUSTER_SECURITY_ANNOTATION) ? deserializeSpec(Annotations.stringAnnotation(kafka, INTERNAL_CLUSTER_SECURITY_ANNOTATION, null)) : null;
        ClusterSecurityStatus clusterSecurityStatus = kafka.getStatus() != null && kafka.getStatus().getClusterSecurity() != null ? deserializeStatus(kafka.getStatus().getClusterSecurity()) : null;

        if (clusterSecurity == null && clusterSecurityStatus == null) {
            // Cluster Security does not exist in status, and it is not configured in the annotation either. We create
            // the context with default values.
            return DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT;
        } else if (clusterSecurity == null) {
            // Cluster Security exists in status, but it is not configured in the annotation. We need to doublecheck
            // that the status uses the default configuration.
            validateSpecAndStatusMatch(ClusterSecurityEncryptionType.STRIMZI_TLS, ClusterSecurityAuthenticationType.STRIMZI_MTLS, clusterSecurityStatus);
            return DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT;
        } else if (clusterSecurityStatus == null) {
            // Cluster Security does not exist in status, but it is configured in the annotation. This is a new cluster
            // or follows the migration process. We use the configuration from the annotation.
            return createContextFromSpec(clusterSecurity);
        } else {
            // Cluster Security exists in status and in the annotation. We need to doublecheck that the status uses
            // the same configuration as the annotation.
            validateSpecAndStatusMatch(clusterSecurity, clusterSecurityStatus);
            return createContextFromSpec(clusterSecurity);
        }
    }

    private static KafkaClusterSecurityContext createContextFromSpec(ClusterSecurity clusterSecurity) {
        return new KafkaClusterSecurityContext(
                clusterSecurity.getEncryption() != null && clusterSecurity.getEncryption().getType() != null ? clusterSecurity.getEncryption().getType() : ClusterSecurityEncryptionType.STRIMZI_TLS,
                clusterSecurity.getAuthentication() != null && clusterSecurity.getAuthentication().getType() != null ? clusterSecurity.getAuthentication().getType() : ClusterSecurityAuthenticationType.STRIMZI_MTLS
        );
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
     * Deserializes the ClusterSecurity from the untyped object to the typed object.
     *
     * @param clusterSecurityJson  JSON String with Cluster Security configuration
     *
     * @return  Typed Cluster Security
     */
    public static ClusterSecurity deserializeSpec(String clusterSecurityJson) {
        if (clusterSecurityJson == null) {
            throw new InvalidResourceException("ClusterSecurity is null and cannot be deserialized.");
        } else {
            try {
                return OBJECT_MAPPER.readValue(clusterSecurityJson, ClusterSecurity.class);
            } catch (IllegalArgumentException | JsonProcessingException e) {
                throw new InvalidResourceException("Failed to deserialize ClusterSecurity configuration", e);
            }
        }
    }

    private static void validateSpecAndStatusMatch(ClusterSecurity clusterSecurity, ClusterSecurityStatus clusterSecurityStatus) {
        validateSpecAndStatusMatch(clusterSecurity.getEncryption().getType(), clusterSecurity.getAuthentication().getType(), clusterSecurityStatus);
    }

    private static void validateSpecAndStatusMatch(ClusterSecurityEncryptionType desiredEncryptionType, ClusterSecurityAuthenticationType desiredAuthenticationType, ClusterSecurityStatus clusterSecurityStatus) {
        if (desiredEncryptionType != clusterSecurityStatus.getEncryption().getType()
                || desiredAuthenticationType != clusterSecurityStatus.getAuthentication().getType()) {
            LOGGER.errorOp("Desired Cluster Security configuration (encryption: {}, authentication: {}) does not match the current configuration (encryption: {}, authentication: {}). If you want to change the Cluster Security configuration, please follow the documentation.",
                    desiredEncryptionType, desiredAuthenticationType, clusterSecurityStatus.getEncryption().getType(), clusterSecurityStatus.getAuthentication().getType());
            throw new InvalidResourceException("Desired Cluster Security configuration does not match the current configuration. " +
                    "If you want to change the Cluster Security configuration, please follow the documentation.");
        }
    }

    /**
     * Validates the desired configuration. This checks things such as that the encryption and authentication mechanisms
     * are compatible and so on.
     */
    private void validateDesiredConfiguration() {
        // Check that mTLS is not enabled when TLS is disabled
        if (authenticationType == ClusterSecurityAuthenticationType.STRIMZI_MTLS && encryptionType != ClusterSecurityEncryptionType.STRIMZI_TLS) {
            LOGGER.errorOp("Desired Cluster Security configuration (encryption: {}, authentication: {}) is not valid: mTLS authentication can be used only with enabled TLS encryption",
                    encryptionType, authenticationType);
            throw new InvalidResourceException("Desired Cluster Security configuration is not valid: mTLS authentication can be used only with enabled TLS encryption.");
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

    /**
     * Returns whether Strimzi-based TLS encryption should be used or not.
     *
     * @return  True if the encryption type is STRIMZI_TLS, false otherwise
     */
    public boolean isStrimziTlsEncryption() {
        return encryptionType == ClusterSecurityEncryptionType.STRIMZI_TLS;
    }

    /**
     * Returns whether Strimzi-based mTLS authentication should be used or not.
     *
     * @return  True if the authentication type is STRIMZI_MTLS, false otherwise
     */
    public boolean isStrimziMtlsAuthentication() {
        return authenticationType == ClusterSecurityAuthenticationType.STRIMZI_MTLS;
    }
}
