/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaSpec;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.StatusUtils;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.HashSet;
import java.util.Set;

/**
 * Shared methods for working with KRaft
 */
public class KRaftUtils {
    /**
     * In KRaft mode, multiple features are currently not supported. This method validates the Kafka CR for the
     * unsupported features and if they are used, throws an InvalidResourceException exception.
     *
     * @param kafkaSpec   The .spec section of the Kafka CR which should be checked
     */
    public static void validateKafkaCrForKRaft(KafkaSpec kafkaSpec)   {
        Set<String> errors = new HashSet<>(0);

        if (kafkaSpec == null)  {
            errors.add("The .spec section of the Kafka custom resource is missing");
        }

        if (!errors.isEmpty())  {
            throw new InvalidResourceException("Kafka configuration is not valid: " + errors);
        }
    }

    /**
     * Validates the metadata version
     *
     * @param metadataVersion   Metadata version that should be validated
     */
    public static void validateMetadataVersion(String metadataVersion)   {
        try {
            MetadataVersion version = MetadataVersion.fromVersionString(metadataVersion);

            // KRaft is supposed to be supported from metadata version 3.0-IV1. But only from metadata version 3.3-IV0,
            // the initial metadata version can be set using the kafka-storage.sh utility. And since most metadata
            // versions do not support downgrade, that means 3.3-IV0 is the oldest metadata version that can be used
            // with Strimzi.
            if (version.isLessThan(MetadataVersion.IBP_3_3_IV0)) {
                throw new InvalidResourceException("The oldest supported metadata version is 3.3-IV0");
            }
        } catch (IllegalArgumentException e)    {
            throw new InvalidResourceException("Metadata version " + metadataVersion + " is invalid", e);
        }
    }

    /**
     * In ZooKeeper mode, some of the fields marked as not required (because they are not used in KRaft) are in fact
     * required. This method validates that the fields are present and in case they are missing, it throws an exception.
     *
     * @param kafkaSpec         The .spec section of the Kafka CR which should be checked
     * @param nodePoolsEnabled  Flag indicating whether Node Pools are enabled or not
     */
    public static void validateKafkaCrForZooKeeper(KafkaSpec kafkaSpec, boolean nodePoolsEnabled)   {
        Set<String> errors = new HashSet<>(0);

        if (kafkaSpec != null)  {
            if (kafkaSpec.getZookeeper() == null)   {
                errors.add("The .spec.zookeeper section of the Kafka custom resource is missing. " +
                        "This section is required for a ZooKeeper-based cluster.");
            }

            if (!nodePoolsEnabled)  {
                if (kafkaSpec.getKafka().getReplicas() == null || kafkaSpec.getKafka().getReplicas() == 0)   {
                    errors.add("The .spec.kafka.replicas property of the Kafka custom resource is missing. " +
                            "This property is required for a ZooKeeper-based Kafka cluster that is not using Node Pools.");
                }

                if (kafkaSpec.getKafka().getStorage() == null)   {
                    errors.add("The .spec.kafka.storage section of the Kafka custom resource is missing. " +
                            "This section is required for a ZooKeeper-based Kafka cluster that is not using Node Pools.");
                }
            }
        } else {
            errors.add("The .spec section of the Kafka custom resource is missing");
        }

        if (!errors.isEmpty())  {
            throw new InvalidResourceException("Kafka configuration is not valid: " + errors);
        }
    }

    /**
     * Generates Kafka CR status warnings about the fields ignored in Kraft mode if they are set - the ZooKeeper section
     * and Kafka replicas and storage configuration.
     *
     * @param kafkaCr       The Kafka custom resource
     * @param kafkaStatus   The Kafka Status to add the warnings to
     */
    public static void kraftWarnings(Kafka kafkaCr, KafkaStatus kafkaStatus)   {
        if (kafkaCr.getSpec().getZookeeper() != null)   {
            kafkaStatus.addCondition(StatusUtils.buildWarningCondition("UnusedZooKeeperConfiguration",
                    "The .spec.zookeeper section in the Kafka custom resource is ignored in KRaft mode and " +
                            "should be removed from the custom resource."));
        }

        nodePoolWarnings(kafkaCr, kafkaStatus);
    }

    /**
     * Generates Kafka CR status warnings about the fields ignored when node pools are used if they are set - the
     * replicas and storage configuration.
     *
     * @param kafkaCr       The Kafka custom resource
     * @param kafkaStatus   The Kafka Status to add the warnings to
     */
    public static void nodePoolWarnings(Kafka kafkaCr, KafkaStatus kafkaStatus)   {
        if (kafkaCr.getSpec().getKafka() != null
                && kafkaCr.getSpec().getKafka().getReplicas() != null
                && kafkaCr.getSpec().getKafka().getReplicas() > 0) {
            kafkaStatus.addCondition(StatusUtils.buildWarningCondition("UnusedReplicasConfiguration",
                    "The .spec.kafka.replicas property in the Kafka custom resource is ignored when node pools " +
                            "are used and should be removed from the custom resource."));
        }

        if (kafkaCr.getSpec().getKafka() != null
                && kafkaCr.getSpec().getKafka().getStorage() != null) {
            kafkaStatus.addCondition(StatusUtils.buildWarningCondition("UnusedStorageConfiguration",
                    "The .spec.kafka.storage section in the Kafka custom resource is ignored when node pools " +
                            "are used and should be removed from the custom resource."));
        }
    }

    /**
     * Validate the Kafka version set in the Kafka custom resource (in spec.kafka.version), together with the
     * metadata version (in spec.kafka.metadataVersion) and the configured inter.broker.protocol.version
     * and log.message.format.version. (in spec.kafka.config).
     * They need to be all aligned and at least 3.7.0 to support ZooKeeper to KRaft migration, otherwise the check
     * throws an {@code InvalidResourceException}.
     *
     * @param kafkaVersionFromCr    Kafka version from the custom resource
     * @param metadataVersionFromCr Metadata version from the custom resource
     * @param interBrokerProtocolVersionFromCr  Inter broker protocol version from the configuration of the Kafka custom resource
     * @param logMessageFormatVersionFromCr Log message format version from the configuration of the Kafka custom resource
     */
    public static void validateVersionsForKRaftMigration(String kafkaVersionFromCr, String metadataVersionFromCr,
                                                         String interBrokerProtocolVersionFromCr, String logMessageFormatVersionFromCr) {
        // validate 3.7.0 <= kafka.version && metadataVersion/IBP/LMF == kafka.version

        MetadataVersion kafkaVersion = MetadataVersion.fromVersionString(kafkaVersionFromCr);
        // this should check that spec.kafka.version is >= 3.7.0
        boolean isMigrationSupported = kafkaVersion.isAtLeast(MetadataVersion.IBP_3_7_IV0);

        MetadataVersion metadataVersion = MetadataVersion.fromVersionString(metadataVersionFromCr);
        MetadataVersion interBrokerProtocolVersion = MetadataVersion.fromVersionString(interBrokerProtocolVersionFromCr);
        MetadataVersion logMessageFormatVersion = MetadataVersion.fromVersionString(logMessageFormatVersionFromCr);

        if (!isMigrationSupported ||
                metadataVersion.compareTo(interBrokerProtocolVersion) != 0 ||
                metadataVersion.compareTo(logMessageFormatVersion) != 0) {
            String message = String.format("Migration cannot be performed with Kafka version %s, metadata version %s, inter.broker.protocol.version %s, log.message.format.version %s. " +
                            "Please make sure the Kafka version, metadata version, inter.broker.protocol.version and log.message.format.version " +
                            "are all set to the same value, which must be equal to, or higher than 3.7.0",
                    kafkaVersion, metadataVersion, interBrokerProtocolVersion, logMessageFormatVersion);
            throw new InvalidResourceException(message);
        }
    }
}
