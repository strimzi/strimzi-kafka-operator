/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.operator.cluster.model.KafkaConfiguration;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlConfigurationParameters;


/**
 * Represents a model for the Cruise Control Metrics Reporter
 *
 * @param topicName             Name of the Metrics Reporter topic
 * @param numPartitions         Number of partitions of the Metrics Reporter topic
 * @param replicationFactor     Replication factor of the Metrics Reporter topic
 * @param minInSyncReplicas     Minimal number of in sync replicas of the metrics reporter topic
 */
public record CruiseControlMetricsReporter(String topicName, Integer numPartitions, Integer replicationFactor, Integer minInSyncReplicas) {
    // Configuration field names
    private static final String KAFKA_NUM_PARTITIONS_CONFIG_FIELD = "num.partitions";
    private static final String KAFKA_REPLICATION_FACTOR_CONFIG_FIELD = "default.replication.factor";

    /**
     * Kafka configuration option for configuring metrics reporters
     */
    public static final String KAFKA_METRIC_REPORTERS_CONFIG_FIELD = "metric.reporters";
    /**
     * Class of the Cruise Control Metrics reporter
     */
    public static final String CRUISE_CONTROL_METRIC_REPORTER = "com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter";

    /**
     * Creates an CruiseControlMetricsReporter instance based on the Kafka custom resource, its configuration and number of brokers
     *
     * @param kafka             The Kafka custom resource
     * @param configuration     The user-provider configuration of the Kafka cluster
     * @param numberOfBrokers   Number of broker nodes in the Kafka cluster
     *
     * @return  Instance of CruiseControlMetricsReporter or null if Cruise Control is not enabled
     */
    public static CruiseControlMetricsReporter fromCrd(Kafka kafka, KafkaConfiguration configuration, long numberOfBrokers)  {
        if (kafka.getSpec().getCruiseControl() != null) {
            String topicName = CruiseControlConfigurationParameters.DEFAULT_METRIC_REPORTER_TOPIC_NAME;
            if (kafka.getSpec().getCruiseControl().getConfig() != null
                    && kafka.getSpec().getCruiseControl().getConfig().get(CruiseControlConfigurationParameters.METRIC_REPORTER_TOPIC_NAME.getValue()) != null)  {
                topicName = kafka.getSpec().getCruiseControl().getConfig().get(CruiseControlConfigurationParameters.METRIC_REPORTER_TOPIC_NAME.getValue()).toString();
            }

            Integer numPartitions = null;
            if (configuration.getConfigOption(CruiseControlConfigurationParameters.METRICS_TOPIC_NUM_PARTITIONS.getValue()) == null) {
                numPartitions = Integer.parseInt(configuration.getConfigOption(KAFKA_NUM_PARTITIONS_CONFIG_FIELD, "1"));
            }

            Integer replicationFactor = null;
            if (configuration.getConfigOption(CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR.getValue()) == null) {
                replicationFactor = Integer.parseInt(configuration.getConfigOption(KAFKA_REPLICATION_FACTOR_CONFIG_FIELD, "1"));
            }

            Integer minInSyncReplicas = null;
            if (configuration.getConfigOption(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR.getValue()) == null) {
                minInSyncReplicas = 1;
            }

            validateCruiseControl(kafka, configuration, numberOfBrokers, replicationFactor, minInSyncReplicas);

            return new CruiseControlMetricsReporter(topicName, numPartitions, replicationFactor, minInSyncReplicas);
        } else {
            // Cruise Control is not enabled
            return null;
        }
    }

    /**
     * Validates the different values of the Cruise Control configuration such as number of replicas for the Metrics Reporter topic etc.
     *
     * @param kafka                 The Kafka custom resource
     * @param configuration         The user-provider configuration of the Kafka cluster
     * @param numberOfBrokers       Number of broker nodes in the Kafka cluster
     * @param replicationFactor     The replication factor of the Metrics Reporter topic
     * @param minInSyncReplicas     The minimal number of in-sync replicas of the Metrics Reporter topic
     */
    private static void validateCruiseControl(Kafka kafka, KafkaConfiguration configuration, long numberOfBrokers, Integer replicationFactor, Integer minInSyncReplicas)  {
        if (numberOfBrokers < 2) {
            throw new InvalidResourceException("Kafka " + kafka.getMetadata().getNamespace() + "/" + kafka.getMetadata().getName() +
                    " has invalid configuration. Cruise Control cannot be deployed with a Kafka cluster which has only one broker. It requires at least two Kafka brokers.");
        }

        if (replicationFactor == null // When it is null, we validate the value from the user configuration. If it is not null, it is either 1 or it was already validated in the KafkaCluster class
                && Integer.parseInt(configuration.getConfigOption(CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR.getValue())) > numberOfBrokers) {
            throw new InvalidResourceException("Kafka " + kafka.getMetadata().getNamespace() + "/" + kafka.getMetadata().getName() +
                    " has invalid configuration. Cruise Control metrics reporter replication factor (" + configuration.getConfigOption(CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR.getValue()) + ") cannot be higher than number of brokers (" + numberOfBrokers + ").");
        }

        if (minInSyncReplicas == null) { // When it is null, we validate the value from the user configuration
            int userConfiguredMinInSync = Integer.parseInt(configuration.getConfigOption(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR.getValue()));
            int configuredCcReplicationFactor = replicationFactor != null ? replicationFactor : Integer.parseInt(configuration.getConfigOption(CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR.getValue()));
            if (userConfiguredMinInSync > configuredCcReplicationFactor) {
                throw new IllegalArgumentException(
                        "The Cruise Control metric topic minISR was set to a value (" + userConfiguredMinInSync + ") " +
                                "which is higher than the number of replicas for that topic (" + configuredCcReplicationFactor + "). " +
                                "Please ensure that the Cruise Control metrics topic minISR is <= to the topic's replication factor."
                );
            }
        }
    }
}