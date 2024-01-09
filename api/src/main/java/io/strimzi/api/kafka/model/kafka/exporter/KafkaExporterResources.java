/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.exporter;

/**
 * Encapsulates the naming scheme used for the resources which the Cluster Operator manages for a
 * {@code KafkaExporter} instance.
 */
public class KafkaExporterResources {
    protected KafkaExporterResources() { }

    /**
     * Returns the name of the Kafka Exporter {@code Deployment}.
     * @param kafkaClusterName  The {@code metadata.name} of the {@code Kafka} resource.
     * @return The name of the corresponding Kafka Exporter {@code Deployment}.
     */
    public static String componentName(String kafkaClusterName) {
        return kafkaClusterName + "-kafka-exporter";
    }

    /**
     * Returns the name of the Kafka Exporter {@code ServiceAccount}.
     * @param kafkaClusterName  The {@code metadata.name} of the {@code Kafka} resource.
     * @return The name of the corresponding Kafka Exporter {@code ServiceAccount}.
     */
    public static String serviceAccountName(String kafkaClusterName) {
        return componentName(kafkaClusterName);
    }

    /**
     * Returns the name of the Prometheus {@code Service}.
     * @param kafkaClusterName  The {@code metadata.name} of the {@code Kafka} resource.
     * @return The name of the corresponding {@code Service}.
     */
    public static String serviceName(String kafkaClusterName) {
        return kafkaClusterName + "-kafka-exporter";
    }

    /**
     * Returns the name of the Kafka Exporter {@code Secret} for a {@code Kafka} cluster of the given name.
     * This {@code Secret} will only exist if {@code Kafka.spec.kafkaExporter} is configured in the
     * {@code Kafka} resource with the given name.
     *
     * @param clusterName  The {@code metadata.name} of the {@code Kafka} resource.
     * @return The name of the corresponding Kafka Exporter {@code Secret}.
     */
    public static String secretName(String clusterName) {
        return componentName(clusterName) + "-certs";
    }
}
