/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

/**
 * Encapsulates the naming scheme used for the resources which the Cluster Operator manages for a
 * {@code KafkaExporter} cluster.
 */
public class KafkaExporterResources {
    protected KafkaExporterResources() { }

    /**
     * Returns the name of the Kafka Exporter {@code Deployment} for a {@code KafkaExporter} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaExporter} resource.
     * @return The name of the corresponding Kafka Exporter {@code Deployment}.
     */
    public static String deploymentName(String clusterName) {
        return clusterName + "-kafka-exporter";
    }

    /**
     * Returns the name of the Kafka Exporter {@code ServiceAccount} for a {@code KafkaExporter} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaExporter} resource.
     * @return The name of the corresponding Kafka Exporter {@code ServiceAccount}.
     */
    public static String serviceAccountName(String clusterName) {
        return deploymentName(clusterName);
    }

    /**
     * Returns the name of the Prometheus {@code Service} for a {@code KafkaExporter} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaExporter} resource.
     * @return The name of the corresponding {@code Service}.
     */
    public static String serviceName(String clusterName) {
        return clusterName + "-kafka-exporter";
    }
}
