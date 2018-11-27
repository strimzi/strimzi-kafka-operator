/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

/**
 * Encapsulates the naming scheme used for the resources which the Cluster Operator manages for a
 * {@code KafkaConnect} cluster.
 */
public class KafkaConnectResources {

    private KafkaConnectResources() { }

    /**
     * Returns the name of the Kafka Connect {@code Deployment} for a {@code KafkaConnect} cluster of the given name.
     * @param connectName  The {@code metadata.name} of the {@code KafkaConnect} resource.
     * @return The name of the corresponding Kafka Connect {@code Deployment}.
     */
    public static String kafkaConnectDeploymentName(String connectName) {
        return connectName + "-connect";
    }

    /**
     * Returns the name of the Kafka Connect REST API {@code Service} for a {@code KafkaConnect} cluster of the given name.
     * @param connectName  The {@code metadata.name} of the {@code KafkaConnect} resource.
     * @return The name of the corresponding Kafka Connect REST API {@code Service}.
     */
    public static String kafkaConnectRestApiServiceName(String connectName) {
        return connectName + "-connect-api";
    }

    public static String kafkaConnectRestApiAddress(String connectName) {
        return kafkaConnectRestApiServiceName(connectName) + ":8083";
    }

    /**
     * Returns the name of the Kafka Connect metrics and log {@code ConfigMap} for a {@code KafkaConnect} cluster of the given name.
     * @param connectName  The {@code metadata.name} of the {@code KafkaConnect} resource.
     * @return The name of the corresponding Kafka Connect metrics and log {@code ConfigMap}.
     */
    public static String kafkaConnectMetricsAndLogConfigMapName(String connectName) {
        return connectName + "-connect-config";
    }
}
