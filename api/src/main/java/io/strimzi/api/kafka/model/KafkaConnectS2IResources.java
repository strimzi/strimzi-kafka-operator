/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

/**
 * Encapsulates the naming scheme used for the resources which the Cluster Operator manages for a
 * {@code KafkaConnectS2I} cluster.
 */
public class KafkaConnectS2IResources extends KafkaConnectResources {
    private KafkaConnectS2IResources() {
        super();
    }

    /**
     * Returns the name of the Kafka Connect S2I {@code BuildConfig} for a {@code KafkaConnectS2I} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaConnectS2I} resource.
     * @return The name of the corresponding Kafka Connect S2I {@code BuildConfig}.
     */
    public static String buildConfigName(String clusterName) {
        return deploymentName(clusterName);
    }

    /**
     * Returns the name of the Kafka Connect S2I {@code ImageStream} for a {@code KafkaConnectS2I} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaConnectS2I} resource.
     * @return The name of the corresponding Kafka Connect S2I {@code ImageStream}.
     */
    public static String targetImageStreamName(String clusterName) {
        return deploymentName(clusterName);
    }

    /**
     * Returns the name of the Kafka Connect S2I {@code ImageStream} for a {@code KafkaConnectS2I} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaConnectS2I} resource.
     * @return The name of the corresponding Kafka Connect S2I {@code ImageStream}.
     */
    public static String sourceImageStreamName(String clusterName) {
        return deploymentName(clusterName) + "-source";
    }
}
