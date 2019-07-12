/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

/**
 * Encapsulates the naming scheme used for the resources which the Cluster Operator manages for a
 * {@code KafkaBridge} cluster.
 */
public class KafkaBridgeResources {
    private KafkaBridgeResources() { }

    /**
     * Returns the name of the Kafka Bridge {@code Deployment} for a {@code KafkaBridge} cluster of the given name.
     * This {@code Deployment} will only exist if {@code KafkaBridge} is deployed by Cluster Operator..
     * @param clusterName  The {@code metadata.name} of the {@code KafkaBridge} resource.
     * @return The name of the corresponding Kafka Bridge {@code Deployment}.
     */
    public static String deploymentName(String clusterName) {
        return clusterName + "-bridge";
    }

    /**
     * Returns the name of the HTTP REST {@code Service} for a {@code KafkaBridge} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaBridge} resource.
     * @return The name of the corresponding bootstrap {@code Service}.
     */
    public static String serviceName(String clusterName) {
        return clusterName + "-bridge-service";
    }

    /**
     * Returns the name of the Kafka Bridge metrics and log {@code ConfigMap} for a {@code KafkaBrdige} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaBrdige} resource.
     * @return The name of the corresponding Kafka Bridge metrics and log {@code ConfigMap}.
     */
    public static String metricsAndLogConfigMapName(String clusterName) {
        return clusterName + "-bridge-config";
    }

    /**
     * Returns the name of the Kafka Bridge {@code ServiceAccount} for a {@code KafkaBridge} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaBridge} resource.
     * @return The name of the corresponding Kafka Bridge {@code ServiceAccount}.
     */
    public static String serviceAccountName(String clusterName) {
        return deploymentName(clusterName);
    }

}
