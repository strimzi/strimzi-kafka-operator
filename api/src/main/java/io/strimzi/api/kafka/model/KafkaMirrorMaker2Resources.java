/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

/**
 * Encapsulates the naming scheme used for the resources which the Cluster Operator manages for a
 * {@code KafkaMirrorMaker2} cluster.
 */
public class KafkaMirrorMaker2Resources {
    
    /**
     * Returns the name of the Kafka Mirror Maker 2 {@code Deployment} for a {@code KafkaMirrorMaker2} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaMirrorMaker2} resource.
     * @return The name of the corresponding Kafka Mirror Maker 2 {@code Deployment}.
     */
    public static String deploymentName(String clusterName) {
        return clusterName + "-mirrormaker2";
    }

    /**
     * Returns the name of the Kafka Mirror Maker 2 {@code ServiceAccount} for a {@code KafkaMirrorMaker2} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaMirrorMaker2} resource.
     * @return The name of the corresponding Kafka Mirror Maker 2 {@code ServiceAccount}.
     */
    public static String serviceAccountName(String clusterName) {
        return deploymentName(clusterName);
    }

    /**
     * Returns the name of the HTTP REST {@code Service} for a {@code KafkaMirrorMaker2} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaMirrorMaker2} resource.
     * @return The name of the corresponding {@code Service}.
     */
    public static String serviceName(String clusterName) {
        return clusterName + "-mirrormaker2-api";
    }

    /**
     * Returns the name of the Kafka Mirror Maker 2 metrics and log {@code ConfigMap} for a {@code KafkaMirrorMaker2} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaMirrorMaker2} resource.
     * @return The name of the corresponding KafkaMirrorMaker2 metrics and log {@code ConfigMap}.
     */
    public static String metricsAndLogConfigMapName(String clusterName) {
        return clusterName + "-mirrormaker2-config";
    }

    /**
     * Returns the URL of the Kafka Mirror Maker 2 REST API for a {@code KafkaMirrorMaker2} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaMirrorMaker2} resource.
     * @param namespace The namespace where {@code KafkaMirrorMaker2} cluster is running.
     * @param port The port on which the {@code KafkaMirrorMaker2} API is available.
     * @return The base URL of the {@code KafkaMirrorMaker2} REST API.
     */
    public static String url(String clusterName, String namespace, int port) {
        return "http://" + serviceName(clusterName) + "." + namespace + ".svc:" + port;
    }
}
