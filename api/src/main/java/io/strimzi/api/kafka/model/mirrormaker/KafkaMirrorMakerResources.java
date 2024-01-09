/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.mirrormaker;

/**
 * Encapsulates the naming scheme used for the resources which the Cluster Operator manages for a
 * {@code KafkaMirrorMaker} cluster.
 */
public class KafkaMirrorMakerResources {
    protected KafkaMirrorMakerResources() { }

    /**
     * Returns the name of the Kafka MirrorMaker {@code Deployment} for a {@code KafkaMirrorMaker} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaMirrorMaker} resource.
     * @return The name of the corresponding Kafka MirrorMaker {@code Deployment}.
     */
    public static String componentName(String clusterName) {
        return clusterName + "-mirror-maker";
    }

    /**
     * Returns the name of the Kafka MirrorMaker {@code ServiceAccount} for a {@code KafkaMirrorMaker} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaMirrorMaker} resource.
     * @return The name of the corresponding Kafka MirrorMaker {@code ServiceAccount}.
     */
    public static String serviceAccountName(String clusterName) {
        return componentName(clusterName);
    }

    /**
     * Returns the name of the Prometheus {@code Service} for a {@code KafkaMirrorMaker} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaMirrorMaker} resource.
     * @return The name of the corresponding {@code Service}.
     */
    public static String serviceName(String clusterName) {
        return clusterName + "-mirror-maker";
    }

    /**
     * Returns the name of the Kafka MirrorMaker metrics and log {@code ConfigMap} for a {@code KafkaMirrorMaker} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code KafkaMirrorMaker} resource.
     * @return The name of the corresponding Kafka MirrorMaker metrics and log {@code ConfigMap}.
     */
    public static String metricsAndLogConfigMapName(String clusterName) {
        return clusterName + "-mirror-maker-config";
    }
}
