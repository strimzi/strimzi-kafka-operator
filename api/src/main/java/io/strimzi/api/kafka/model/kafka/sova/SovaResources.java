/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.sova;

/**
 * Encapsulates the naming scheme used for the resources which the Cluster Operator manages for a
 * {@code Sova} instance.
 */
public class SovaResources {
    protected SovaResources() { }

    /**
     * Returns the name of the Sova {@code Deployment}.
     * @param kafkaClusterName  The {@code metadata.name} of the {@code Kafka} resource.
     * @return The name of the corresponding Sova {@code Deployment}.
     */
    public static String componentName(String kafkaClusterName) {
        return kafkaClusterName + "-sova";
    }

    /**
     * Returns the name of the Sova {@code ServiceAccount}.
     * @param kafkaClusterName  The {@code metadata.name} of the {@code Kafka} resource.
     * @return The name of the corresponding Sova {@code ServiceAccount}.
     */
    public static String serviceAccountName(String kafkaClusterName) {
        return componentName(kafkaClusterName);
    }

    /**
     * Returns the name of the Sova {@code Secret} for a {@code Kafka} cluster of the given name.
     * This {@code Secret} will only exist if {@code Kafka.spec.extraMetrics} is configured in the
     * {@code Kafka} resource with the given name.
     *
     * @param clusterName  The {@code metadata.name} of the {@code Kafka} resource.
     * @return The name of the corresponding Sova {@code Secret}.
     */
    public static String secretName(String clusterName) {
        return componentName(clusterName) + "-certs";
    }

    /**
     * Returns the name of the Sova {@code RoleBinding} which allows Sova to read the Secrets with the certificates.
     *
     * @param clusterName  The {@code metadata.name} of the {@code Kafka} resource.
     * @return The name of the corresponding Sova {@code RoleBinding}.
     */
    public static String roleBindingName(String clusterName) {
        return componentName(clusterName) + "-role";
    }

    /**
     * Returns the name of the Sova logging {@code ConfigMap} for a {@code Kafka} cluster of the given name.
     *
     * @param clusterName  The {@code metadata.name} of the {@code Kafka} resource.
     * @return The name of the corresponding Sova logging {@code ConfigMap}.
     */
    public static String configMapName(String clusterName) {
        return componentName(clusterName) + "-logging";
    }
}
