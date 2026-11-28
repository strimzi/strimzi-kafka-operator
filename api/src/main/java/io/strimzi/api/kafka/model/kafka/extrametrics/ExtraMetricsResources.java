/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka.extrametrics;

/**
 * Encapsulates the naming scheme used for the resources which the Cluster Operator manages for a
 * {@code ExtraMetrics} instance.
 */
public class ExtraMetricsResources {
    protected ExtraMetricsResources() { }

    /**
     * Returns the name of the Extra Metrics {@code Deployment}.
     * @param kafkaClusterName  The {@code metadata.name} of the {@code Kafka} resource.
     * @return The name of the corresponding Extra Metrics {@code Deployment}.
     */
    public static String componentName(String kafkaClusterName) {
        return kafkaClusterName + "-extra-metrics";
    }

    /**
     * Returns the name of the Extra Metrics {@code ServiceAccount}.
     * @param kafkaClusterName  The {@code metadata.name} of the {@code Kafka} resource.
     * @return The name of the corresponding Extra Metrics {@code ServiceAccount}.
     */
    public static String serviceAccountName(String kafkaClusterName) {
        return componentName(kafkaClusterName);
    }

    /**
     * Returns the name of the Extra Metrics {@code Secret} for a {@code Kafka} cluster of the given name.
     * This {@code Secret} will only exist if {@code Kafka.spec.extraMetrics} is configured in the
     * {@code Kafka} resource with the given name.
     *
     * @param clusterName  The {@code metadata.name} of the {@code Kafka} resource.
     * @return The name of the corresponding Extra Metrics {@code Secret}.
     */
    public static String secretName(String clusterName) {
        return componentName(clusterName) + "-certs";
    }

    /**
     * Returns the name of the Extra Metrics {@code RoleBinding} which allows Extra Metrics to read the Secrets with the certificates.
     *
     * @param clusterName  The {@code metadata.name} of the {@code Kafka} resource.
     * @return The name of the corresponding Extra Metrics {@code RoleBinding}.
     */
    public static String roleBindingName(String clusterName) {
        return componentName(clusterName) + "-role";
    }

    /**
     * Returns the name of the Extra Metrics logging {@code ConfigMap} for a {@code Kafka} cluster of the given name.
     *
     * @param clusterName  The {@code metadata.name} of the {@code Kafka} resource.
     * @return The name of the corresponding Extra Metrics logging {@code ConfigMap}.
     */
    public static String configMapName(String clusterName) {
        return componentName(clusterName) + "-logging";
    }
}
