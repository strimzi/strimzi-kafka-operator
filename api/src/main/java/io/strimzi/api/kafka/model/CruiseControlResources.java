/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

/**
 * Encapsulates the naming scheme used for the resources which the Cluster Operator manages for a
 * {@code CruiseControl} cluster.
 */
public class CruiseControlResources {

    /**
     * Returns the name of the Cruise Control {@code Deployment} for a {@code Kafka} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code Kafka} resource.
     * @return The name of the corresponding Cruise Control {@code Deployment}.
     */
    public static String deploymentName(String clusterName) {
        return clusterName + "-cruise-control";
    }

    /**
     * Returns the name of the Cruise Control {@code ServiceAccount} for a {@code Kafka} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code Kafka} resource.
     * @return The name of the corresponding Cruise Control {@code ServiceAccount}.
     */
    public static String serviceAccountName(String clusterName) {
        return deploymentName(clusterName);
    }

    /**
     * Returns the name of the Cruise Control {@code Service} for a {@code Kafka} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code Kafka} resource.
     * @return The name of the corresponding Cruise Control {@code Service}.
     */
    public static String serviceName(String clusterName) {
        return deploymentName(clusterName);
    }

    /**
     * Returns qualified name of the service which works across different namespaces.
     *
     * @param clusterName   The {@code metadata.name} of the {@code Kafka} resource.
     * @param namespace     Namespace of the Cruise Control deployment
     * @return              qualified namespace in the format "&lt;service-name&gt;.&lt;namespace&gt;.svc"
     */
    public static String qualifiedServiceName(String clusterName, String namespace) {
        return serviceName(clusterName) + "." + namespace + ".svc";
    }

    /**
     * Returns the name of the Cruise Control {@code Secret} for a {@code Kafka} cluster of the given name.
     * This {@code Secret} will only exist if {@code Kafka.spec.cruiseControl} is configured in the
     * {@code Kafka} resource with the given name.
     *
     * @param clusterName  The {@code metadata.name} of the {@code Kafka} resource.
     * @return The name of the corresponding Cruise Control {@code Secret}.
     */
    public static String secretName(String clusterName) {
        return deploymentName(clusterName) + "-certs";
    }

    /**
     * Returns the name of the Cruise Control log {@code ConfigMap} for a {@code Kafka} cluster of the given name.
     * @param clusterName  The {@code metadata.name} of the {@code Kafka} resource.
     * @return The name of the corresponding Cruise Control log {@code ConfigMap}.
     */
    public static String logAndMetricsConfigMapName(String clusterName) {
        return clusterName + "-cruise-control-config";
    }
}
