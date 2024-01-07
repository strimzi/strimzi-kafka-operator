/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.jmxtrans;

/**
 * Encapsulates the naming scheme used for the resources which the Cluster Operator manages for a
 * {@code JmxTrans} instance.
 */
public class JmxTransResources {
    protected JmxTransResources() { }
    /**
     * Returns the name of the JmxTrans {@code Deployment}.
     * @param kafkaClusterName  The {@code metadata.name} of the {@code Kafka} resource.
     * @return The name of the corresponding JmxTrans {@code Deployment}.
     */
    public static String componentName(String kafkaClusterName) {
        return kafkaClusterName + "-kafka-jmx-trans";
    }

    /**
     * Returns the name of the JmxTrans {@code ServiceAccount}.
     * @param kafkaClusterName  The {@code metadata.name} of the {@code Kafka} resource.
     * @return The name of the corresponding JmxTrans {@code ServiceAccount}.
     */
    public static String serviceAccountName(String kafkaClusterName) {
        return componentName(kafkaClusterName);
    }

    /**
     * Returns the name of the JmxTrans {@code ConfigMap}.
     *
     * @param kafkaClusterName  The {@code metadata.name} of the {@code Kafka} resource.
     *
     * @return The name of the corresponding JmxTrans {@code ConfigMap}.
     */
    public static String configMapName(String kafkaClusterName) {
        return kafkaClusterName + "-jmxtrans-config";
    }
}