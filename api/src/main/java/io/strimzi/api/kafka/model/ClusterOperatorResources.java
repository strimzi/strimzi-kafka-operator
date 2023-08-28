/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

/**
 * Encapsulates the naming scheme used for resources related to the Cluster Operator.
 */
public class ClusterOperatorResources {
    private ClusterOperatorResources() { }

    /**
     * Returns the name of the Cluster Operator Deployment.
     * @return The name of the Cluster Operator Deployment.
     */
    public static String deploymentName() {
        return "strimzi-cluster-operator";
    }

    /**
     * Returns the name of the Cluster Operator ConfigMap.
     * @return The name of the Cluster Operator ConfigMap.
     */
    public static String metricsAndLogConfigMapName() {
        return deploymentName();
    }

    /**
     * Returns the name of the Cluster Operator ClusterRoleBinding.
     * @return The name of the Cluster Operator ClusterRoleBinding.
     */
    public static String clusterRoleBindingName() {
        return deploymentName();
    }

    /**
     * Returns the name of the Cluster Operator broker delegation ClusterRoleBinding.
     * @return The name of the Cluster Operator broker delegation ClusterRoleBinding.
     */
    public static String brokerDelegationClusterRoleBindingName() {
        return deploymentName() + "-kafka-broker-delegation";
    }

    /**
     * Returns the name of the Cluster Operator client delegation ClusterRoleBinding.
     * @return The name of the Cluster Operator client delegation ClusterRoleBinding.
     */
    public static String clientDelegationClusterRoleBindingName() {
        return deploymentName() + "-kafka-client-delegation";
    }

    /**
     * Returns the name of the Cluster Operator global ClusterRole.
     * @return The name of the Cluster Operator global ClusterRole.
     */
    public static String globalClusterRoleName() {
        return deploymentName() + "-global";
    }

    /**
     * Returns the name of the Cluster Operator leader election ClusterRole.
     * @return The name of the Cluster Operator leader election ClusterRole.
     */
    public static String leaderElectionClusterRoleName() {
        return deploymentName() + "-leader-election";
    }

    /**
     * Returns the name of the Cluster Operator namespaced ClusterRole.
     * @return The name of the Cluster Operator namespaced ClusterRole.
     */
    public static String namespacedClusterRoleName() {
        return deploymentName() + "-namespaced";
    }

    /**
     * Returns the name of the Cluster Operator watched ClusterRole.
     * @return The name of the Cluster Operator watched ClusterRole.
     */
    public static String watchedClusterRoleName() {
        return deploymentName() + "-watched";
    }

    /**
     * Returns the name of the Kafka client ClusterRole.
     * @return The name of the Kafka client ClusterRole.
     */
    public static String kafkaClientClusterRoleName() {
        return "strimzi-kafka-client";
    }

    /**
     * Returns the name of the Kafka broker ClusterRole.
     * @return The name of the Kafka broker ClusterRole.
     */
    public static String kafkaBrokerClusterRoleName() {
        return "strimzi-kafka-broker";
    }

    /**
     * Returns the name of the Kafka CustomResourceDefinition.
     * @param name The spec.names.plural of the Kafka CustomResourceDefinition resource.
     * @return The metadata.name of the corresponding Kafka CustomResourceDefinition.
     */
    public static String kafkaCustomResourceDefinitionName(String name) {
        return name + ".kafka.strimzi.io";
    }

    /**
     * Returns the name of the Core CustomResourceDefinition.
     * @param name The spec.names.plural of the Core CustomResourceDefinition resource.
     * @return The metadata.name of the corresponding Core CustomResourceDefinition.
     */
    public static String coreCustomResourceDefinitionName(String name) {
        return name + ".core.strimzi.io";
    }
}
