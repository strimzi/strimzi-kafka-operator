/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.crd;

import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Environment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KafkaNodePoolTemplates {

    private KafkaNodePoolTemplates() {}

    public static KafkaNodePoolBuilder defaultKafkaNodePool(String namespaceName, String nodePoolName, String kafkaClusterName, int kafkaReplicas) {
        return new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withNamespace(namespaceName)
                .withName(nodePoolName)
                .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, kafkaClusterName))
            .endMetadata()
            .withNewSpec()
                .withReplicas(kafkaReplicas)
            .endSpec();
    }

    public static KafkaNodePoolBuilder kafkaNodePoolWithControllerRole(String namespaceName, String nodePoolName, String kafkaClusterName, int kafkaReplicas) {
        return defaultKafkaNodePool(namespaceName, nodePoolName, kafkaClusterName, kafkaReplicas)
            .editOrNewSpec()
                .addToRoles(ProcessRoles.CONTROLLER)
            .endSpec();
    }

    public static KafkaNodePoolBuilder kafkaNodePoolWithControllerRoleAndPersistentStorage(String namespaceName, String nodePoolName, String kafkaClusterName, int kafkaReplicas) {
        return kafkaNodePoolWithControllerRole(namespaceName, nodePoolName, kafkaClusterName, kafkaReplicas)
            .editOrNewSpec()
                .withNewPersistentClaimStorage()
                    .withSize("1Gi")
                    .withDeleteClaim(true)
                .endPersistentClaimStorage()
            .endSpec();
    }

    public static KafkaNodePoolBuilder kafkaNodePoolWithBrokerRole(String namespaceName, String nodePoolName, String kafkaClusterName, int kafkaReplicas) {
        return defaultKafkaNodePool(namespaceName, nodePoolName, kafkaClusterName, kafkaReplicas)
            .editOrNewSpec()
                .addToRoles(ProcessRoles.BROKER)
            .endSpec();
    }

    public static KafkaNodePoolBuilder kafkaNodePoolWithBrokerRoleAndPersistentStorage(String namespaceName, String nodePoolName, String kafkaClusterName, int kafkaReplicas) {
        return kafkaNodePoolWithBrokerRole(namespaceName, nodePoolName, kafkaClusterName, kafkaReplicas)
            .editOrNewSpec()
                .withNewPersistentClaimStorage()
                    .withSize("1Gi")
                    .withDeleteClaim(true)
                .endPersistentClaimStorage()
            .endSpec();
    }

    /**
     * Creates a KafkaNodePoolBuilder for a Kafka instance (mirroring its mandatory specification) with roles based
     * on the environment setting (TestConstants.USE_KRAFT_MODE) having BROKER role in Zookeeper and Kraft mode alike
     * adding CONTROLLER role as well if Kraft is enabled.
     *
     * @param nodePoolName The name of the node pool.
     * @param kafka The Kafka instance (Model of Kafka).
     * @param kafkaNodePoolReplicas The number of kafka broker replicas for the given node pool.
     * @return KafkaNodePoolBuilder configured with the appropriate (environment variable based) roles based on.
     */
    public static KafkaNodePoolBuilder kafkaBasedNodePoolWithFgBasedRole(String nodePoolName, Kafka kafka, int kafkaNodePoolReplicas) {
        List<ProcessRoles> roles = new ArrayList<>();
        roles.add(ProcessRoles.BROKER);

        if (Environment.isKRaftModeEnabled()) {
            roles.add(ProcessRoles.CONTROLLER);
        }
        return kafkaBasedNodePoolWithRole(nodePoolName, kafka, roles, kafkaNodePoolReplicas);
    }

    /**
     * Creates a KafkaNodePoolBuilder for a Kafka instance (mirroring its mandatory specification) with only the BROKER role.
     *
     * @param nodePoolName The name of the node pool.
     * @param kafka The Kafka instance (Model of Kafka).
     * @param kafkaNodePoolReplicas The number of kafka broker replicas for the given node pool.
     * @return KafkaNodePoolBuilder configured with the BROKER role.
     */
    public static KafkaNodePoolBuilder kafkaBasedNodePoolWithBrokerRole(String nodePoolName, Kafka kafka, int kafkaNodePoolReplicas) {
        return kafkaBasedNodePoolWithRole(nodePoolName, kafka, List.of(ProcessRoles.BROKER), kafkaNodePoolReplicas);
    }

    /**
     * Creates a KafkaNodePoolBuilder for a Kafka instance (mirroring its mandatory specification) with only the CONTROLLER role.
     *
     * @param nodePoolName The name of the node pool.
     * @param kafka The Kafka instance (Model of Kafka).
     * @param kafkaNodePoolReplicas The number of kafka broker replicas for the given node pool.
     * @return KafkaNodePoolBuilder configured with the CONTROLLER role.
     */
    public static KafkaNodePoolBuilder kafkaBasedNodePoolWithControllerRole(String nodePoolName, Kafka kafka, int kafkaNodePoolReplicas) {
        return kafkaBasedNodePoolWithRole(nodePoolName, kafka, List.of(ProcessRoles.CONTROLLER), kafkaNodePoolReplicas);
    }

    /**
     * Creates a KafkaNodePoolBuilder for a Kafka instance (mirroring its mandatory specification) with both BROKER and CONTROLLER roles.
     *
     * @param nodePoolName The name of the node pool.
     * @param kafka The Kafka instance (Model of Kafka).
     * @param kafkaNodePoolReplicas The number of kafka broker replicas for the given node pool.
     * @return KafkaNodePoolBuilder configured with both BROKER and CONTROLLER roles.
     */
    public static KafkaNodePoolBuilder kafkaBasedNodePoolWithDualRole(String nodePoolName, Kafka kafka, int kafkaNodePoolReplicas) {
        return kafkaBasedNodePoolWithRole(nodePoolName, kafka, List.of(ProcessRoles.BROKER, ProcessRoles.CONTROLLER), kafkaNodePoolReplicas);
    }

    /**
     * Private helper method to create a KafkaNodePoolBuilder with specified roles, besides that completely mirroring all
     * necessary (mandatory) specification from Kafka Custom Resource.
     *
     * @param nodePoolName The name of the node pool.
     * @param kafka The Kafka instance (Model of Kafka).
     * @param roles The roles to be assigned to the node pool.
     * @param kafkaNodePoolReplicas The number of kafka broker replicas for the given node pool.
     * @return KafkaNodePoolBuilder configured with the given roles and specifications.
     */
    private static KafkaNodePoolBuilder kafkaBasedNodePoolWithRole(String nodePoolName, Kafka kafka, List<ProcessRoles> roles, int kafkaNodePoolReplicas) {
        return new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName(nodePoolName)
                .withNamespace(kafka.getMetadata().getNamespace())
                .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, kafka.getMetadata().getName()))
            .endMetadata()
            .withNewSpec()
                .withRoles(roles)
                .withReplicas(kafkaNodePoolReplicas)
                .withStorage(kafka.getSpec().getKafka().getStorage())
                .withJvmOptions(kafka.getSpec().getKafka().getJvmOptions())
                .withResources(kafka.getSpec().getKafka().getResources())
            .endSpec();
    }
}
