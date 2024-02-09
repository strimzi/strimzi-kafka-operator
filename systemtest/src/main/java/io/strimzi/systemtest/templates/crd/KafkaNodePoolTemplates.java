/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.crd;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.kafka.EphemeralStorage;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Environment;

import java.util.List;
import java.util.Map;

public class KafkaNodePoolTemplates {

    private KafkaNodePoolTemplates() {}

    public static KafkaNodePoolBuilder defaultKafkaNodePool(String namespaceName, String nodePoolName, String kafkaClusterName, int kafkaReplicas, List<ProcessRoles> processRoles) {
        KafkaNodePoolBuilder kafkaNodePoolBuilder = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withNamespace(namespaceName)
                .withName(nodePoolName)
                .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, kafkaClusterName))
            .endMetadata()
            .withNewSpec()
                .withReplicas(kafkaReplicas)
                .withStorage(new EphemeralStorage())
                .withRoles(processRoles)
            .endSpec();

        if (!Environment.isSharedMemory()) {
            if (processRoles.size() == 1 && processRoles.get(0).equals(ProcessRoles.CONTROLLER)) {
                kafkaNodePoolBuilder
                    .editSpec()
                        // For controllers using 512Mi is too much and on the other hand 128Mi is causing OOM problem at the start.
                        .withResources(new ResourceRequirementsBuilder()
                            .addToLimits("memory", new Quantity("256Mi"))
                            .addToRequests("memory", new Quantity("256Mi"))
                            .build())
                    .endSpec();
            } else {
                kafkaNodePoolBuilder
                    .editSpec()
                        // we use such values, because on environments where it is limited to 7Gi, we are unable to deploy
                        // Cluster Operator, two Kafka clusters and MirrorMaker/2. Such situation may result in an OOM problem.
                        // For Kafka using 784Mi is too much and on the other hand 256Mi is causing OOM problem at the start.
                        .withResources(new ResourceRequirementsBuilder()
                            .addToLimits("memory", new Quantity("512Mi"))
                            .addToRequests("memory", new Quantity("512Mi"))
                            .build())
                    .endSpec();
            }
        }

        return kafkaNodePoolBuilder;
    }

    public static KafkaNodePoolBuilder controllerPool(String namespaceName, String nodePoolName, String kafkaClusterName, int kafkaReplicas) {
        return defaultKafkaNodePool(namespaceName, nodePoolName, kafkaClusterName, kafkaReplicas, List.of(ProcessRoles.CONTROLLER));
    }

    public static KafkaNodePoolBuilder controllerPoolPersistentStorage(String namespaceName, String nodePoolName, String kafkaClusterName, int kafkaReplicas) {
        return controllerPool(namespaceName, nodePoolName, kafkaClusterName, kafkaReplicas)
            .editOrNewSpec()
                .withNewPersistentClaimStorage()
                    .withSize("1Gi")
                    .withDeleteClaim(true)
                .endPersistentClaimStorage()
            .endSpec();
    }

    public static KafkaNodePoolBuilder brokerPool(String namespaceName, String nodePoolName, String kafkaClusterName, int kafkaReplicas) {
        return defaultKafkaNodePool(namespaceName, nodePoolName, kafkaClusterName, kafkaReplicas, List.of(ProcessRoles.BROKER));
    }

    public static KafkaNodePoolBuilder brokerPoolPersistentStorage(String namespaceName, String nodePoolName, String kafkaClusterName, int kafkaReplicas) {
        return brokerPool(namespaceName, nodePoolName, kafkaClusterName, kafkaReplicas)
            .editOrNewSpec()
                .withNewPersistentClaimStorage()
                    .withSize("1Gi")
                    .withDeleteClaim(true)
                .endPersistentClaimStorage()
            .endSpec();
    }

    public static KafkaNodePoolBuilder mixedPool(String namespaceName, String nodePoolName, String kafkaClusterName, int kafkaReplicas) {
        return defaultKafkaNodePool(namespaceName, nodePoolName, kafkaClusterName, kafkaReplicas, List.of(ProcessRoles.BROKER, ProcessRoles.CONTROLLER));
    }

    public static KafkaNodePoolBuilder mixedPoolPersistentStorage(String namespaceName, String nodePoolName, String kafkaClusterName, int kafkaReplicas) {
        return mixedPool(namespaceName, nodePoolName, kafkaClusterName, kafkaReplicas)
            .editOrNewSpec()
                .withNewPersistentClaimStorage()
                    .withSize("1Gi")
                    .withDeleteClaim(true)
                .endPersistentClaimStorage()
            .endSpec();
    }
}
