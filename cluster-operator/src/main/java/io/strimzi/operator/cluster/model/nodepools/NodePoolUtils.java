/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.nodepools;

import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.cluster.model.KafkaPool;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.cluster.operator.resource.SharedEnvironmentProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Class which contains different utility methods for working with Node Pools
 */
public class NodePoolUtils {
    /**
     * Generates KafkaPool instances from Kafka and KafkaNodePool resources.
     *
     * @param reconciliation    Reconciliation marker
     * @param kafka             Kafka custom resource
     * @param nodePools         List of node pools belonging to this cluster
     * @param oldStorage        Maps with old storage configurations, where the key is the name of the controller
     *                          resource (e.g. my-cluster-pool-a) and the value is the current storage configuration
     * @param currentPods       Map with current pods, where the key is the name of the controller resource
     *                          (e.g. my-cluster-pool-a) and the value is a list with Pod names
     * @param useKRaft          Flag indicating if KRaft is enabled
     * @param sharedEnvironmentProvider Shared environment provider
     *
     * @return  List of KafkaPool instances belonging to given Kafka cluster
     */
    public static List<KafkaPool> createKafkaPools(
            Reconciliation reconciliation,
            Kafka kafka,
            List<KafkaNodePool> nodePools,
            Map<String, Storage> oldStorage,
            Map<String, List<String>> currentPods,
            boolean useKRaft,
            SharedEnvironmentProvider sharedEnvironmentProvider
    )    {
        // We create the Kafka pool resources
        List<KafkaPool> pools = new ArrayList<>();

        if (nodePools == null)   {
            // Node pools are not used => we create the default virtual node pool

            // Name of the controller resource for the virtual node pool
            String virtualNodePoolComponentName = kafka.getMetadata().getName() + "-" + VirtualNodePoolConverter.DEFAULT_NODE_POOL_NAME;

            int currentReplicas = 0;
            if (currentPods.get(virtualNodePoolComponentName) != null) {
                // We are converting from regular Kafka resource which is not using node pools. So the pods will be numbered
                // continuously from 0. So we can use this to create the list of currently used Node IDs.
                currentReplicas = currentPods.get(virtualNodePoolComponentName).size();
            }

            // We create the virtual KafkaNodePool custom resource
            KafkaNodePool virtualNodePool = VirtualNodePoolConverter.convertKafkaToVirtualNodePool(kafka, currentReplicas, useKRaft);

            // We prepare ID Assignment
            NodeIdAssignor assignor = new NodeIdAssignor(List.of(virtualNodePool));

            pools.add(
                    KafkaPool.fromCrd(
                            reconciliation,
                            kafka,
                            virtualNodePool,
                            assignor.assignmentForPool(virtualNodePool.getMetadata().getName()),
                            oldStorage.get(virtualNodePoolComponentName),
                            ModelUtils.createOwnerReference(kafka, false),
                            sharedEnvironmentProvider
                    )
            );
        } else {
            validateNodePools(kafka, nodePools, useKRaft);

            // We prepare ID Assignment
            NodeIdAssignor assignor = new NodeIdAssignor(nodePools);

            // We create the Kafka pool resources
            for (KafkaNodePool nodePool : nodePools) {
                pools.add(
                        KafkaPool.fromCrd(
                                reconciliation,
                                kafka,
                                nodePool,
                                assignor.assignmentForPool(nodePool.getMetadata().getName()),
                                oldStorage.get(KafkaPool.componentName(kafka, nodePool)),
                                ModelUtils.createOwnerReference(nodePool, false),
                                sharedEnvironmentProvider
                        )
                );
            }
        }

        return pools;
    }

    /**
     * Validates KafkaNodePools
     *
     * @param kafka         The Kafka custom resource
     * @param nodePools     The list with KafkaNodePool resources
     * @param useKRaft      Flag indicating whether KRaft is enabled or not
     */
    public static void validateNodePools(Kafka kafka, List<KafkaNodePool> nodePools, boolean useKRaft)    {
        // If there are no node pools, the rest of the validation makes no sense, so we throw an exception right away
        if (nodePools.isEmpty()
                || nodePools.stream().noneMatch(np -> np.getSpec().getReplicas() > 0))    {
            throw new InvalidResourceException("KafkaNodePools are enabled, but the KafkaNodePool for Kafka cluster " + kafka.getMetadata().getName() + " either don't exists or have 0 replicas. " +
                    "Please make sure at least one KafkaNodePool resource exists, is in the same namespace as the Kafka resource, has at least one replica, and has the strimzi.io/cluster label set to the name of the Kafka resource.");
        } else {
            List<String> errors = new ArrayList<>();

            if (useKRaft) {
                errors.addAll(validateKRaftProcessRoles(nodePools));
            } else {
                errors.addAll(validateZooKeeperBasedProcessRoles(nodePools));
            }

            // Throw an exception if there are any errors
            if (!errors.isEmpty()) {
                throw new InvalidResourceException("Tha Kafka cluster " + kafka.getMetadata().getName() + " is invalid: " + errors);
            }
        }
    }

    /**
     * ZooKeeper based cluster needs to have only the broker role. This method checks if this condition is fulfilled.
     *
     * @param nodePools     Node pools
     *
     * @return  List with errors found during the validation
     */
    private static List<String> validateZooKeeperBasedProcessRoles(List<KafkaNodePool> nodePools)    {
        List<String> errors = new ArrayList<>();

        for (KafkaNodePool pool : nodePools)    {
            if (pool.getSpec().getRoles() == null || pool.getSpec().getRoles().isEmpty())   {
                // Pools need to have at least one role
                errors.add("KafkaNodePool " + pool.getMetadata().getName() + " has no role defined in .spec.roles");
            } else if (pool.getSpec().getRoles().contains(ProcessRoles.CONTROLLER))   {
                // ZooKeeper based cluster allows only the broker tole
                errors.add("KafkaNodePool " + pool.getMetadata().getName() + " contains invalid roles configuration. " +
                        "In a ZooKeeper-based Kafka cluster, the KafkaNodePool role has to be always set only to the 'broker' role.");
            }
        }

        return errors;
    }

    /**
     * KRaft cluster needs to have at least one broker and one controller (could be both in the same node). This method
     * checks if this condition is fulfilled.
     *
     * @param nodePools     Node pools
     *
     * @return  List with errors found during the validation
     */
    private static List<String> validateKRaftProcessRoles(List<KafkaNodePool> nodePools)    {
        List<String> errors = new ArrayList<>();

        boolean hasBroker = false;
        boolean hasController = false;

        for (KafkaNodePool pool : nodePools)    {
            if (pool.getSpec().getRoles() == null || pool.getSpec().getRoles().isEmpty())   {
                // Pools need to have at least one role
                errors.add("KafkaNodePool " + pool.getMetadata().getName() + " has no role defined in .spec.roles");
            } else if (pool.getSpec().getReplicas() > 0)   {
                // Has at least one replica => pools without replicas do not count

                if (pool.getSpec().getRoles().contains(ProcessRoles.BROKER))   {
                    hasBroker = true;
                }

                if (pool.getSpec().getRoles().contains(ProcessRoles.CONTROLLER))   {
                    hasController = true;
                }
            }
        }

        if (!hasBroker) {
            errors.add("At least one KafkaNodePool with the broker role and at least one replica is required when KRaft mode is enabled");
        }

        if (!hasController) {
            errors.add("At least one KafkaNodePool with the controller role and at least one replica is required when KRaft mode is enabled");
        }

        return errors;
    }
}
