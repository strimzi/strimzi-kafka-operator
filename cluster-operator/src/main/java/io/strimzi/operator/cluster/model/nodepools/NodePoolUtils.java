/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.nodepools;

import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.cluster.model.KafkaPool;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.operator.resource.SharedEnvironmentProvider;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import org.apache.kafka.common.Uuid;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Class which contains different utility methods for working with Node Pools
 */
public class NodePoolUtils {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(NodePoolUtils.class.getName());

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
            KafkaNodePool virtualNodePool = VirtualNodePoolConverter.convertKafkaToVirtualNodePool(kafka, currentReplicas);

            // We prepare ID Assignment
            NodeIdAssignor assignor = new NodeIdAssignor(reconciliation, List.of(virtualNodePool));

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
            validateNodePools(reconciliation, kafka, nodePools, useKRaft);

            // We prepare ID Assignment
            NodeIdAssignor assignor = new NodeIdAssignor(reconciliation, nodePools);

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
     * @param reconciliation    Reconciliation marker
     * @param kafka             The Kafka custom resource
     * @param nodePools         The list with KafkaNodePool resources
     * @param useKRaft          Flag indicating whether KRaft is enabled or not
     */
    public static void validateNodePools(Reconciliation reconciliation, Kafka kafka, List<KafkaNodePool> nodePools, boolean useKRaft)    {
        // If there are no node pools, the rest of the validation makes no sense, so we throw an exception right away
        if (nodePools.isEmpty()
                || nodePools.stream().noneMatch(np -> np.getSpec().getReplicas() > 0))    {
            throw new InvalidResourceException("KafkaNodePools are enabled, but the KafkaNodePool for Kafka cluster " + kafka.getMetadata().getName() + " either don't exists or have 0 replicas. " +
                    "Please make sure at least one KafkaNodePool resource exists, is in the same namespace as the Kafka resource, has at least one replica, and has the strimzi.io/cluster label set to the name of the Kafka resource.");
        } else {
            List<String> errors = new ArrayList<>();

            if (useKRaft) {
                // Validate process roles
                errors.addAll(validateKRaftProcessRoles(nodePools));

                // Validate JBOD storage
                errors.addAll(validateKRaftJbodStorage(nodePools));

                // Validate cluster IDs
                errors.addAll(validateKRaftClusterIds(reconciliation, kafka, nodePools));
            } else {
                // Validate process roles
                errors.addAll(validateZooKeeperBasedProcessRoles(nodePools));

                // Validate cluster IDs
                errors.addAll(validateZooKeeperBasedClusterIds(reconciliation, kafka, nodePools));
            }

            validateNodeIdRanges(reconciliation, nodePools);

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


    /**
     * In KRaft mode, JBOD storage with multiple disks is currently not supported. This method validates the
     * KafkaNodePool resources whether they contain JBOD storage with multiple disks or not.
     *
     * @param nodePools   List of Kafka Node Pools
     *
     * @return  List with errors found during the validation
     */
    public static List<String> validateKRaftJbodStorage(List<KafkaNodePool> nodePools)   {
        List<String> errors = new ArrayList<>();

        for (KafkaNodePool pool : nodePools)    {
            if (pool.getSpec().getStorage() != null
                    && pool.getSpec().getStorage() instanceof JbodStorage jbod) {
                if (jbod.getVolumes().size() > 1) {
                    errors.add("Using more than one disk in a JBOD storage is currently not supported when the UseKRaft feature gate is enabled (in KafkaNodePool " + pool.getMetadata().getName() + ")");
                }
            }
        }

        return errors;
    }

    /**
     * Extracts the cluster IDs found in the Kafka and KafkaNodePool custom resources and validates whether the
     * resources do not belong to different Kafka cluster. This method is used for ZooKeeper based Kafka clusters. In
     * ZooKeeper based clusters, Kafka generates the cluster ID on its own. So the Node Pools cannot have a cluster ID
     * before the Kafka cluster.
     *
     * @param reconciliation    Reconciliation marker
     * @param kafka             The Kafka custom resource which should be validated
     * @param pools             The list of KafkaNodePool custom resources which should be validated
     *
     * @return  List with errors found while checking the cluster IDs or null if no issues found
     */
    public static List<String> validateZooKeeperBasedClusterIds(Reconciliation reconciliation, Kafka kafka, List<KafkaNodePool> pools)   {
        String kafkaCrClusterId = null; // Stores the cluster ID found in the Kafka CR
        boolean hasClusterIdTooEarly = false; // indicates if some node pool has a cluster ID before the Kafka CR
        boolean hasConflict = false; // indicates if multiple different cluster IDs have been found

        // Try to extract the cluster ID from the Kafka CR status first
        if (kafka.getStatus() != null && kafka.getStatus().getClusterId() != null) {
            kafkaCrClusterId = kafka.getStatus().getClusterId();
        }

        // Go through the pools, try to extract the Cluster ID and validate it against already found
        // For ZooKeeper based clusters, the Node Pools are allowed to have:
        //     * No cluster ID regardless whether the Kafka CR has some cluster ID or not (e.g. with new node pools)
        //     * The same cluster ID as the Kafka CR if the Kafka CR has a cluster ID already
        //
        // But they are not allowed to have any cluster ID when the Kafka CR does not have one yet.
        for (KafkaNodePool pool : pools)   {
            if (pool.getStatus() != null && pool.getStatus().getClusterId() != null)   {
                if (kafkaCrClusterId == null)   {
                    hasClusterIdTooEarly = true;
                    LOGGER.warnCr(reconciliation, "Cluster ID {} found in KafkaNodePool {} is not allowed without the same cluster ID being present in the Kafka custom resource", pool.getStatus().getClusterId(), pool.getMetadata().getName());
                } else if (!kafkaCrClusterId.equals(pool.getStatus().getClusterId())) {
                    hasConflict = true;
                    LOGGER.warnCr(reconciliation, "Cluster ID {} found in KafkaNodePool {} is not the same as the cluster ID {} found in the Kafka custom resource", pool.getStatus().getClusterId(), pool.getMetadata().getName(), kafkaCrClusterId);
                }
            }
        }

        if (hasConflict) {
            return List.of("The Kafka custom resource and its KafkaNodePool resources use different cluster IDs.");
        } else if (hasClusterIdTooEarly)   {
            return List.of("The KafkaNodePool resources should not have cluster ID set before the Kafka custom resource.");
        } else {
            return List.of();
        }
    }

    /**
     * Validates the strimzi.io/next-node-ids and strimzi.io/remove-node-ids annotations on the KafkaNodePool resources
     * and if they are invalid, a warning is logged. Invalid annotations do not break reconciliation and do not throw
     * exception. If a scaling event happens and the corresponding annotation is invalid, the operator will ignore it
     * and just pick up the next suitable node ID. But validating the annotation here help to give user a warning about
     * the annotation(s) being invalid earlier - before some scaling happens.
     *
     * @param reconciliation    Reconciliation marker
     * @param pools             List of node pool where the node ID range annotations should be validated
     */
    public static void validateNodeIdRanges(Reconciliation reconciliation, List<KafkaNodePool> pools)   {
        pools.forEach(pool -> {
            String nextNodeIds = Annotations.stringAnnotation(pool, Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, null);
            if (nextNodeIds != null
                    && !NodeIdRange.isValidNodeIdRange(nextNodeIds))    {
                LOGGER.warnCr(reconciliation, "Invalid annotation {} on KafkaNodePool {} with value {}", Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, pool.getMetadata().getName(), nextNodeIds);
            }

            String removeNodeIds = Annotations.stringAnnotation(pool, Annotations.ANNO_STRIMZI_IO_REMOVE_NODE_IDS, null);
            if (removeNodeIds != null
                    && !NodeIdRange.isValidNodeIdRange(removeNodeIds))    {
                LOGGER.warnCr(reconciliation, "Invalid annotation {} on KafkaNodePool {} with value {}", Annotations.ANNO_STRIMZI_IO_REMOVE_NODE_IDS, pool.getMetadata().getName(), nextNodeIds);
            }
        });
    }

    /**
     * Extracts the cluster IDs found in the Kafka and KafkaNodePool custom resources and validates whether the
     * resources do not belong to different Kafka cluster. This method is used for KRaft based Kafka clusters. In Kraft
     * based Kafka clusters, we set the cluster ID our-self. So it might happen that the KafkaNodePool status has the
     * new cluster ID but the Kafka CR does not due to some intermittent failure. This differs from ZooKeeper based
     * Kafka clusters which have their own validation method.
     *
     * @param reconciliation    Reconciliation marker
     * @param kafka             The Kafka custom resource which should be validated
     * @param pools             The list of KafkaNodePool custom resources which should be validated
     *
     * @return  List with errors found while checking the cluster IDs or null if no issues found
     */
    public static List<String> validateKRaftClusterIds(Reconciliation reconciliation, Kafka kafka, List<KafkaNodePool> pools)   {
        String firstFoundClusterId = null; // Stores the first found Cluster ID
        boolean hasConflict = false; // indicates if multiple different cluster IDs have been found

        // Try to extract the cluster ID from the Kafka CR status first
        if (kafka.getStatus() != null && kafka.getStatus().getClusterId() != null) {
            firstFoundClusterId = kafka.getStatus().getClusterId();
        }

        // Go through the pools, try to extract the Cluster ID and validate it against already found cluster ID.
        for (KafkaNodePool pool : pools)   {
            if (pool.getStatus() != null && pool.getStatus().getClusterId() != null)   {
                if (firstFoundClusterId == null)   {
                    firstFoundClusterId = pool.getStatus().getClusterId();
                } else if (!firstFoundClusterId.equals(pool.getStatus().getClusterId())) {
                    hasConflict = true;
                    LOGGER.warnCr(reconciliation, "Cluster ID {} found in KafkaNodePool {} is not the same as the previously found cluster ID {}", pool.getStatus().getClusterId(), pool.getMetadata().getName(), firstFoundClusterId);
                }
            }
        }

        if (hasConflict)   {
            return List.of("The Kafka custom resource and its KafkaNodePool resources use different cluster IDs.");
        } else {
            return List.of();
        }
    }

    /**
     * If the cluster already exists and has a cluster ID set in its status, it will be extracted from it. If it doesn't
     * exist in the status yet, a null will be returned.
     *
     * @param kafkaCr   The Kafka CR from which the cluster ID should be extracted
     * @param pools     The list with Kafka pools belonging to this cluster
     *
     * @return  The extracted cluster ID or null if it is not set
     */
    public static String getClusterIdIfSet(Kafka kafkaCr, List<KafkaNodePool> pools)   {
        // This method expects the cluster IDs to be already validated with the validation methods in this class
        if (kafkaCr.getStatus() != null && kafkaCr.getStatus().getClusterId() != null) {
            return kafkaCr.getStatus().getClusterId();
        } else if (pools != null)    {
            return pools
                    .stream()
                    .filter(pool -> pool.getStatus() != null && pool.getStatus().getClusterId() != null)
                    .map(pool -> pool.getStatus().getClusterId())
                    .findFirst()
                    .orElse(null);
        } else {
            return null;
        }
    }

    /**
     * If the cluster already exists and has a cluster ID set in its status, it will be extracted from it. If it doesn't
     * exist in the status yet, a new cluster ID will be generated. The cluster ID is used to bootstrap KRaft clusters.
     * The clusterID is added to the KafkaStatus in KafkaReconciler method clusterId(...).
     *
     * @param kafkaCr   The Kafka CR from which the cluster ID should be extracted
     * @param pools     The list with Kafka pools belonging to this cluster
     *
     * @return  The extracted or generated cluster ID
     */
    public static String getOrGenerateKRaftClusterId(Kafka kafkaCr, List<KafkaNodePool> pools)   {
        String clusterId = getClusterIdIfSet(kafkaCr, pools);

        if (clusterId == null) {
            clusterId = Uuid.randomUuid().toString();
        }

        return clusterId;
    }
}
