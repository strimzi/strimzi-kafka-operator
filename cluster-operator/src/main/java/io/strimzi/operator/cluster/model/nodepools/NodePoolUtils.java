/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.nodepools;

import io.strimzi.api.kafka.model.kafka.JbodStorage;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.cluster.model.KafkaPool;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.model.SharedEnvironmentProvider;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.InvalidResourceException;
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
     * @param reconciliation                Reconciliation marker
     * @param kafka                         Kafka custom resource
     * @param nodePools                     List of node pools belonging to this cluster
     * @param oldStorage                    Maps with old storage configurations, where the key is the name of the controller
     *                                      resource (e.g. my-cluster-pool-a) and the value is the current storage configuration
     * @param versionChange                 Describes Kafka versions used by this cluster
     * @param sharedEnvironmentProvider     Shared environment provider
     *
     * @return  List of KafkaPool instances belonging to given Kafka cluster
     */
    public static List<KafkaPool> createKafkaPools(
            Reconciliation reconciliation,
            Kafka kafka,
            List<KafkaNodePool> nodePools,
            Map<String, Storage> oldStorage,
            KafkaVersionChange versionChange,
            SharedEnvironmentProvider sharedEnvironmentProvider)    {
        // We create the Kafka pool resources
        List<KafkaPool> pools = new ArrayList<>();

        validateNodePools(reconciliation, kafka, nodePools, versionChange);

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

        return pools;
    }

    /**
     * Validates KafkaNodePools
     *
     * @param reconciliation    Reconciliation marker
     * @param kafka             The Kafka custom resource
     * @param nodePools         The list with KafkaNodePool resources
     * @param versionChange     Describes Kafka versions used by this cluster
     */
    public static void validateNodePools(Reconciliation reconciliation, Kafka kafka, List<KafkaNodePool> nodePools, KafkaVersionChange versionChange)    {
        // If there are no node pools, the rest of the validation makes no sense, so we throw an exception right away
        if (nodePools.isEmpty()
                || nodePools.stream().noneMatch(np -> np.getSpec().getReplicas() > 0))    {
            throw new InvalidResourceException("KafkaNodePools are enabled, but KafkaNodePools for Kafka cluster " + kafka.getMetadata().getName() + " either don't exist or have 0 replicas. " +
                    "Please make sure at least one KafkaNodePool resource exists, is in the same namespace as the Kafka resource, has at least one replica, and has the strimzi.io/cluster label set to the name of the Kafka resource.");
        } else {
            List<String> errors = new ArrayList<>();

            // Validate process roles
            errors.addAll(validateKRaftProcessRoles(nodePools));

            // Validate JBOD storage
            errors.addAll(validateKRaftJbodStorage(nodePools, versionChange));

            // Validate ID ranges
            validateNodeIdRanges(reconciliation, nodePools);

            // Throw an exception if there are any errors
            if (!errors.isEmpty()) {
                throw new InvalidResourceException("The Kafka cluster " + kafka.getMetadata().getName() + " is invalid: " + errors);
            }
        }
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
     * In KRaft mode, JBOD storage with multiple disks is supported from Kafka 3.7.0. But only one disk can be marked as
     * the one used for storing the metadata.
     *
     * @param nodePools         List of Kafka Node Pools
     * @param versionChange     Describes Kafka versions used by this cluster
     *
     * @return  List with errors found during the validation
     */
    public static List<String> validateKRaftJbodStorage(List<KafkaNodePool> nodePools, KafkaVersionChange versionChange)   {
        List<String> errors = new ArrayList<>();

        for (KafkaNodePool pool : nodePools)    {
            if (pool.getSpec().getStorage() != null
                    && pool.getSpec().getStorage() instanceof JbodStorage jbod) {
                if (jbod.getVolumes().size() > 1 && KafkaVersion.compareDottedIVVersions(versionChange.metadataVersion(), "3.7-IV2") < 0) {
                    // When running Kafka older than 3.7.0, JBOD storage is not supported in KRaft.
                    // This check should be removed when all the Kafka versions supported by Strimzi have minimal metadata version 3.7-IV2 and newer (kept for upgrade purposes).
                    // This is tracked in https://github.com/strimzi/strimzi-kafka-operator/issues/9960.
                    errors.add("Using more than one disk in a JBOD storage in KRaft mode is supported only with Apache Kafka 3.7.0 or newer and metadata version 3.7-IV2 or newer (in KafkaNodePool " + pool.getMetadata().getName() + ")");
                }
            }
        }

        return errors;
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
