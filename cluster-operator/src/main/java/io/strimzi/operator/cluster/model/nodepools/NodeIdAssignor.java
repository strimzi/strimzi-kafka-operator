/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.nodepools;

import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * Generates the Node ID assignments from / for node pools
 */
public class NodeIdAssignor {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(NodePoolUtils.class.getName());

    private final Map<String, NodeIdAssignment> assignments = new HashMap<>();

    /**
     * Constructs the NodeIdAssignor instance.
     *
     * @param reconciliation    Reconciliation marker
     * @param nodePools         List of KafkaNodePool resources including their statuses
     */
    /* test */ NodeIdAssignor(Reconciliation reconciliation, KafkaNodePool... nodePools) {
        this(reconciliation, Arrays.asList(nodePools));
    }

    /**
     * Constructs the NodeIdAssignor instance
     *
     * @param reconciliation    Reconciliation marker
     * @param nodePools         List of KafkaNodePool resources including their statuses
     */
    public NodeIdAssignor(Reconciliation reconciliation, List<KafkaNodePool> nodePools) {
        // Collect the existing assigned IDs
        TreeSet<Integer> idsInUse = new TreeSet<>();

        for (KafkaNodePool pool : nodePools)  {
            if (pool.getStatus() != null && pool.getStatus().getNodeIds() != null)  {
                for (Integer id : pool.getStatus().getNodeIds())    {
                    if (!idsInUse.add(id))   {
                        // When add returns false, it means the ID was already present in the list
                        throw new InvalidConfigurationException("Found duplicate node ID " + id + " in node pool " + pool.getMetadata().getName());
                    }
                }
            }
        }

        // Identify the scale-down or up IDs
        for (KafkaNodePool pool : nodePools)  {
            TreeSet<Integer> current;
            TreeSet<Integer> desired;
            TreeSet<Integer> usedToBeBroker;
            TreeSet<Integer> toBeRemoved = new TreeSet<>();
            TreeSet<Integer> toBeAdded = new TreeSet<>();

            // If there are no IDs in the status, there is no scale-down
            if (pool.getStatus() != null && pool.getStatus().getNodeIds() != null)  {
                current = new TreeSet<>(pool.getStatus().getNodeIds());

                if (pool.getSpec().getReplicas() < pool.getStatus().getNodeIds().size())    {
                    // We have a scale-down
                    desired = new TreeSet<>(current);

                    // Provides the node IDs which the user would like to remove first
                    NodeIdRange removeIdRange = removeIdRange(reconciliation, pool);

                    for (int i = 0; i < (current.size() - pool.getSpec().getReplicas()); i++)   {
                        toBeRemoved.add(nextRemoveId(desired, removeIdRange));
                    }
                } else if (pool.getSpec().getReplicas() > pool.getStatus().getNodeIds().size())    {
                    // We have a scale-up
                    desired = new TreeSet<>(current);

                    // Provides the node IDs which the user would like to assign to the next broker(s)
                    NodeIdRange nextIdRange = nextIdRange(reconciliation, pool);

                    for (int i = 0; i < (pool.getSpec().getReplicas() - current.size()); i++)   {
                        Integer nextId = provideNextId(idsInUse, nextIdRange);
                        toBeAdded.add(nextId);
                        desired.add(nextId);
                    }
                } else {
                    // We have no change
                    desired = current;
                }

                usedToBeBroker = brokerNodesBecomingControllerOnlyNodes(pool, current, desired);
            } else {
                // New pool? It is all scale-up
                current = new TreeSet<>();
                usedToBeBroker = new TreeSet<>();
                desired = new TreeSet<>();

                // Provides the node IDs which the user would like to assign to the next broker(s)
                NodeIdRange nextIdRange = nextIdRange(reconciliation, pool);

                for (int i = 0; i < pool.getSpec().getReplicas(); i++)   {
                    Integer nextId = provideNextId(idsInUse, nextIdRange);
                    toBeAdded.add(nextId);
                    desired.add(nextId);
                }
            }

            assignments.put(pool.getMetadata().getName(), new NodeIdAssignment(current, desired, toBeRemoved, toBeAdded, usedToBeBroker));
        }
    }

    /**
     * @return  Returns all node assignments
     */
    public Map<String, NodeIdAssignment> assignments()   {
        return assignments;
    }

    /**
     * Returns node assignments for a particular node pool
     *
     * @param poolName  Name of the node pool for which the node assignments should be returned
     *
     * @return  Node assignments for given node pool or null if they don't exist
     */
    public NodeIdAssignment assignmentForPool(String poolName)   {
        return assignments.get(poolName);
    }

    /**
     * Generates the next node ID based on the already used IDs
     *
     * @param idsInUse      TreeSet with already used IDs. This should contain all IDs currently used (including those which
     *                      will be scaled done) or those already generated. The newly generated ID is automatically added
     *                      to this set by this method.
     * @param nextIdRange   The NodeIdRange which will pick up the next node ID
     *
     * @return  The next node ID
     */
    private int provideNextId(TreeSet<Integer> idsInUse, NodeIdRange nextIdRange) {
        // We try to first get the Node ID from range first
        Integer nextId = nextUnusedIdFromRange(idsInUse, nextIdRange);

        // We did not get an ID from the ID range. So we get the next ID only
        if (nextId == null) {
            nextId = nextUnusedId(idsInUse);
        }

        // We add the new ID to used IDs
        idsInUse.add(nextId);

        return nextId;
    }

    /**
     * Finds the next unused ID sequentially. It will take the highest used ID and increment is by 1.
     *
     * @param idsInUse  Set with currently used IDs
     *
     * @return  The next unused Node ID
     */
    private Integer nextUnusedId(TreeSet<Integer> idsInUse) {
        if (idsInUse.isEmpty()) {
            // Its empty -> we start with 0
            return 0;
        } else if (idsInUse.size() == idsInUse.last() + 1)    {
            // Size is the same as the last index -> no gaps
            return idsInUse.last() + 1;
        } else {
            // There are some IDs but also gaps. Let's try to find a gap
            return findNextUnusedIdFromGap(idsInUse);
        }
    }

    /**
     * Finds the lowest available number from a gap inside the set. It starts from 0 and increments by one until it
     * finds a used ID. Algorithm based on binary search was considered, but for the expected numbers of node IDs
     * (== nodes of a Kafka cluster) it did not seem worth it (increased complexity over marginally faster execution).
     *
     * @param setWithGaps   Set with some gaps
     *
     * @return  The lowest available ID
     */
    /* test */ static Integer findNextUnusedIdFromGap(NavigableSet<Integer> setWithGaps)  {
        int next = 0;

        while (setWithGaps.contains(next))  {
            next++;
        }

        return next;
    }

    /**
     * Finds the next node ID from a range which is not in use. The IDs are found from range specified in the Node Pool
     * annotation strimzi.io/next-node-ids.
     *
     * @param idsInUse      Set with currently used IDs
     * @param nextIdRange   The NodeIdRange which will pick up the next node ID
     *
     * @return  Next unused node ID from the range or null if no such ID was found
     */
    private Integer nextUnusedIdFromRange(TreeSet<Integer> idsInUse, NodeIdRange nextIdRange)   {
        Integer nextId = null;

        if (nextIdRange != null)    {
            while ((nextId = nextIdRange.getNextNodeId()) != null)  {
                // If the next ID is not in use, we can break the while loop
                // If it is already used, we continue wth the next ID
                if (!idsInUse.contains(nextId)) {
                    break;
                }
            }
        }

        return nextId;
    }

    /**
     * Creates the Node ID range based on the strimzi.io/next-node-ids annotation on the Node Pool.
     *
     * @param reconciliation    Reconciliation marker
     * @param pool              NodePool instance
     *
     * @return  The NodeIdRange based on the annotation or null when the annotation is not set or is invalid.
     */
    private NodeIdRange nextIdRange(Reconciliation reconciliation, KafkaNodePool pool)   {
        String nextNodeIds = Annotations.stringAnnotation(pool, Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, null);

        try {
            return nextNodeIds == null ? null : new NodeIdRange(nextNodeIds);
        } catch (NodeIdRange.InvalidNodeIdRangeException e) {
            LOGGER.warnCr(reconciliation, "Failed to use " + Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS + " " + nextNodeIds + ". New node IDs will be assigned sequentially.", e);
            return null;
        }
    }

    /**
     * Generates the next node ID which should be removed based on the already used Node IDs and the user preferred range.
     *
     * @param desired           TreeSet with already used IDs. This should contain all IDs currently used (including those which
     *                          will be scaled done) or those already generated. The newly generated ID is automatically added
     *                          to this set by this method.
     * @param removeIdRange     The NodeIdRange which will pick up the next node ID
     *
     * @return  The next node ID to remove in scale down
     */
    private int nextRemoveId(TreeSet<Integer> desired, NodeIdRange removeIdRange) {
        // We try to first get the Node ID from range first
        Integer removeId = removeUsedIdFromRange(desired, removeIdRange);

        // We did not get an ID from the ID range. So we get the next ID only
        if (removeId == null) {
            removeId = desired.pollLast();
        }

        return removeId;
    }

    /**
     * Finds the next node ID which should be removed from the existing node IDs. The IDs are found from range specified
     * in the Node Pool annotation strimzi.io/remove-node-ids.
     *
     * @param desired           Set with currently used IDs
     * @param removeIdRange     The NodeIdRange which will pick up the next node ID
     *
     * @return  Next unused node ID from the range or null if no such ID was found
     */
    private Integer removeUsedIdFromRange(TreeSet<Integer> desired, NodeIdRange removeIdRange)   {
        Integer removeId = null;

        if (removeIdRange != null)    {
            while ((removeId = removeIdRange.getNextNodeId()) != null)  {
                // If the next ID is in use (remove returns true), we can break the while loop
                // If it is not used, we continue with the next ID
                if (desired.remove(removeId)) {
                    break;
                }
            }
        }

        return removeId;
    }

    /**
     * Creates the Node ID range based on the strimzi.io/remove-node-ids annotation on the Node Pool.
     *
     * @param reconciliation    Reconciliation marker
     * @param pool              NodePool instance
     *
     * @return  The NodeIdRange based on the annotation or null when the annotation is not set or is invalid.
     */
    private NodeIdRange removeIdRange(Reconciliation reconciliation, KafkaNodePool pool)   {
        String removeNodeIds = Annotations.stringAnnotation(pool, Annotations.ANNO_STRIMZI_IO_REMOVE_NODE_IDS, null);

        try {
            return removeNodeIds == null ? null : new NodeIdRange(removeNodeIds);
        } catch (NodeIdRange.InvalidNodeIdRangeException e) {
            LOGGER.warnCr(reconciliation, "Failed to use " + Annotations.ANNO_STRIMZI_IO_REMOVE_NODE_IDS + " " + removeNodeIds + ". Nodes will be scaled down from the highest node ID.", e);
            return null;
        }
    }

    /**
     * Finds any Kafka nodes that used to have the broker role but should not have it anymore. These nodes require a
     * special treatment - for example they should have a special check for not having any partition replicas assigned.
     *
     * @param pool      Node Pool to check for the roles change
     * @param current   Current node IDs belonging to this node pool
     * @param desired   Desired node IDs belonging to this node pool
     *
     * @return  Set of node IDs that used to have the broker role but will not have it anymore
     */
    /* test */ static TreeSet<Integer> brokerNodesBecomingControllerOnlyNodes(KafkaNodePool pool, Set<Integer> current, Set<Integer> desired) {
        if (pool.getStatus() != null
                && pool.getSpec().getRoles() != null
                && pool.getStatus().getRoles() != null
                && pool.getStatus().getRoles().contains(ProcessRoles.BROKER) // Used to have the broker role
                && !pool.getSpec().getRoles().contains(ProcessRoles.BROKER)) { // But should not have it anymore
            // Collect all node IDs that are both current and desired (i.e. we do not care about old nodes being scaled down or new nodes being scaled up)
            TreeSet<Integer> usedToBeBroker = new TreeSet<>(desired);
            usedToBeBroker.retainAll(current);
            return usedToBeBroker;
        } else {
            return new TreeSet<>();
        }
    }
}

