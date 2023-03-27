/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.nodepools;

import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.operator.common.InvalidConfigurationException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Generates the Node ID assignments from / for node pools
 */
public class NodeIdAssignor {
    private final Map<String, NodeIdAssignment> assignments = new HashMap<>();

    /**
     * Constructs the NodeIdAssignor instance.
     *
     * @param nodePools     List of KafkaNodePool resources including their statuses
     */
    /* test */ NodeIdAssignor(KafkaNodePool... nodePools) {
        this(Arrays.asList(nodePools));
    }

    /**
     * Constructs the NodeIdAssignor instance
     *
     * @param nodePools     List of KafkaNodePool resources including their statuses
     */
    public NodeIdAssignor(List<KafkaNodePool> nodePools) {
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
            TreeSet<Integer> toBeRemoved = new TreeSet<>();
            TreeSet<Integer> toBeAdded = new TreeSet<>();

            // If there are no IDs in the status, there is no scale-down
            if (pool.getStatus() != null && pool.getStatus().getNodeIds() != null)  {
                current = new TreeSet<>(pool.getStatus().getNodeIds());

                if (pool.getSpec().getReplicas() < pool.getStatus().getNodeIds().size())    {
                    // We have a scale-down
                    desired = new TreeSet<>(current);

                    for (int i = 0; i < (current.size() - pool.getSpec().getReplicas()); i++)   {
                        toBeRemoved.add(desired.pollLast());
                    }
                } else if (pool.getSpec().getReplicas() > pool.getStatus().getNodeIds().size())    {
                    // We have a scale-up
                    desired = new TreeSet<>(current);

                    for (int i = 0; i < (pool.getSpec().getReplicas() - current.size()); i++)   {
                        Integer nextId = provideNextId(idsInUse);
                        toBeAdded.add(nextId);
                        desired.add(nextId);
                    }
                } else {
                    // We have no change
                    desired = current;
                }
            } else {
                // New pool? It is all scale-up
                current = new TreeSet<>();
                desired = new TreeSet<>();

                for (int i = 0; i < pool.getSpec().getReplicas(); i++)   {
                    Integer nextId = provideNextId(idsInUse);
                    toBeAdded.add(nextId);
                    desired.add(nextId);
                }
            }

            assignments.put(pool.getMetadata().getName(), new NodeIdAssignment(current, desired, toBeRemoved, toBeAdded));
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
     * @param idsInUse  TreeSet with already used IDs. This should contain all IDs currently used (including those which
     *                  will be scaled done) or those already generated. The newly generated ID is automatically added
     *                  to this set by this method.
     *
     * @return  The next node ID
     */
    private int provideNextId(TreeSet<Integer> idsInUse) {
        int nextId;

        // TODO: We should provide more advanced strategies for assigning the node IDs. For example
        //           * Search for gaps
        //           * Use annotations to control the next ID
        //           * Assigning various node ID ranges to different node pools
        //           * Configuring the next IDs which should be used for a new broker
        //           * Configuring which broker should be removed at scale down
        //           * (more situations are described in the proposal)
        //       This is tracked by https://github.com/strimzi/strimzi-kafka-operator/issues/8590

        if (idsInUse.isEmpty()) {
            nextId = 0;
        } else {
            nextId = idsInUse.last() + 1;
        }

        idsInUse.add(nextId);
        return nextId;
    }
}

