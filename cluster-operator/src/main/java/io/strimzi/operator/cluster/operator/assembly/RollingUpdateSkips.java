/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.common.ConditionBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.nodepools.NodeIdRange;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.StatusUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Holds the result of resolving the strimzi.io/skip-rolling-update annotations from the KafkaNodePool resources
 * into the set of node IDs which are excluded from automatic rolling updates.
 *
 * @param skippedNodeIds            Node IDs which are excluded from automatic rolling updates
 * @param skippedNodeIdsByPool      Skipped node IDs grouped by the name of the KafkaNodePool which owns them
 * @param ignoredControllerNodeIds  Node IDs which were requested to be skipped but are ignored because they have
 *                                  (or might have) the controller role
 * @param rejectedNodeIds           Annotation values or node IDs which were rejected as invalid (wrong format,
 *                                  unknown node ID, or node ID not belonging to the annotated pool)
 */
public record RollingUpdateSkips(Set<Integer> skippedNodeIds, Map<String, Set<Integer>> skippedNodeIdsByPool, Set<Integer> ignoredControllerNodeIds, List<String> rejectedNodeIds) {
    /**
     * Type of the Kafka and KafkaNodePool status condition used to surface active skips
     */
    public static final String ROLLING_UPDATE_SKIPPED_CONDITION_TYPE = "RollingUpdateSkipped";

    /**
     * An empty resolution => no nodes are excluded from automatic rolling updates
     */
    public static final RollingUpdateSkips EMPTY = new RollingUpdateSkips(Set.of(), Map.of(), Set.of(), List.of());

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(RollingUpdateSkips.class.getName());

    // Used to extract the skipped node IDs back from the message of a previously set RollingUpdateSkipped condition
    private static final Pattern CONDITION_MESSAGE_PATTERN = Pattern.compile("^Nodes \\[([0-9, ]*)] are excluded");

    /**
     * Resolves the strimzi.io/skip-rolling-update annotations set on the KafkaNodePool resources into the set of node
     * IDs which should be excluded from automatic rolling updates. Node IDs which do not belong to the annotated pool
     * or have an invalid format are rejected. Node IDs which resolve to a node with the controller role (either by the
     * desired role of the pool or by the role labels of an existing Pod) are ignored and remain fully managed.
     *
     * @param reconciliation    Reconciliation marker
     * @param nodePools         List of the KafkaNodePool resources belonging to the cluster
     * @param nodes             Set of the desired nodes of the Kafka cluster
     * @param pods              List of the existing Kafka Pods used to check the actual node roles
     *
     * @return  The resolved skips
     */
    public static RollingUpdateSkips resolve(Reconciliation reconciliation, List<KafkaNodePool> nodePools, Set<NodeRef> nodes, List<Pod> pods)   {
        Map<Integer, NodeRef> nodesById = nodes.stream().collect(Collectors.toMap(NodeRef::nodeId, Function.identity()));
        Map<String, Pod> podsByName = pods.stream().collect(Collectors.toMap(pod -> pod.getMetadata().getName(), Function.identity()));

        Set<Integer> skipped = new TreeSet<>();
        Map<String, Set<Integer>> skippedByPool = new HashMap<>();
        Set<Integer> ignoredControllers = new TreeSet<>();
        List<String> rejected = new ArrayList<>();

        for (KafkaNodePool pool : nodePools)    {
            String skipAnnotation = Annotations.stringAnnotation(pool, Annotations.ANNO_STRIMZI_IO_SKIP_ROLLING_UPDATE, null);

            if (skipAnnotation == null || skipAnnotation.isBlank())   {
                continue;
            }

            String poolName = pool.getMetadata().getName();

            for (Integer nodeId : parseNodeIds(reconciliation, poolName, skipAnnotation, rejected))  {
                NodeRef node = nodesById.get(nodeId);

                if (node == null || !poolName.equals(node.poolName()))    {
                    LOGGER.warnCr(reconciliation, "Node ID {} from the {} annotation on KafkaNodePool {} does not belong to this node pool and will be ignored", nodeId, Annotations.ANNO_STRIMZI_IO_SKIP_ROLLING_UPDATE, poolName);
                    rejected.add(poolName + ": " + nodeId);
                } else if (node.controller() || podClaimsControllerRole(podsByName.get(node.podName()))) {
                    LOGGER.warnCr(reconciliation, "Node ID {} from the {} annotation on KafkaNodePool {} has the controller role and will remain fully managed", nodeId, Annotations.ANNO_STRIMZI_IO_SKIP_ROLLING_UPDATE, poolName);
                    ignoredControllers.add(nodeId);
                } else {
                    skipped.add(nodeId);
                    skippedByPool.computeIfAbsent(poolName, name -> new TreeSet<>()).add(nodeId);
                }
            }
        }

        return new RollingUpdateSkips(skipped, Map.copyOf(skippedByPool), ignoredControllers, List.copyOf(rejected));
    }

    /**
     * Parses the value of the skip-rolling-update annotation (for example `[2,5]` or `[2,4-6]`) into individual node
     * IDs. Invalid entries are rejected individually, and the remaining valid entries are still applied.
     *
     * @param reconciliation    Reconciliation marker
     * @param poolName          Name of the KafkaNodePool the annotation was found on (used in log and status messages)
     * @param skipAnnotation    The value of the annotation
     * @param rejected          List where the rejected values are collected
     *
     * @return  Set with the valid node IDs from the annotation
     */
    private static Set<Integer> parseNodeIds(Reconciliation reconciliation, String poolName, String skipAnnotation, List<String> rejected)  {
        Set<Integer> nodeIds = new TreeSet<>();
        String trimmed = skipAnnotation.trim();

        if (!trimmed.startsWith("[") || !trimmed.endsWith("]"))  {
            LOGGER.warnCr(reconciliation, "The {} annotation on KafkaNodePool {} has an invalid format and will be ignored: {}", Annotations.ANNO_STRIMZI_IO_SKIP_ROLLING_UPDATE, poolName, skipAnnotation);
            rejected.add(poolName + ": " + skipAnnotation);
            return nodeIds;
        }

        String content = trimmed.substring(1, trimmed.length() - 1).trim();

        if (content.isEmpty())  {
            return nodeIds;
        }

        for (String entry : content.split(","))    {
            String trimmedEntry = entry.trim();

            try {
                NodeIdRange range = new NodeIdRange("[" + trimmedEntry + "]");
                Integer nodeId;
                while ((nodeId = range.getNextNodeId()) != null)    {
                    nodeIds.add(nodeId);
                }
            } catch (NodeIdRange.InvalidNodeIdRangeException e) {
                LOGGER.warnCr(reconciliation, "Node ID {} from the {} annotation on KafkaNodePool {} is invalid and will be ignored", trimmedEntry, Annotations.ANNO_STRIMZI_IO_SKIP_ROLLING_UPDATE, poolName);
                rejected.add(poolName + ": " + trimmedEntry);
            }
        }

        return nodeIds;
    }

    /**
     * Checks whether an existing Pod claims the controller role through its role labels. Desired and actual roles can
     * diverge during a role transition, so the skip is honored only when the existing Pod does not claim the
     * controller role either.
     *
     * @param pod   The Pod to check. Might be null when the Pod does not exist.
     *
     * @return  True when the Pod exists and its labels claim the controller role. False otherwise.
     */
    private static boolean podClaimsControllerRole(Pod pod) {
        return pod != null
                && pod.getMetadata().getLabels() != null
                && "true".equals(pod.getMetadata().getLabels().get(Labels.STRIMZI_CONTROLLER_ROLE_LABEL));
    }

    /**
     * Indicates whether any node is excluded from automatic rolling updates
     *
     * @return  True when at least one node is excluded from automatic rolling updates. False otherwise.
     */
    public boolean hasSkippedNodes()    {
        return !skippedNodeIds.isEmpty();
    }

    /**
     * Indicates whether the RollingUpdateSkipped condition should be set on the Kafka custom resource status
     *
     * @return  True when the resolution found any skipped, ignored or rejected node IDs and the status condition
     *          should be set. False otherwise.
     */
    public boolean shouldReportCondition()    {
        return !skippedNodeIds.isEmpty() || !ignoredControllerNodeIds.isEmpty() || !rejectedNodeIds.isEmpty();
    }

    /**
     * Generates the RollingUpdateSkipped condition for the Kafka custom resource status
     *
     * @return  The condition describing the active skips
     */
    public Condition kafkaStatusCondition()   {
        StringBuilder message = new StringBuilder("Nodes ").append(skippedNodeIds).append(" are excluded from automatic rolling updates through the ").append(Annotations.ANNO_STRIMZI_IO_SKIP_ROLLING_UPDATE).append(" annotation.");

        if (!ignoredControllerNodeIds.isEmpty())    {
            message.append(" Node IDs ignored because of the controller role: ").append(ignoredControllerNodeIds).append(".");
        }

        if (!rejectedNodeIds.isEmpty())    {
            message.append(" Rejected invalid node IDs: ").append(rejectedNodeIds).append(".");
        }

        return new ConditionBuilder()
                .withLastTransitionTime(StatusUtils.iso8601Now())
                .withType(ROLLING_UPDATE_SKIPPED_CONDITION_TYPE)
                .withStatus("True")
                .withReason("SkipRollingUpdateAnnotation")
                .withMessage(message.toString())
                .build();
    }

    /**
     * Generates the RollingUpdateSkipped condition for a KafkaNodePool status
     *
     * @param skippedNodeIdsInPool  The skipped node IDs belonging to the node pool
     *
     * @return  The condition describing the active skips in this node pool
     */
    public static Condition nodePoolStatusCondition(Set<Integer> skippedNodeIdsInPool)   {
        return new ConditionBuilder()
                .withLastTransitionTime(StatusUtils.iso8601Now())
                .withType(ROLLING_UPDATE_SKIPPED_CONDITION_TYPE)
                .withStatus("True")
                .withReason("SkipRollingUpdateAnnotation")
                .withMessage("Nodes " + skippedNodeIdsInPool + " are excluded from automatic rolling updates through the " + Annotations.ANNO_STRIMZI_IO_SKIP_ROLLING_UPDATE + " annotation.")
                .build();
    }

    /**
     * Extracts the skipped node IDs from the message of a previously set RollingUpdateSkipped condition. This is used
     * to detect changes to the set of skipped nodes between reconciliations for emitting Kubernetes Events.
     *
     * @param condition The RollingUpdateSkipped condition from the previous reconciliation. Might be null.
     *
     * @return  Set with the node IDs which were skipped in the previous reconciliation
     */
    public static Set<Integer> skippedNodeIdsFromCondition(Condition condition)   {
        Set<Integer> nodeIds = new TreeSet<>();

        if (condition != null && condition.getMessage() != null)   {
            Matcher matcher = CONDITION_MESSAGE_PATTERN.matcher(condition.getMessage());

            if (matcher.find())    {
                for (String id : matcher.group(1).split(","))   {
                    String trimmedId = id.trim();
                    if (!trimmedId.isEmpty())   {
                        nodeIds.add(Integer.parseInt(trimmedId));
                    }
                }
            }
        }

        return nodeIds;
    }
}
