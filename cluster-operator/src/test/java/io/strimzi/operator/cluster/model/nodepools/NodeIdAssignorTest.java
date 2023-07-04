/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.nodepools;

import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class NodeIdAssignorTest {
    @Test
    public void testNewSinglePoolCluster()  {
        NodeIdAssignor assignor = new NodeIdAssignor(Reconciliation.DUMMY_RECONCILIATION, createPool("my-pool", 3, null));

        assertThat(assignor.assignments().size(), is(1));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.current(), is(Set.of()));
        assertThat(assignment.desired(), is(Set.of(0, 1, 2)));
        assertThat(assignment.toBeAdded(), is(Set.of(0, 1, 2)));
        assertThat(assignment.toBeRemoved(), is(Set.of()));
    }

    @Test
    public void testNewSinglePoolClusterWithNextIds()  {
        NodeIdAssignor assignor = new NodeIdAssignor(Reconciliation.DUMMY_RECONCILIATION, createPool("my-pool", 3, Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[1000-1999]")));

        assertThat(assignor.assignments().size(), is(1));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.current(), is(Set.of()));
        assertThat(assignment.desired(), is(Set.of(1000, 1001, 1002)));
        assertThat(assignment.toBeAdded(), is(Set.of(1000, 1001, 1002)));
        assertThat(assignment.toBeRemoved(), is(Set.of()));
    }

    @Test
    public void testNewSinglePoolClusterWithNextIdsForSomeNodes()  {
        NodeIdAssignor assignor = new NodeIdAssignor(Reconciliation.DUMMY_RECONCILIATION, createPool("my-pool", 5, Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[1000-1002]")));

        assertThat(assignor.assignments().size(), is(1));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.current(), is(Set.of()));
        assertThat(assignment.desired(), is(Set.of(0, 1, 1000, 1001, 1002)));
        assertThat(assignment.toBeAdded(), is(Set.of(0, 1, 1000, 1001, 1002)));
        assertThat(assignment.toBeRemoved(), is(Set.of()));
    }

    @Test
    public void testExistingSinglePoolClusterWithoutChange()  {
        NodeIdAssignor assignor = new NodeIdAssignor(Reconciliation.DUMMY_RECONCILIATION, createPool("my-pool", 3, null, List.of(0, 1, 2)));

        assertThat(assignor.assignments().size(), is(1));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.current(), is(Set.of(0, 1, 2)));
        assertThat(assignment.desired(), is(Set.of(0, 1, 2)));
        assertThat(assignment.toBeAdded(), is(Set.of()));
        assertThat(assignment.toBeRemoved(), is(Set.of()));
    }

    @Test
    public void testExistingSinglePoolClusterOutOfSequenceWithoutChange()  {
        NodeIdAssignor assignor = new NodeIdAssignor(Reconciliation.DUMMY_RECONCILIATION, createPool("my-pool", 3, null, List.of(2, 3, 5)));

        assertThat(assignor.assignments().size(), is(1));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.current(), is(Set.of(2, 3, 5)));
        assertThat(assignment.desired(), is(Set.of(2, 3, 5)));
        assertThat(assignment.toBeAdded(), is(Set.of()));
        assertThat(assignment.toBeRemoved(), is(Set.of()));
    }

    @Test
    public void testNewSinglePoolClusterWithNextIdsWithoutChange()  {
        NodeIdAssignor assignor = new NodeIdAssignor(Reconciliation.DUMMY_RECONCILIATION, createPool("my-pool", 3, Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[2000-2999]"), List.of(1000, 1001, 1002)));

        assertThat(assignor.assignments().size(), is(1));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.current(), is(Set.of(1000, 1001, 1002)));
        assertThat(assignment.desired(), is(Set.of(1000, 1001, 1002)));
        assertThat(assignment.toBeAdded(), is(Set.of()));
        assertThat(assignment.toBeRemoved(), is(Set.of()));
    }

    @Test
    public void testNewSinglePoolClusterWithRemoveIdsWithoutChange()  {
        NodeIdAssignor assignor = new NodeIdAssignor(Reconciliation.DUMMY_RECONCILIATION, createPool("my-pool", 3, Map.of(Annotations.ANNO_STRIMZI_IO_REMOVE_NODE_IDS, "[2000-2999]"), List.of(1000, 1001, 1002)));

        assertThat(assignor.assignments().size(), is(1));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.current(), is(Set.of(1000, 1001, 1002)));
        assertThat(assignment.desired(), is(Set.of(1000, 1001, 1002)));
        assertThat(assignment.toBeAdded(), is(Set.of()));
        assertThat(assignment.toBeRemoved(), is(Set.of()));
    }

    @Test
    public void testExistingSinglePoolClusterScaleDown()  {
        NodeIdAssignor assignor = new NodeIdAssignor(Reconciliation.DUMMY_RECONCILIATION, createPool("my-pool", 3, null, List.of(0, 1, 2, 3, 4)));

        assertThat(assignor.assignments().size(), is(1));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.current(), is(Set.of(0, 1, 2, 3, 4)));
        assertThat(assignment.desired(), is(Set.of(0, 1, 2)));
        assertThat(assignment.toBeAdded(), is(Set.of()));
        assertThat(assignment.toBeRemoved(), is(Set.of(3, 4)));
    }

    @Test
    public void testExistingSinglePoolClusterScaleDownWithRemoveIds()  {
        NodeIdAssignor assignor = new NodeIdAssignor(Reconciliation.DUMMY_RECONCILIATION, createPool("my-pool", 3, Map.of(Annotations.ANNO_STRIMZI_IO_REMOVE_NODE_IDS, "[1-3]"), List.of(0, 1, 2, 3, 4)));

        assertThat(assignor.assignments().size(), is(1));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.current(), is(Set.of(0, 1, 2, 3, 4)));
        assertThat(assignment.desired(), is(Set.of(0, 3, 4)));
        assertThat(assignment.toBeAdded(), is(Set.of()));
        assertThat(assignment.toBeRemoved(), is(Set.of(1, 2)));
    }

    @Test
    public void testExistingSinglePoolClusterScaleDownWithUnusedRemoveIds()  {
        NodeIdAssignor assignor = new NodeIdAssignor(Reconciliation.DUMMY_RECONCILIATION, createPool("my-pool", 3, Map.of(Annotations.ANNO_STRIMZI_IO_REMOVE_NODE_IDS, "[1000-1999]"), List.of(0, 1, 2, 3, 4)));

        assertThat(assignor.assignments().size(), is(1));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.current(), is(Set.of(0, 1, 2, 3, 4)));
        assertThat(assignment.desired(), is(Set.of(0, 1, 2)));
        assertThat(assignment.toBeAdded(), is(Set.of()));
        assertThat(assignment.toBeRemoved(), is(Set.of(3, 4)));
    }

    @Test
    public void testExistingSinglePoolClusterScaleDownBigClusterWithRemoveIds()  {
        NodeIdAssignor assignor = new NodeIdAssignor(Reconciliation.DUMMY_RECONCILIATION, createPool("my-pool", 5000, Map.of(Annotations.ANNO_STRIMZI_IO_REMOVE_NODE_IDS, "[10000-19999, 4613, 1919, 1874]"), IntStream.range(0, 5002).boxed().collect(Collectors.toList())));

        assertThat(assignor.assignments().size(), is(1));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.toBeRemoved(), is(Set.of(4613, 1919)));
    }

    @Test
    public void testExistingSinglePoolClusterScaleUp()  {
        NodeIdAssignor assignor = new NodeIdAssignor(Reconciliation.DUMMY_RECONCILIATION, createPool("my-pool", 5, Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[1000-1999]"), List.of(0, 1, 2)));

        assertThat(assignor.assignments().size(), is(1));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.current(), is(Set.of(0, 1, 2)));
        assertThat(assignment.desired(), is(Set.of(0, 1, 2, 1000, 1001)));
        assertThat(assignment.toBeAdded(), is(Set.of(1000, 1001)));
        assertThat(assignment.toBeRemoved(), is(Set.of()));
    }

    @Test
    public void testExistingSinglePoolClusterScaleUpWithNextIds()  {
        NodeIdAssignor assignor = new NodeIdAssignor(Reconciliation.DUMMY_RECONCILIATION, createPool("my-pool", 5, null, List.of(0, 1, 2)));

        assertThat(assignor.assignments().size(), is(1));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.current(), is(Set.of(0, 1, 2)));
        assertThat(assignment.desired(), is(Set.of(0, 1, 2, 3, 4)));
        assertThat(assignment.toBeAdded(), is(Set.of(3, 4)));
        assertThat(assignment.toBeRemoved(), is(Set.of()));
    }

    @Test
    public void testExistingSinglePoolBigClusterScaleUpWithNextIds()  {
        // We scale cluster from 5000 nodes to 5003 nodes
        NodeIdAssignor assignor = new NodeIdAssignor(Reconciliation.DUMMY_RECONCILIATION, createPool("my-pool", 5003, Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[1000-1999, 6000, 7000, 8000]"), IntStream.range(0, 5000).boxed().collect(Collectors.toList())));

        assertThat(assignor.assignments().size(), is(1));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.toBeAdded(), is(Set.of(6000, 7000, 8000)));
    }

    @Test
    public void testNewMultiPoolCluster()  {
        NodeIdAssignor assignor = new NodeIdAssignor(
                Reconciliation.DUMMY_RECONCILIATION, createPool("my-pool", 3, null),
                createPool("my-pool2", 3, null)
        );

        assertThat(assignor.assignments().size(), is(2));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.current(), is(Set.of()));
        assertThat(assignment.desired(), is(Set.of(0, 1, 2)));
        assertThat(assignment.toBeAdded(), is(Set.of(0, 1, 2)));
        assertThat(assignment.toBeRemoved(), is(Set.of()));

        assignment = assignor.assignmentForPool("my-pool2");
        assertThat(assignment.current(), is(Set.of()));
        assertThat(assignment.desired(), is(Set.of(3, 4, 5)));
        assertThat(assignment.toBeAdded(), is(Set.of(3, 4, 5)));
        assertThat(assignment.toBeRemoved(), is(Set.of()));
    }

    @Test
    public void testNewMultiPoolClusterWithNextIds()  {
        NodeIdAssignor assignor = new NodeIdAssignor(
                Reconciliation.DUMMY_RECONCILIATION, createPool("my-pool", 3, Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[1000-1999]")),
                createPool("my-pool2", 3, Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[2000-2999]"))
        );

        assertThat(assignor.assignments().size(), is(2));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.current(), is(Set.of()));
        assertThat(assignment.desired(), is(Set.of(1000, 1001, 1002)));
        assertThat(assignment.toBeAdded(), is(Set.of(1000, 1001, 1002)));
        assertThat(assignment.toBeRemoved(), is(Set.of()));

        assignment = assignor.assignmentForPool("my-pool2");
        assertThat(assignment.current(), is(Set.of()));
        assertThat(assignment.desired(), is(Set.of(2000, 2001, 2002)));
        assertThat(assignment.toBeAdded(), is(Set.of(2000, 2001, 2002)));
        assertThat(assignment.toBeRemoved(), is(Set.of()));
    }

    @Test
    public void testExistingMultiPoolCluster()  {
        NodeIdAssignor assignor = new NodeIdAssignor(
                Reconciliation.DUMMY_RECONCILIATION, createPool("my-pool", 3, null, List.of(0, 1, 2)),
                createPool("my-pool2", 3, null, List.of(3, 4, 5))
        );

        assertThat(assignor.assignments().size(), is(2));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.current(), is(Set.of(0, 1, 2)));
        assertThat(assignment.desired(), is(Set.of(0, 1, 2)));
        assertThat(assignment.toBeAdded(), is(Set.of()));
        assertThat(assignment.toBeRemoved(), is(Set.of()));

        assignment = assignor.assignmentForPool("my-pool2");
        assertThat(assignment.current(), is(Set.of(3, 4, 5)));
        assertThat(assignment.desired(), is(Set.of(3, 4, 5)));
        assertThat(assignment.toBeAdded(), is(Set.of()));
        assertThat(assignment.toBeRemoved(), is(Set.of()));
    }

    @Test
    public void testExistingMultiPoolClusterScaleUp()  {
        NodeIdAssignor assignor = new NodeIdAssignor(
                Reconciliation.DUMMY_RECONCILIATION, createPool("my-pool", 5, null, List.of(0, 1, 2)),
                createPool("my-pool2", 5, null, List.of(3, 4, 5))
        );

        assertThat(assignor.assignments().size(), is(2));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.current(), is(Set.of(0, 1, 2)));
        assertThat(assignment.desired(), is(Set.of(0, 1, 2, 6, 7)));
        assertThat(assignment.toBeAdded(), is(Set.of(6, 7)));
        assertThat(assignment.toBeRemoved(), is(Set.of()));

        assignment = assignor.assignmentForPool("my-pool2");
        assertThat(assignment.current(), is(Set.of(3, 4, 5)));
        assertThat(assignment.desired(), is(Set.of(3, 4, 5, 8, 9)));
        assertThat(assignment.toBeAdded(), is(Set.of(8, 9)));
        assertThat(assignment.toBeRemoved(), is(Set.of()));
    }

    @Test
    public void testExistingMultiPoolClusterScaleUpWithNextIds()  {
        NodeIdAssignor assignor = new NodeIdAssignor(
                Reconciliation.DUMMY_RECONCILIATION, createPool("my-pool", 5, Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[1000-1999]"), List.of(0, 1, 2)),
                createPool("my-pool2", 5, Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[2000-2999]"), List.of(3, 4, 5))
        );

        assertThat(assignor.assignments().size(), is(2));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.current(), is(Set.of(0, 1, 2)));
        assertThat(assignment.desired(), is(Set.of(0, 1, 2, 1000, 1001)));
        assertThat(assignment.toBeAdded(), is(Set.of(1000, 1001)));
        assertThat(assignment.toBeRemoved(), is(Set.of()));

        assignment = assignor.assignmentForPool("my-pool2");
        assertThat(assignment.current(), is(Set.of(3, 4, 5)));
        assertThat(assignment.desired(), is(Set.of(3, 4, 5, 2000, 2001)));
        assertThat(assignment.toBeAdded(), is(Set.of(2000, 2001)));
        assertThat(assignment.toBeRemoved(), is(Set.of()));
    }

    @Test
    public void testExistingMultiPoolClusterScaleDown()  {
        NodeIdAssignor assignor = new NodeIdAssignor(
                Reconciliation.DUMMY_RECONCILIATION, createPool("my-pool", 3, null, List.of(0, 1, 2, 3, 4)),
                createPool("my-pool2", 3, null, List.of(5, 6, 7, 8, 9))
        );

        assertThat(assignor.assignments().size(), is(2));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.current(), is(Set.of(0, 1, 2, 3, 4)));
        assertThat(assignment.desired(), is(Set.of(0, 1, 2)));
        assertThat(assignment.toBeAdded(), is(Set.of()));
        assertThat(assignment.toBeRemoved(), is(Set.of(3, 4)));

        assignment = assignor.assignmentForPool("my-pool2");
        assertThat(assignment.current(), is(Set.of(5, 6, 7, 8, 9)));
        assertThat(assignment.desired(), is(Set.of(5, 6, 7)));
        assertThat(assignment.toBeAdded(), is(Set.of()));
        assertThat(assignment.toBeRemoved(), is(Set.of(8, 9)));
    }

    @Test
    public void testExistingMultiPoolClusterScaleDownWithRemoveIds()  {
        NodeIdAssignor assignor = new NodeIdAssignor(
                Reconciliation.DUMMY_RECONCILIATION, createPool("my-pool", 3, Map.of(Annotations.ANNO_STRIMZI_IO_REMOVE_NODE_IDS, "[3, 2, 1]"), List.of(0, 1, 2, 3, 4)),
                createPool("my-pool2", 3, Map.of(Annotations.ANNO_STRIMZI_IO_REMOVE_NODE_IDS, "[6-8]"), List.of(5, 6, 7, 8, 9))
        );

        assertThat(assignor.assignments().size(), is(2));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.current(), is(Set.of(0, 1, 2, 3, 4)));
        assertThat(assignment.desired(), is(Set.of(0, 1, 4)));
        assertThat(assignment.toBeAdded(), is(Set.of()));
        assertThat(assignment.toBeRemoved(), is(Set.of(3, 2)));

        assignment = assignor.assignmentForPool("my-pool2");
        assertThat(assignment.current(), is(Set.of(5, 6, 7, 8, 9)));
        assertThat(assignment.desired(), is(Set.of(5, 8, 9)));
        assertThat(assignment.toBeAdded(), is(Set.of()));
        assertThat(assignment.toBeRemoved(), is(Set.of(6, 7)));
    }

    @Test
    public void testExistingMultiPoolClusterScaleDownUp()  {
        NodeIdAssignor assignor = new NodeIdAssignor(
                Reconciliation.DUMMY_RECONCILIATION, createPool("my-pool", 3, null, List.of(0, 1, 2, 3, 4)),
                createPool("my-pool2", 5, null, List.of(5, 6, 7))
        );

        assertThat(assignor.assignments().size(), is(2));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.current(), is(Set.of(0, 1, 2, 3, 4)));
        assertThat(assignment.desired(), is(Set.of(0, 1, 2)));
        assertThat(assignment.toBeAdded(), is(Set.of()));
        assertThat(assignment.toBeRemoved(), is(Set.of(3, 4)));

        assignment = assignor.assignmentForPool("my-pool2");
        assertThat(assignment.current(), is(Set.of(5, 6, 7)));
        assertThat(assignment.desired(), is(Set.of(5, 6, 7, 8, 9)));
        assertThat(assignment.toBeAdded(), is(Set.of(8, 9)));
        assertThat(assignment.toBeRemoved(), is(Set.of()));
    }

    @Test
    public void testExistingMultiPoolClusterScaleUpDown()  {
        NodeIdAssignor assignor = new NodeIdAssignor(
                Reconciliation.DUMMY_RECONCILIATION, createPool("my-pool", 5, null, List.of(0, 1, 2)),
                createPool("my-pool2", 3, null, List.of(3, 4, 5, 6, 7))
        );

        assertThat(assignor.assignments().size(), is(2));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.current(), is(Set.of(0, 1, 2)));
        assertThat(assignment.desired(), is(Set.of(0, 1, 2, 8, 9)));
        assertThat(assignment.toBeAdded(), is(Set.of(8, 9)));
        assertThat(assignment.toBeRemoved(), is(Set.of()));

        assignment = assignor.assignmentForPool("my-pool2");
        assertThat(assignment.current(), is(Set.of(3, 4, 5, 6, 7)));
        assertThat(assignment.desired(), is(Set.of(3, 4, 5)));
        assertThat(assignment.toBeAdded(), is(Set.of()));
        assertThat(assignment.toBeRemoved(), is(Set.of(6, 7)));
    }

    @Test
    public void testExistingMultiPoolClusterScaleUpDownWithNodeIds()  {
        NodeIdAssignor assignor = new NodeIdAssignor(
                Reconciliation.DUMMY_RECONCILIATION, createPool("my-pool", 5, Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[1000-1999]"), List.of(0, 1, 2)),
                createPool("my-pool2", 3, Map.of(Annotations.ANNO_STRIMZI_IO_REMOVE_NODE_IDS, "[5-8]"), List.of(3, 4, 5, 6, 7))
        );

        assertThat(assignor.assignments().size(), is(2));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.current(), is(Set.of(0, 1, 2)));
        assertThat(assignment.desired(), is(Set.of(0, 1, 2, 1000, 1001)));
        assertThat(assignment.toBeAdded(), is(Set.of(1000, 1001)));
        assertThat(assignment.toBeRemoved(), is(Set.of()));

        assignment = assignor.assignmentForPool("my-pool2");
        assertThat(assignment.current(), is(Set.of(3, 4, 5, 6, 7)));
        assertThat(assignment.desired(), is(Set.of(3, 4, 7)));
        assertThat(assignment.toBeAdded(), is(Set.of()));
        assertThat(assignment.toBeRemoved(), is(Set.of(5, 6)));
    }

    @Test
    public void testDuplicateIds()  {
        InvalidConfigurationException ex = assertThrows(InvalidConfigurationException.class, () -> new NodeIdAssignor(
                Reconciliation.DUMMY_RECONCILIATION, createPool("my-pool", 3, null, List.of(0, 1, 2)),
                createPool("my-pool2", 3, null, List.of(2, 3, 4))
        ));

        assertThat(ex.getMessage(), is("Found duplicate node ID 2 in node pool my-pool2"));
    }

    @Test
    public void testFindingIdsInGaps() {
        assertThat(NodeIdAssignor.findNextUnusedIdFromGap(new TreeSet<>(Set.of(0, 2))), is(1));
        assertThat(NodeIdAssignor.findNextUnusedIdFromGap(new TreeSet<>(Set.of(0, 2, 3, 4, 5))), is(1));
        assertThat(NodeIdAssignor.findNextUnusedIdFromGap(new TreeSet<>(Set.of(0, 1, 3, 4, 5))), is(2));
        assertThat(NodeIdAssignor.findNextUnusedIdFromGap(new TreeSet<>(Set.of(0, 1, 2, 4, 5))), is(3));
        assertThat(NodeIdAssignor.findNextUnusedIdFromGap(new TreeSet<>(Set.of(0, 1, 2, 3, 5))), is(4));
        assertThat(NodeIdAssignor.findNextUnusedIdFromGap(new TreeSet<>(Set.of(0, 3, 4, 5))), is(1));
        assertThat(NodeIdAssignor.findNextUnusedIdFromGap(new TreeSet<>(Set.of(0, 4, 5))), is(1));
        assertThat(NodeIdAssignor.findNextUnusedIdFromGap(new TreeSet<>(Set.of(0, 5))), is(1));
    }

    //////////////////////////////////////////////////
    // Utility methods
    //////////////////////////////////////////////////

    private static KafkaNodePool createPool(String name, int replicas, Map<String, String> annotations) {
        return new KafkaNodePoolBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withAnnotations(annotations)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(replicas)
                .endSpec()
                .build();
    }

    private static KafkaNodePool createPool(String name, int replicas, Map<String, String> annotations, List<Integer> assignedIds) {
        return new KafkaNodePoolBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withAnnotations(annotations)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(replicas)
                .endSpec()
                .withNewStatus()
                    .withNodeIds(assignedIds)
                .endStatus()
                .build();
    }
}
