/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.nodepools;

import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.operator.common.InvalidConfigurationException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class NodeIdAssignorTest {
    @Test
    public void testNewSinglePoolCluster()  {
        NodeIdAssignor assignor = new NodeIdAssignor(createPool("my-pool", 3));

        assertThat(assignor.assignments().size(), is(1));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.current(), is(Set.of()));
        assertThat(assignment.desired(), is(Set.of(0, 1, 2)));
        assertThat(assignment.toBeAdded(), is(Set.of(0, 1, 2)));
        assertThat(assignment.toBeRemoved(), is(Set.of()));
    }

    @Test
    public void testExistingSinglePoolClusterWithoutChange()  {
        NodeIdAssignor assignor = new NodeIdAssignor(createPool("my-pool", 3, List.of(0, 1, 2)));

        assertThat(assignor.assignments().size(), is(1));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.current(), is(Set.of(0, 1, 2)));
        assertThat(assignment.desired(), is(Set.of(0, 1, 2)));
        assertThat(assignment.toBeAdded(), is(Set.of()));
        assertThat(assignment.toBeRemoved(), is(Set.of()));
    }

    @Test
    public void testExistingSinglePoolClusterOutOfSequenceWithoutChange()  {
        NodeIdAssignor assignor = new NodeIdAssignor(createPool("my-pool", 3, List.of(2, 3, 5)));

        assertThat(assignor.assignments().size(), is(1));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.current(), is(Set.of(2, 3, 5)));
        assertThat(assignment.desired(), is(Set.of(2, 3, 5)));
        assertThat(assignment.toBeAdded(), is(Set.of()));
        assertThat(assignment.toBeRemoved(), is(Set.of()));
    }

    @Test
    public void testExistingSinglePoolClusterScaleDown()  {
        NodeIdAssignor assignor = new NodeIdAssignor(createPool("my-pool", 3, List.of(0, 1, 2, 3, 4)));

        assertThat(assignor.assignments().size(), is(1));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.current(), is(Set.of(0, 1, 2, 3, 4)));
        assertThat(assignment.desired(), is(Set.of(0, 1, 2)));
        assertThat(assignment.toBeAdded(), is(Set.of()));
        assertThat(assignment.toBeRemoved(), is(Set.of(3, 4)));
    }

    @Test
    public void testExistingSinglePoolClusterScaleUp()  {
        NodeIdAssignor assignor = new NodeIdAssignor(createPool("my-pool", 5, List.of(0, 1, 2)));

        assertThat(assignor.assignments().size(), is(1));

        NodeIdAssignment assignment = assignor.assignmentForPool("my-pool");
        assertThat(assignment.current(), is(Set.of(0, 1, 2)));
        assertThat(assignment.desired(), is(Set.of(0, 1, 2, 3, 4)));
        assertThat(assignment.toBeAdded(), is(Set.of(3, 4)));
        assertThat(assignment.toBeRemoved(), is(Set.of()));
    }

    @Test
    public void testNewMultiPoolCluster()  {
        NodeIdAssignor assignor = new NodeIdAssignor(
                createPool("my-pool", 3),
                createPool("my-pool2", 3)
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
    public void testExistingMultiPoolCluster()  {
        NodeIdAssignor assignor = new NodeIdAssignor(
                createPool("my-pool", 3, List.of(0, 1, 2)),
                createPool("my-pool2", 3, List.of(3, 4, 5))
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
                createPool("my-pool", 5, List.of(0, 1, 2)),
                createPool("my-pool2", 5, List.of(3, 4, 5))
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
    public void testExistingMultiPoolClusterScaleDown()  {
        NodeIdAssignor assignor = new NodeIdAssignor(
                createPool("my-pool", 3, List.of(0, 1, 2, 3, 4)),
                createPool("my-pool2", 3, List.of(5, 6, 7, 8, 9))
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
    public void testExistingMultiPoolClusterScaleDownUp()  {
        NodeIdAssignor assignor = new NodeIdAssignor(
                createPool("my-pool", 3, List.of(0, 1, 2, 3, 4)),
                createPool("my-pool2", 5, List.of(5, 6, 7))
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
                createPool("my-pool", 5, List.of(0, 1, 2)),
                createPool("my-pool2", 3, List.of(3, 4, 5, 6, 7))
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
    public void testDuplicateIds()  {
        InvalidConfigurationException ex = assertThrows(InvalidConfigurationException.class, () -> new NodeIdAssignor(
                createPool("my-pool", 3, List.of(0, 1, 2)),
                createPool("my-pool2", 3, List.of(2, 3, 4))
        ));

        assertThat(ex.getMessage(), is("Found duplicate node ID 2 in node pool my-pool2"));
    }

    //////////////////////////////////////////////////
    // Utility methods
    //////////////////////////////////////////////////

    private static KafkaNodePool createPool(String name, int replicas) {
        return new KafkaNodePoolBuilder()
                .withNewMetadata()
                    .withName(name)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(replicas)
                .endSpec()
                .build();
    }

    private static KafkaNodePool createPool(String name, int replicas, List<Integer> assignedIds) {
        return new KafkaNodePoolBuilder()
                .withNewMetadata()
                    .withName(name)
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
