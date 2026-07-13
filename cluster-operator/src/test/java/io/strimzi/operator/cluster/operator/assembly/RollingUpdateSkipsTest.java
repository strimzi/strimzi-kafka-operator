/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class RollingUpdateSkipsTest {
    private final static Reconciliation RECONCILIATION = Reconciliation.DUMMY_RECONCILIATION;

    private final static Set<NodeRef> NODES = Set.of(
            new NodeRef("my-cluster-controllers-0", 0, "controllers", true, false),
            new NodeRef("my-cluster-controllers-1", 1, "controllers", true, false),
            new NodeRef("my-cluster-controllers-2", 2, "controllers", true, false),
            new NodeRef("my-cluster-brokers-10", 10, "brokers", false, true),
            new NodeRef("my-cluster-brokers-11", 11, "brokers", false, true),
            new NodeRef("my-cluster-brokers-12", 12, "brokers", false, true)
    );

    private static KafkaNodePool pool(String name, String skipAnnotation, ProcessRoles... roles)   {
        KafkaNodePoolBuilder builder = new KafkaNodePoolBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace("my-namespace")
                .endMetadata()
                .withNewSpec()
                    .withReplicas(3)
                    .withRoles(roles)
                .endSpec();

        if (skipAnnotation != null) {
            builder.editMetadata().withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_SKIP_ROLLING_UPDATE, skipAnnotation)).endMetadata();
        }

        return builder.build();
    }

    @Test
    public void testNoAnnotationsMeansNoSkips()   {
        RollingUpdateSkips skips = RollingUpdateSkips.resolve(
                RECONCILIATION,
                List.of(pool("controllers", null, ProcessRoles.CONTROLLER), pool("brokers", null, ProcessRoles.BROKER)),
                NODES,
                List.of()
        );

        assertThat(skips.hasSkippedNodes(), is(false));
        assertThat(skips.hasRejectedNodeIds(), is(false));
        assertThat(skips.hasIgnoredControllerNodeIds(), is(false));
        assertThat(skips.skippedNodeIds(), is(Set.of()));
    }

    @Test
    public void testSkipsSingleIdsAndRanges()   {
        RollingUpdateSkips skips = RollingUpdateSkips.resolve(
                RECONCILIATION,
                List.of(pool("controllers", null, ProcessRoles.CONTROLLER), pool("brokers", "[10,11-12]", ProcessRoles.BROKER)),
                NODES,
                List.of()
        );

        assertThat(skips.hasSkippedNodes(), is(true));
        assertThat(skips.skippedNodeIds(), is(Set.of(10, 11, 12)));
        assertThat(skips.skippedNodeIdsByPool(), is(Map.of("brokers", Set.of(10, 11, 12))));
        assertThat(skips.ignoredControllerNodeIds(), is(Set.of()));
        assertThat(skips.rejectedNodeIds(), is(List.of()));
    }

    @Test
    public void testIdsNotBelongingToThePoolAreRejected()   {
        RollingUpdateSkips skips = RollingUpdateSkips.resolve(
                RECONCILIATION,
                List.of(pool("controllers", null, ProcessRoles.CONTROLLER), pool("brokers", "[10,99]", ProcessRoles.BROKER)),
                NODES,
                List.of()
        );

        assertThat(skips.skippedNodeIds(), is(Set.of(10)));
        assertThat(skips.rejectedNodeIds(), is(List.of("brokers: 99")));
    }

    @Test
    public void testInvalidEntriesAreRejectedIndividually()   {
        RollingUpdateSkips skips = RollingUpdateSkips.resolve(
                RECONCILIATION,
                List.of(pool("brokers", "[10,abc,-5,11]", ProcessRoles.BROKER)),
                NODES,
                List.of()
        );

        assertThat(skips.skippedNodeIds(), is(Set.of(10, 11)));
        assertThat(skips.rejectedNodeIds(), hasItems("brokers: abc", "brokers: -5"));
    }

    @Test
    public void testCompletelyInvalidAnnotationIsRejected()   {
        RollingUpdateSkips skips = RollingUpdateSkips.resolve(
                RECONCILIATION,
                List.of(pool("brokers", "10,11", ProcessRoles.BROKER)),
                NODES,
                List.of()
        );

        assertThat(skips.hasSkippedNodes(), is(false));
        assertThat(skips.hasRejectedNodeIds(), is(true));
        assertThat(skips.rejectedNodeIds(), is(List.of("brokers: 10,11")));
    }

    @Test
    public void testEmptyAnnotationMeansNoSkips()   {
        RollingUpdateSkips skips = RollingUpdateSkips.resolve(
                RECONCILIATION,
                List.of(pool("brokers", "[]", ProcessRoles.BROKER)),
                NODES,
                List.of()
        );

        assertThat(skips.hasSkippedNodes(), is(false));
        assertThat(skips.hasRejectedNodeIds(), is(false));
        assertThat(skips.hasIgnoredControllerNodeIds(), is(false));
    }

    @Test
    public void testControllerNodesAreIgnored()   {
        RollingUpdateSkips skips = RollingUpdateSkips.resolve(
                RECONCILIATION,
                List.of(pool("controllers", "[0,1]", ProcessRoles.CONTROLLER), pool("brokers", "[10]", ProcessRoles.BROKER)),
                NODES,
                List.of()
        );

        assertThat(skips.skippedNodeIds(), is(Set.of(10)));
        assertThat(skips.ignoredControllerNodeIds(), is(Set.of(0, 1)));
    }

    @Test
    public void testCombinedNodesAreIgnored()   {
        Set<NodeRef> combinedNodes = Set.of(
                new NodeRef("my-cluster-combined-0", 0, "combined", true, true),
                new NodeRef("my-cluster-combined-1", 1, "combined", true, true),
                new NodeRef("my-cluster-combined-2", 2, "combined", true, true)
        );

        RollingUpdateSkips skips = RollingUpdateSkips.resolve(
                RECONCILIATION,
                List.of(pool("combined", "[0]", ProcessRoles.CONTROLLER, ProcessRoles.BROKER)),
                combinedNodes,
                List.of()
        );

        assertThat(skips.hasSkippedNodes(), is(false));
        assertThat(skips.ignoredControllerNodeIds(), is(Set.of(0)));
    }

    @Test
    public void testPodClaimingControllerRoleIsIgnored()   {
        // The desired role is broker-only, but the existing Pod still claims the controller role (e.g. during a role
        // transition), so the skip must not be honored
        Pod transitioningPod = new PodBuilder()
                .withNewMetadata()
                    .withName("my-cluster-brokers-10")
                    .withLabels(Map.of(Labels.STRIMZI_CONTROLLER_ROLE_LABEL, "true"))
                .endMetadata()
                .build();

        RollingUpdateSkips skips = RollingUpdateSkips.resolve(
                RECONCILIATION,
                List.of(pool("brokers", "[10,11]", ProcessRoles.BROKER)),
                NODES,
                List.of(transitioningPod)
        );

        assertThat(skips.skippedNodeIds(), is(Set.of(11)));
        assertThat(skips.ignoredControllerNodeIds(), is(Set.of(10)));
    }

    @Test
    public void testConditionAndRoundTrip()   {
        RollingUpdateSkips skips = RollingUpdateSkips.resolve(
                RECONCILIATION,
                List.of(pool("controllers", "[0]", ProcessRoles.CONTROLLER), pool("brokers", "[10,12,99]", ProcessRoles.BROKER)),
                NODES,
                List.of()
        );

        Condition condition = skips.kafkaStatusCondition();

        assertThat(condition.getType(), is("RollingUpdateSkipped"));
        assertThat(condition.getStatus(), is("True"));
        assertThat(condition.getMessage(), containsString("Nodes [10, 12] are excluded from automatic rolling updates"));

        // The ignored and rejected node IDs are reported through separate warning conditions
        Condition ignoredCondition = skips.ignoredControllersWarningCondition();
        assertThat(ignoredCondition.getType(), is("Warning"));
        assertThat(ignoredCondition.getReason(), is("SkipRollingUpdateControllersIgnored"));
        assertThat(ignoredCondition.getMessage(), containsString("[0]"));

        Condition rejectedCondition = skips.rejectedNodeIdsWarningCondition();
        assertThat(rejectedCondition.getType(), is("Warning"));
        assertThat(rejectedCondition.getReason(), is("InvalidSkipRollingUpdateAnnotation"));
        assertThat(rejectedCondition.getMessage(), containsString("[brokers: 99]"));

        // The skipped node IDs can be extracted back from the condition of the previous reconciliation
        assertThat(RollingUpdateSkips.skippedNodeIdsFromCondition(condition), is(Set.of(10, 12)));
        assertThat(RollingUpdateSkips.skippedNodeIdsFromCondition(null), is(Set.of()));
    }
}
