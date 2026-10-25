/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicBuilder;
import io.strimzi.operator.topic.cruisecontrol.CruiseControlHandler;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsHolder;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsProvider;
import io.strimzi.operator.topic.model.Pair;
import io.strimzi.operator.topic.model.ReconcilableTopic;
import io.strimzi.operator.topic.model.Results;
import io.strimzi.operator.topic.model.TopicOperatorException;
import io.strimzi.operator.topic.model.TopicState;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class ReplicasChangeTest {
    private static final String TOPIC_NAME = "mixed-replicas";
    private static final String NAMESPACE = "test";
    private static final List<Node> BROKERS = List.of(
        new Node(0, "localhost", 9092), new Node(1, "localhost", 9093), new Node(2, "localhost", 9094));

    @ParameterizedTest(name = "{0}, Cruise Control={1}")
    @MethodSource("replicaStates")
    void shouldReconcileOnlyReplicaMismatches(ReplicaState state, boolean cruiseControlEnabled) {
        var fixture = fixture(state, cruiseControlEnabled);
        var results = fixture.controller().checkReplicasChanges(
            Stream.of(new Pair<>(fixture.topic(), fixture.state())), List.of(fixture.topic()));

        if (cruiseControlEnabled) {
            verify(fixture.cruiseControl()).requestPendingChanges(state.mismatch() ? List.of(fixture.topic()) : List.of());
        } else {
            assertThat(results.size(), is(state.mismatch() ? 1 : 0));
            results.forEachRightResult((topic, result) -> fail("Unexpected successful replica-change result"));
            results.forEachLeftResult((topic, error) -> {
                assertThat(topic, is(fixture.topic()));
                assertThat(error, instanceOf(TopicOperatorException.NotSupported.class));
            });
            verifyNoInteractions(fixture.cruiseControl());
        }
    }

    @Test
    void shouldIgnoreStablePartitionBesideReassignmentToDesiredReplicas() {
        var fixture = fixture(new ReplicaState("reassignment to desired", 2, List.of(2, 3),
            Map.of(1, new PartitionReassignment(List.of(0, 1, 2), List.of(2), List.of(1))), false), false);

        var mismatches = fixture.kafkaHandler().filterByReassignmentTargetReplicas(
            List.of(new Pair<>(fixture.topic(), fixture.state())));

        assertThat(mismatches, is(List.of()));
    }

    private static Stream<Arguments> replicaStates() {
        var states = List.of(
            new ReplicaState("mixed replica counts", 2, List.of(2, 3), Map.of(), true),
            new ReplicaState("matching replica counts", 2, List.of(2, 2), Map.of(), false),
            new ReplicaState("uniform wrong replica count", 2, List.of(3, 3), Map.of(), true),
            new ReplicaState("empty newly created topic metadata", 2, List.of(), Map.of(), false),
            new ReplicaState("unspecified replicas", null, List.of(2, 3), Map.of(), false),
            new ReplicaState("reassignment to desired", 2, List.of(2, 3),
                Map.of(1, new PartitionReassignment(List.of(0, 1, 2), List.of(2), List.of(1))), false),
            new ReplicaState("reassignment to wrong target", 2, List.of(2, 3),
                Map.of(1, new PartitionReassignment(List.of(0, 1, 2), List.of(2), List.of())), true),
            new ReplicaState("unreassigned mismatch beside valid reassignment", 2, List.of(3, 3),
                Map.of(1, new PartitionReassignment(List.of(0, 1, 2), List.of(2), List.of(1))), true)
        );
        return states.stream().flatMap(state -> Stream.of(Arguments.of(state, false), Arguments.of(state, true)));
    }

    private static Fixture fixture(ReplicaState state, boolean cruiseControlEnabled) {
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:9092",
            TopicOperatorConfig.WATCHED_NAMESPACE.key(), NAMESPACE,
            TopicOperatorConfig.ENABLE_ADDITIONAL_METRICS.key(), "false",
            TopicOperatorConfig.SKIP_CLUSTER_CONFIG_REVIEW.key(), "true",
            TopicOperatorConfig.CRUISE_CONTROL_ENABLED.key(), Boolean.toString(cruiseControlEnabled)
        ));
        var topic = new KafkaTopicBuilder()
            .withNewMetadata().withName(TOPIC_NAME).withNamespace(NAMESPACE).endMetadata()
            .withNewSpec().withPartitions(state.replicaCounts().size()).withReplicas(state.desiredReplicas()).endSpec()
            .build();
        var reconcilableTopic = TestUtil.reconcilableTopic(topic, NAMESPACE);
        var partitions = IntStream.range(0, state.replicaCounts().size())
            .mapToObj(index -> {
                var replicas = BROKERS.subList(0, state.replicaCounts().get(index));
                return new TopicPartitionInfo(index, replicas.get(0), replicas, replicas);
            }).toList();
        var topicState = new TopicState(new TopicDescription(TOPIC_NAME, false, partitions), null);

        var reassignments = state.reassignments().entrySet().stream().collect(Collectors.toMap(
            entry -> new TopicPartition(TOPIC_NAME, entry.getKey()), Map.Entry::getValue));
        var reassignmentResult = mock(ListPartitionReassignmentsResult.class);
        when(reassignmentResult.reassignments()).thenReturn(KafkaFuture.completedFuture(reassignments));
        var admin = mock(Admin.class);
        when(admin.listPartitionReassignments(anySet())).thenReturn(reassignmentResult);

        var cruiseControl = mock(CruiseControlHandler.class);
        when(cruiseControl.requestPendingChanges(anyList())).thenReturn(new Results());
        when(cruiseControl.requestOngoingChanges(anyList())).thenReturn(new Results());
        var metrics = new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null,
            new TopicOperatorMetricsProvider(new SimpleMeterRegistry()));
        var kafkaHandler = new KafkaHandler(config, metrics, admin);
        var controller = new BatchingTopicController(config, Map.of(), mock(KubernetesHandler.class),
            kafkaHandler, metrics, cruiseControl);
        return new Fixture(controller, kafkaHandler, cruiseControl, reconcilableTopic, topicState);
    }

    private record Fixture(BatchingTopicController controller, KafkaHandler kafkaHandler,
                           CruiseControlHandler cruiseControl, ReconcilableTopic topic, TopicState state) { }

    private record ReplicaState(String name, Integer desiredReplicas, List<Integer> replicaCounts,
                                Map<Integer, PartitionReassignment> reassignments, boolean mismatch) {
        @Override
        public String toString() {
            return name;
        }
    }
}
