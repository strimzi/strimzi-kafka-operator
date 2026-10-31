/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.api.kafka.model.topic.KafkaTopicBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsHolder;
import io.strimzi.operator.topic.model.Pair;
import io.strimzi.operator.topic.model.ReconcilableTopic;
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

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaHandlerTest {
    @Test
    void shouldAccountForRemovingReplicasWhenCalculatingTargetRf() {
        var topicName = "my-topic";
        var tp = new TopicPartition(topicName, 0);

        // The KafkaTopic still requests RF=2, while the observed assignment
        // has RF=1 during the reassignment
        var kafkaTopic = new KafkaTopicBuilder()
                .withNewMetadata()
                    .withNamespace("test")
                    .withName(topicName)
                .endMetadata()
                .withNewSpec()
                    .withPartitions(1)
                    .withReplicas(2)
                .endSpec()
                .build();

        var reconcilableTopic = new ReconcilableTopic(
                Reconciliation.DUMMY_RECONCILIATION,
                kafkaTopic,
                topicName
        );

        var node = new Node(0, "localhost", 9092);
        var partitionInfo = new TopicPartitionInfo(
                0,
                node,
                List.of(node),
                List.of(node)
        );
        var topicDescription = new TopicDescription(
                topicName,
                false,
                List.of(partitionInfo)
        );
        var topicState = new TopicState(topicDescription, null);

        // Kafka reports both replicas in the reassignment's replica list,
        // but replica 1 is being removed. The target RF is therefore 1.
        var reassignment = new PartitionReassignment(
                List.of(0, 1), // replicas
                List.of(),     // addingReplicas
                List.of(1)     // removingReplicas
        );

        var admin = mock(Admin.class);
        var reassignmentsResult = mock(ListPartitionReassignmentsResult.class);

        when(admin.listPartitionReassignments(Set.of(tp)))
                .thenReturn(reassignmentsResult);
        when(reassignmentsResult.reassignments())
                .thenReturn(KafkaFuture.completedFuture(Map.of(tp, reassignment)));

        var config = mock(TopicOperatorConfig.class);
        when(config.enableAdditionalMetrics()).thenReturn(false);
        when(config.watchedNamespace()).thenReturn("test");

        var handler = new KafkaHandler(
                config,
                mock(TopicOperatorMetricsHolder.class),
                admin
        );

        // The handler evaluates whether the reassignment accounts
        // for the observed replication-factor mismatch
        var result = handler.filterByReassignmentTargetReplicas(
                List.of(new Pair<>(reconcilableTopic, topicState))
        );

        // Target RF=1 still differs from desired RF=2, so the topic
        // must remain in the result for unsupported-RF handling
        assertEquals(1, result.size());
        assertEquals(reconcilableTopic, result.get(0).getKey());
        assertTrue(result.get(0).getValue().isRight());
    }
}
