/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicBuilder;
import io.strimzi.api.kafka.model.topic.KafkaTopicStatusBuilder;
import io.strimzi.api.kafka.model.topic.ReplicasChangeState;
import io.strimzi.api.kafka.model.topic.ReplicasChangeStatusBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.topic.cruisecontrol.CruiseControlHandler;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsHolder;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsProvider;
import io.strimzi.operator.topic.model.Either;
import io.strimzi.operator.topic.model.Pair;
import io.strimzi.operator.topic.model.PartitionedByError;
import io.strimzi.operator.topic.model.ReconcilableTopic;
import io.strimzi.operator.topic.model.Results;
import io.strimzi.operator.topic.model.TopicOperatorException;
import io.strimzi.operator.topic.model.TopicState;
import io.strimzi.test.container.StrimziKafkaCluster;
import io.strimzi.test.interfaces.TestSeparator;
import io.strimzi.test.mockkube3.MockKube3;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;

class ReplicasChangeIT implements TestSeparator {
    private static final String NAMESPACE = TestUtil.namespaceName(ReplicasChangeIT.class);

    private static MockKube3 mockKube;
    private static KubernetesClient kubernetesClient;

    private StrimziKafkaCluster kafkaCluster;
    private Admin kafkaAdminClient;

    @BeforeAll
    public static void beforeAll() {
        mockKube = new MockKube3.MockKube3Builder()
            .withKafkaTopicCrd()
            .withDeletionController()
            .withNamespaces(NAMESPACE)
            .build();
        mockKube.start();
        kubernetesClient = mockKube.client();
    }

    @AfterAll
    public static void afterAll() {
        mockKube.stop();
    }

    @BeforeEach
    public void beforeEach() {
        kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withKraft()
                .withNumberOfBrokers(1)
                .withInternalTopicReplicationFactor(1)
                .withSharedNetwork()
                .build();
        kafkaCluster.start();
        kafkaAdminClient = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers()));
    }

    @AfterEach
    public void afterEach() {
        TestUtil.cleanupNamespace(kubernetesClient, NAMESPACE);
        kafkaAdminClient.close();
        kafkaCluster.stop();
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void replicasChangeShouldBeReconciled(boolean cruiseControlEnabled) {
        var topicName = "my-topic";
        var replicationFactor = 1;

        // setup: .spec.replicas != replicationFactor
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
            TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
            TopicOperatorConfig.USE_FINALIZERS.key(), "true",
            TopicOperatorConfig.ENABLE_ADDITIONAL_METRICS.key(), "false",
            TopicOperatorConfig.CRUISE_CONTROL_ENABLED.key(), Boolean.toString(cruiseControlEnabled)
        ));

        var describeClusterResult = Mockito.mock(DescribeClusterResult.class);
        Mockito.doReturn(KafkaFuture.completedFuture(List.of())).when(describeClusterResult).nodes();

        var partitionReassignmentResult = Mockito.mock(ListPartitionReassignmentsResult.class);
        var topicPartition = Mockito.mock(TopicPartition.class);
        var partitionReassignment = Mockito.mock(PartitionReassignment.class);
        Mockito.doReturn(KafkaFuture.completedFuture(Map.of(topicPartition, partitionReassignment)))
            .when(partitionReassignmentResult).reassignments();

        var kafkaAdmin = Mockito.mock(Admin.class);
        Mockito.doReturn(describeClusterResult).when(kafkaAdmin).describeCluster();
        Mockito.doReturn(partitionReassignmentResult).when(kafkaAdmin).listPartitionReassignments(any(Set.class));
        
        var topicDescription = Mockito.mock(TopicDescription.class);
        var topicPartitionInfo = Mockito.mock(TopicPartitionInfo.class);
        Mockito.doReturn(List.of(topicPartitionInfo)).when(topicDescription).partitions();
        
        var currentState = Mockito.mock(TopicState.class);
        Mockito.doReturn(replicationFactor).when(currentState).uniqueReplicationFactor();
        Mockito.doReturn(topicDescription).when(currentState).description();
        
        var kafkaTopic = new KafkaTopicBuilder()
            .withNewMetadata()
                .withName(topicName)
                .withNamespace(NAMESPACE)
                .addToLabels("key", "VALUE")
            .endMetadata()
            .withNewSpec()
                // we also support string values
                .withConfig(Map.of("min.insync.replicas", "1"))
                .withPartitions(25)
                .withReplicas(++replicationFactor)
            .endSpec()
            .build();
        var reconcilableTopic = new ReconcilableTopic(
            new Reconciliation("test", KafkaTopic.RESOURCE_KIND, NAMESPACE, topicName), kafkaTopic, topicName);
        
        var currentStatesOrError = new PartitionedByError<>(
            List.of(new Pair<>(reconcilableTopic, Either.ofRight(currentState))), List.of());

        var replicaChangeStatus = 
            new ReplicasChangeStatusBuilder()
                .withState(ReplicasChangeState.PENDING)
                .withTargetReplicas(replicationFactor)
                .build();
        
        var pendingResults = new Results();
        pendingResults.addRightResults(List.of(reconcilableTopic));
        pendingResults.addReplicasChange(reconcilableTopic, replicaChangeStatus);

        var cruiseControlHandler = Mockito.mock(CruiseControlHandler.class);
        Mockito.doReturn(pendingResults).when(cruiseControlHandler).requestPendingChanges(anyList());
        Mockito.doReturn(new Results()).when(cruiseControlHandler).requestOngoingChanges(anyList());
        Mockito.doReturn(new Results()).when(cruiseControlHandler).completeZombieChanges(anyList());
        
        // run test
        var metricsHolder = new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry()));
        var controller = new BatchingTopicController(config, Map.of("key", "VALUE"),
            new KubernetesHandler(config, metricsHolder, kubernetesClient),
            new KafkaHandler(config, metricsHolder, kafkaAdmin), metricsHolder,
            cruiseControlHandler);
        var results = controller.checkReplicasChanges(currentStatesOrError.ok(), List.of(reconcilableTopic));

        if (cruiseControlEnabled) {
            assertThat(results.size(), is(1));
            assertThat(results.getReplicasChanges().get(reconcilableTopic), is(replicaChangeStatus));
        } else {
            assertThat(results.size(), is(1));
            results.forEachLeftResult((rt, e) -> assertThat(e, instanceOf(TopicOperatorException.NotSupported.class)));
        }
    }
    
    @Test
    public void replicasChangeShouldCompleteWhenSpecIsReverted() {
        var topicName = "my-topic";
        int replicationFactor = 3;

        // setup: pending with error and .spec.replicas == replicationFactor
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
            TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
            TopicOperatorConfig.USE_FINALIZERS.key(), "true",
            TopicOperatorConfig.ENABLE_ADDITIONAL_METRICS.key(), "false",
            TopicOperatorConfig.CRUISE_CONTROL_ENABLED.key(), "true"
        ));

        var describeClusterResult = Mockito.mock(DescribeClusterResult.class);
        Mockito.doReturn(KafkaFuture.completedFuture(List.of())).when(describeClusterResult).nodes();

        var partitionReassignmentResult = Mockito.mock(ListPartitionReassignmentsResult.class);
        var topicPartition = Mockito.mock(TopicPartition.class);
        var partitionReassignment = Mockito.mock(PartitionReassignment.class);
        Mockito.doReturn(KafkaFuture.completedFuture(Map.of(topicPartition, partitionReassignment)))
            .when(partitionReassignmentResult).reassignments();

        var kafkaAdmin = Mockito.mock(Admin.class);
        Mockito.doReturn(describeClusterResult).when(kafkaAdmin).describeCluster();
        Mockito.doReturn(partitionReassignmentResult).when(kafkaAdmin).listPartitionReassignments(any(Set.class));

        var topicDescription = Mockito.mock(TopicDescription.class);
        var topicPartitionInfo = Mockito.mock(TopicPartitionInfo.class);
        Mockito.doReturn(List.of(topicPartitionInfo)).when(topicDescription).partitions();

        var currentState = Mockito.mock(TopicState.class);
        Mockito.doReturn(replicationFactor).when(currentState).uniqueReplicationFactor();
        Mockito.doReturn(topicDescription).when(currentState).description();
        
        var kafkaTopic = new KafkaTopicBuilder()
            .withNewMetadata()
                .withName(topicName)
                .withNamespace(NAMESPACE)
                .addToLabels("key", "VALUE")
            .endMetadata()
            .withNewSpec()
                .withPartitions(25)
                .withReplicas(replicationFactor)
            .endSpec()
            .withStatus(new KafkaTopicStatusBuilder()
                .withReplicasChange(new ReplicasChangeStatusBuilder()
                        .withMessage("Error message")
                        .withState(ReplicasChangeState.PENDING)
                        .withTargetReplicas(replicationFactor)
                    .build())
                .build())
            .build();

        var reconcilableTopic = new ReconcilableTopic(
            new Reconciliation("test", KafkaTopic.RESOURCE_KIND, NAMESPACE, topicName), kafkaTopic, topicName);

        var completedResults = new Results();
        completedResults.addRightResults(List.of(reconcilableTopic));
        completedResults.addReplicasChange(reconcilableTopic, null);

        var cruiseControlHandler = Mockito.mock(CruiseControlHandler.class);
        Mockito.doReturn(new Results()).when(cruiseControlHandler).requestPendingChanges(anyList());
        Mockito.doReturn(new Results()).when(cruiseControlHandler).requestOngoingChanges(anyList());
        Mockito.doReturn(completedResults).when(cruiseControlHandler).completeZombieChanges(anyList());
        
        var reconcilableTopics = List.of(reconcilableTopic);
        PartitionedByError<ReconcilableTopic, TopicState> currentStatesOrError = new PartitionedByError<>(List.of(), List.of());
        
        // run test
        var metricsHolder = new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry()));
        var controller = new BatchingTopicController(config, Map.of("key", "VALUE"),
            new KubernetesHandler(config, metricsHolder, kubernetesClient),
            new KafkaHandler(config, metricsHolder, kafkaAdmin), metricsHolder,
            cruiseControlHandler);
        var results = controller.checkReplicasChanges(currentStatesOrError.ok(), reconcilableTopics);

        assertThat(results.size(), is(1));
        assertThat(results.getReplicasChanges().get(reconcilableTopic), is(nullValue()));
    }

    @Test
    public void replicasChangeShouldCompleteWhenCruiseControlRestarts() {
        var topicName = "my-topic";
        var replicationFactor = 1;

        // setup: pending with .spec.replicas == replicationFactor
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
            TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
            TopicOperatorConfig.USE_FINALIZERS.key(), "true",
            TopicOperatorConfig.ENABLE_ADDITIONAL_METRICS.key(), "false",
            TopicOperatorConfig.CRUISE_CONTROL_ENABLED.key(), "true"
        ));

        var describeClusterResult = Mockito.mock(DescribeClusterResult.class);
        Mockito.doReturn(KafkaFuture.completedFuture(List.of())).when(describeClusterResult).nodes();

        var partitionReassignmentResult = Mockito.mock(ListPartitionReassignmentsResult.class);
        var topicPartition = Mockito.mock(TopicPartition.class);
        var partitionReassignment = Mockito.mock(PartitionReassignment.class);
        Mockito.doReturn(KafkaFuture.completedFuture(Map.of(topicPartition, partitionReassignment)))
            .when(partitionReassignmentResult).reassignments();

        var kafkaAdmin = Mockito.mock(Admin.class);
        Mockito.doReturn(describeClusterResult).when(kafkaAdmin).describeCluster();
        Mockito.doReturn(partitionReassignmentResult).when(kafkaAdmin).listPartitionReassignments(any(Set.class));

        var topicDescription = Mockito.mock(TopicDescription.class);
        var topicPartitionInfo = Mockito.mock(TopicPartitionInfo.class);
        Mockito.doReturn(List.of(topicPartitionInfo)).when(topicDescription).partitions();

        var currentState = Mockito.mock(TopicState.class);
        Mockito.doReturn(replicationFactor).when(currentState).uniqueReplicationFactor();
        Mockito.doReturn(topicDescription).when(currentState).description();

        var kafkaTopic = new KafkaTopicBuilder()
            .withNewMetadata()
                .withName(topicName)
                .withNamespace(NAMESPACE)
                .addToLabels("key", "VALUE")
            .endMetadata()
            .withNewSpec()
                .withPartitions(25)
                .withReplicas(replicationFactor)
                .endSpec()
            .withStatus(new KafkaTopicStatusBuilder()
                .withReplicasChange(new ReplicasChangeStatusBuilder()
                        .withState(ReplicasChangeState.PENDING)
                        .withTargetReplicas(replicationFactor)
                    .build())
                .build())
            .build();

        var reconcilableTopic = new ReconcilableTopic(
            new Reconciliation("test", KafkaTopic.RESOURCE_KIND, NAMESPACE, topicName), kafkaTopic, topicName);

        var completedResults = new Results();
        completedResults.addRightResults(List.of(reconcilableTopic));
        completedResults.addReplicasChange(reconcilableTopic, null);

        var cruiseControlHandler = Mockito.mock(CruiseControlHandler.class);
        Mockito.doReturn(new Results()).when(cruiseControlHandler).requestPendingChanges(anyList());
        Mockito.doReturn(new Results()).when(cruiseControlHandler).requestOngoingChanges(anyList());
        Mockito.doReturn(completedResults).when(cruiseControlHandler).completeZombieChanges(anyList());

        var reconcilableTopics = List.of(reconcilableTopic);
        PartitionedByError<ReconcilableTopic, TopicState> currentStatesOrError = new PartitionedByError<>(List.of(), List.of());

        // run test
        var metricsHolder = new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry()));
        var controller = new BatchingTopicController(config, Map.of("key", "VALUE"),
            new KubernetesHandler(config, metricsHolder, kubernetesClient),
            new KafkaHandler(config, metricsHolder, kafkaAdmin), metricsHolder,
            cruiseControlHandler);
        var results = controller.checkReplicasChanges(currentStatesOrError.ok(), reconcilableTopics);
        
        assertThat(results.size(), is(1));
        assertThat(results.getReplicasChanges().get(reconcilableTopic), is(nullValue()));
    }
}
