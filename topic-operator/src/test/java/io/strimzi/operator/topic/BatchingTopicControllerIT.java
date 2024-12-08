/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.strimzi.api.kafka.Crds;
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
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verifyNoInteractions;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
class BatchingTopicControllerIT implements TestSeparator {
    private static final String NAMESPACE = TopicOperatorTestUtil.namespaceName(BatchingTopicControllerIT.class);

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
        TopicOperatorTestUtil.cleanupNamespace(kubernetesClient, NAMESPACE);

        kafkaAdminClient.close();
        kafkaCluster.stop();
    }

    private static <T> KafkaFuture<T> interruptedFuture() throws ExecutionException, InterruptedException {
        var future = Mockito.mock(KafkaFuture.class);
        doThrow(new InterruptedException()).when(future).get();
        return future;
    }

    private KafkaTopic createKafkaTopic(String name) {
        return createKafkaTopic(name, 1);
    }

    private KafkaTopic createKafkaTopic(String topicName, int replicas) {
        return Crds.topicOperation(kubernetesClient).resource(new KafkaTopicBuilder()
            .withNewMetadata()
                .withName(topicName)
                .withNamespace(NAMESPACE)
                .addToLabels("key", "VALUE")
            .endMetadata()
                .withNewSpec()
                .withPartitions(2)
                .withReplicas(replicas)
            .endSpec()
            .build()).create();
    }

    private void assertOnUpdateThrowsInterruptedException(Admin kafkaAdmin, KafkaTopic kafkaTopic) {
        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(true).when(config).useFinalizer();
        Mockito.doReturn(false).when(config).enableAdditionalMetrics();

        var metricsHolder = new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry()));
        var controller = new BatchingTopicController(config, Map.of("key", "VALUE"),
            new KubernetesHandler(config, metricsHolder, kubernetesClient),
            new KafkaHandler(config, metricsHolder, kafkaAdmin), metricsHolder,
            new CruiseControlHandler(config, metricsHolder, TopicOperatorUtil.createCruiseControlClient(config)));
        
        var batch = List.of(new ReconcilableTopic(
            new Reconciliation("test", "KafkaTopic", NAMESPACE, TopicOperatorUtil.topicName(kafkaTopic)), kafkaTopic, TopicOperatorUtil.topicName(kafkaTopic)));
        assertThrows(InterruptedException.class, () -> controller.onUpdate(batch));
    }

    @Test
    public void shouldHandleInterruptedExceptionFromDescribeTopics() throws ExecutionException, InterruptedException {
        var topicName = "my-topic";
        var adminSpy = Mockito.spy(kafkaAdminClient);
        var result = Mockito.mock(DescribeTopicsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).allTopicNames();
        Mockito.doReturn(Map.of(topicName, interruptedFuture())).when(result).topicNameValues();
        Mockito.doReturn(result).when(adminSpy).describeTopics(any(Collection.class));

        var kafkaTopic = createKafkaTopic(topicName);
        assertOnUpdateThrowsInterruptedException(adminSpy, kafkaTopic);
    }

    @Test
    public void shouldHandleInterruptedExceptionFromDescribeConfigs() throws ExecutionException, InterruptedException {
        var topicName = "my-topic";
        var adminSpy = Mockito.spy(kafkaAdminClient);
        var result = Mockito.mock(DescribeConfigsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).all();
        Mockito.doReturn(Map.of(topicConfigResource(topicName), interruptedFuture())).when(result).values();
        Mockito.doReturn(result).when(adminSpy).describeConfigs(Mockito.argThat(a -> a.stream().anyMatch(x -> x.type() == ConfigResource.Type.TOPIC)));

        var kafkaTopic = createKafkaTopic(topicName);
        assertOnUpdateThrowsInterruptedException(adminSpy, kafkaTopic);
    }

    private static ConfigResource topicConfigResource(String topicName) {
        return new ConfigResource(ConfigResource.Type.TOPIC, topicName);
    }

    @Test
    public void shouldHandleInterruptedExceptionFromCreateTopics() throws ExecutionException, InterruptedException {
        var topicName = "my-topic";
        var adminSpy = Mockito.spy(kafkaAdminClient);
        var result = Mockito.mock(CreateTopicsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).all();
        Mockito.doReturn(Map.of(topicName, interruptedFuture())).when(result).values();
        Mockito.doReturn(result).when(adminSpy).createTopics(any());

        var kafkaTopic = createKafkaTopic(topicName);
        assertOnUpdateThrowsInterruptedException(adminSpy, kafkaTopic);
    }

    @Test
    public void shouldHandleInterruptedExceptionFromIncrementalAlterConfigs() throws ExecutionException, InterruptedException {
        var topicName = "my-topic";
        kafkaAdminClient.createTopics(List.of(new NewTopic(topicName, 1, (short) 1).configs(Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy")))).all().get();
        var adminSpy = Mockito.spy(kafkaAdminClient);
        var result = Mockito.mock(AlterConfigsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).all();
        Mockito.doReturn(Map.of(topicConfigResource(topicName), interruptedFuture())).when(result).values();
        Mockito.doReturn(result).when(adminSpy).incrementalAlterConfigs(any());
        
        var kafkaTopic = createKafkaTopic(topicName);
        assertOnUpdateThrowsInterruptedException(adminSpy, kafkaTopic);
    }

    @Test
    public void shouldHandleInterruptedExceptionFromCreatePartitions() throws ExecutionException, InterruptedException {
        var topicName = "my-topic";
        kafkaAdminClient.createTopics(List.of(new NewTopic(topicName, 1, (short) 1))).all().get();

        var adminSpy = Mockito.spy(kafkaAdminClient);
        var result = Mockito.mock(CreatePartitionsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).all();
        Mockito.doReturn(Map.of(topicName, interruptedFuture())).when(result).values();
        Mockito.doReturn(result).when(adminSpy).createPartitions(any());
        
        var kafkaTopic = createKafkaTopic(topicName);
        assertOnUpdateThrowsInterruptedException(adminSpy, kafkaTopic);
    }

    // Two brokers
    @Test
    public void shouldHandleInterruptedExceptionFromListReassignments() throws ExecutionException, InterruptedException {
        var topicName = "my-topic";
        kafkaAdminClient.createTopics(List.of(new NewTopic(topicName, 2, (short) 1))).all().get();

        var adminSpy = Mockito.spy(kafkaAdminClient);
        var result = Mockito.mock(ListPartitionReassignmentsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).reassignments();
        Mockito.doReturn(result).when(adminSpy).listPartitionReassignments(any(Set.class));
        
        var kafkaTopic = createKafkaTopic(topicName, 2);
        assertOnUpdateThrowsInterruptedException(adminSpy, kafkaTopic);
    }

    @Test
    public void shouldHandleInterruptedExceptionFromDeleteTopics() throws ExecutionException, InterruptedException {
        var topicName = "my-topic";
        kafkaAdminClient.createTopics(List.of(new NewTopic(topicName, 1, (short) 1))).all().get();
        createKafkaTopic(topicName);
        Crds.topicOperation(kubernetesClient).inNamespace(NAMESPACE).withName(topicName).edit(theKt -> 
            new KafkaTopicBuilder(theKt).editOrNewMetadata().addToFinalizers(KubernetesHandler.FINALIZER_STRIMZI_IO_TO).endMetadata().build());
        Crds.topicOperation(kubernetesClient).inNamespace(NAMESPACE).withName(topicName).delete();
        var withDeletionTimestamp = Crds.topicOperation(kubernetesClient).inNamespace(NAMESPACE).withName(topicName).get();

        var adminSpy = Mockito.spy(kafkaAdminClient);
        var result = Mockito.mock(DeleteTopicsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).all();
        Mockito.doReturn(Map.of(topicName, interruptedFuture())).when(result).topicNameValues();
        Mockito.doReturn(result).when(adminSpy).deleteTopics(any(TopicCollection.TopicNameCollection.class));
        assertOnUpdateThrowsInterruptedException(adminSpy, withDeletionTimestamp);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void replicasChangeShouldBeReconciled(boolean cruiseControlEnabled) {
        var topicName = "my-topic";
        var replicationFactor = 1;

        // setup: .spec.replicas != replicationFactor
        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(true).when(config).useFinalizer();
        Mockito.doReturn(false).when(config).enableAdditionalMetrics();
        Mockito.doReturn(cruiseControlEnabled).when(config).cruiseControlEnabled();

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
        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(true).when(config).useFinalizer();
        Mockito.doReturn(false).when(config).enableAdditionalMetrics();
        Mockito.doReturn(true).when(config).cruiseControlEnabled();

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
        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(true).when(config).useFinalizer();
        Mockito.doReturn(false).when(config).enableAdditionalMetrics();
        Mockito.doReturn(true).when(config).cruiseControlEnabled();

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

    @Test
    public void shouldNotCallGetClusterConfigWhenDisabled() {
        var kafkaAdmin = Mockito.mock(Admin.class);
        var config = TopicOperatorConfig.buildFromMap(Map.of(
              TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
              TopicOperatorConfig.NAMESPACE.key(), "some-namespace",
              TopicOperatorConfig.SASL_ENABLED.key(), "true",
              TopicOperatorConfig.SKIP_CLUSTER_CONFIG_REVIEW.key(), "true"
        ));

        var metricsHolder = new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry()));
        new BatchingTopicController(config, Map.of("key", "VALUE"),
            new KubernetesHandler(config, metricsHolder, kubernetesClient),
            new KafkaHandler(config, metricsHolder, kafkaAdmin), metricsHolder,
            new CruiseControlHandler(config, metricsHolder, TopicOperatorUtil.createCruiseControlClient(config)));

        verifyNoInteractions(kafkaAdmin);
    }

    @Test
    public void shouldIgnoreWithCruiseControlThrottleConfigInKafka() throws InterruptedException, ExecutionException {
        var kafkaAdminClientSpy = Mockito.spy(kafkaAdminClient);
        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(true).when(config).cruiseControlEnabled();

        // setup topic in Kafka
        kafkaAdminClient.createTopics(List.of(new NewTopic("my-topic", 2, (short) 1).configs(Map.of(
            "leader.replication.throttled.replicas", "13:0,13:1,45:0,45:1",
            "follower.replication.throttled.replicas", "13:0,13:1,45:0,45:1"
        )))).all().get();

        // setup topic in Kube
        var testTopic = Crds.topicOperation(kubernetesClient).resource(
            new KafkaTopicBuilder()
                .withNewMetadata()
                    .withName("my-topic")
                    .withNamespace(NAMESPACE)
                    .addToLabels("key", "VALUE")
                .endMetadata()
                .withNewSpec()
                    .withPartitions(2)
                    .withReplicas(1)
                .endSpec()
                .build()).create();

        // run test
        var metricsHolder = new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry()));
        var controller = new BatchingTopicController(config, Map.of("key", "VALUE"),
            new KubernetesHandler(config, metricsHolder, kubernetesClient),
            new KafkaHandler(config, metricsHolder, kafkaAdminClientSpy), metricsHolder,
            new CruiseControlHandler(config, metricsHolder, TopicOperatorUtil.createCruiseControlClient(config)));
        controller.onUpdate(List.of(new ReconcilableTopic(
            new Reconciliation("test", KafkaTopic.RESOURCE_KIND, NAMESPACE, "my-topic"), testTopic, "my-topic")));

        Mockito.verify(kafkaAdminClientSpy, Mockito.never()).incrementalAlterConfigs(any());

        testTopic = Crds.topicOperation(kubernetesClient).inNamespace(NAMESPACE).withName("my-topic").get();
        assertEquals(1, testTopic.getStatus().getConditions().size());
        assertEquals("True", testTopic.getStatus().getConditions().get(0).getStatus());
    }

    @Test
    public void shouldReconcileAndWarnWithThrottleConfigInKube() throws InterruptedException, ExecutionException {
        var kafkaAdminClientSpy = Mockito.spy(kafkaAdminClient);
        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(true).when(config).cruiseControlEnabled();

        // setup topic in Kafka
        kafkaAdminClient.createTopics(List.of(new NewTopic("my-topic", 2, (short) 1).configs(Map.of(
            "leader.replication.throttled.replicas", "13:0,13:1,45:0,45:1",
            "follower.replication.throttled.replicas", "13:0,13:1,45:0,45:1"
        )))).all().get();

        // setup topic in Kube
        var testTopic = Crds.topicOperation(kubernetesClient).resource(
            new KafkaTopicBuilder()
                .withNewMetadata()
                    .withName("my-topic")
                    .withNamespace(NAMESPACE)
                    .addToLabels("key", "VALUE")
                .endMetadata()
                .withNewSpec()
                    .withConfig(Map.of(
                        "leader.replication.throttled.replicas", "10:1",
                        "follower.replication.throttled.replicas", "10:1"))
                    .withPartitions(2)
                    .withReplicas(1)
                .endSpec()
                .build()).create();

        // run test
        var metricsHolder = new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry()));
        var controller = new BatchingTopicController(config, Map.of("key", "VALUE"),
            new KubernetesHandler(config, metricsHolder, kubernetesClient),
            new KafkaHandler(config, metricsHolder, kafkaAdminClientSpy), metricsHolder,
            new CruiseControlHandler(config, metricsHolder, TopicOperatorUtil.createCruiseControlClient(config)));
        controller.onUpdate(List.of(new ReconcilableTopic(new Reconciliation("test", KafkaTopic.RESOURCE_KIND, NAMESPACE, "my-topic"), testTopic, "my-topic")));

        Mockito.verify(kafkaAdminClientSpy, Mockito.times(1)).incrementalAlterConfigs(any());

        testTopic = Crds.topicOperation(kubernetesClient).inNamespace(NAMESPACE).withName("my-topic").get();
        assertEquals(3, testTopic.getStatus().getConditions().size());
        assertEquals("True", testTopic.getStatus().getConditions().get(0).getStatus());

        var warning1 = testTopic.getStatus().getConditions().get(1);
        assertEquals("Property follower.replication.throttled.replicas may conflict with throttled rebalances", warning1.getMessage());
        assertEquals(BatchingTopicController.INVALID_CONFIG, warning1.getReason());
        assertEquals("True", warning1.getStatus());

        var warning2 = testTopic.getStatus().getConditions().get(2);
        assertEquals("Property leader.replication.throttled.replicas may conflict with throttled rebalances", warning2.getMessage());
        assertEquals(BatchingTopicController.INVALID_CONFIG, warning2.getReason());
        assertEquals("True", warning2.getStatus());

        // remove warning condition
        testTopic = Crds.topicOperation(kubernetesClient).resource(
            new KafkaTopicBuilder()
                .withNewMetadata()
                    .withName("my-topic")
                    .withNamespace(NAMESPACE)
                    .addToLabels("key", "VALUE")
                .endMetadata()
                    .withNewSpec()
                    .withConfig(Map.of(
                        TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1"))
                    .withPartitions(2)
                    .withReplicas(1)
                .endSpec()
                .build()).update();
        controller.onUpdate(List.of(new ReconcilableTopic(
            new Reconciliation("test", KafkaTopic.RESOURCE_KIND, NAMESPACE, "my-topic"), testTopic, "my-topic")));

        testTopic = Crds.topicOperation(kubernetesClient).inNamespace(NAMESPACE).withName("my-topic").get();
        assertEquals(1, testTopic.getStatus().getConditions().size());
        assertEquals("True", testTopic.getStatus().getConditions().get(0).getStatus());
    }
    
    @ParameterizedTest
    @ValueSource(strings = { "min.insync.replicas, compression.type" })
    public void shouldIgnoreAndWarnWithAlterableConfigOnCreation(String alterableConfig) throws InterruptedException {
        var kafkaAdminClientSpy = Mockito.spy(kafkaAdminClient);
        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(alterableConfig).when(config).alterableTopicConfig();

        // setup topic in Kube
        var testTopic = Crds.topicOperation(kubernetesClient).resource(
            new KafkaTopicBuilder()
                .withNewMetadata()
                    .withName("my-topic")
                    .withNamespace(NAMESPACE)
                    .addToLabels("key", "VALUE")
                .endMetadata()
                .withNewSpec()
                    .withConfig(Map.of(
                        TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy",
                        TopicConfig.CLEANUP_POLICY_CONFIG, "compact",
                        TopicConfig.SEGMENT_BYTES_CONFIG, "1073741824"))
                    .withPartitions(2)
                    .withReplicas(1)
                .endSpec()
                .build()).create();

        // run test
        var metricsHolder = new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry()));
        var controller = new BatchingTopicController(config, Map.of("key", "VALUE"),
            new KubernetesHandler(config, metricsHolder, kubernetesClient),
            new KafkaHandler(config, metricsHolder, kafkaAdminClientSpy), metricsHolder,
            new CruiseControlHandler(config, metricsHolder, TopicOperatorUtil.createCruiseControlClient(config)));
        controller.onUpdate(List.of(new ReconcilableTopic(new Reconciliation("test", KafkaTopic.RESOURCE_KIND, NAMESPACE, "my-topic"), testTopic, "my-topic")));

        Mockito.verify(kafkaAdminClientSpy, Mockito.never()).incrementalAlterConfigs(any());

        testTopic = Crds.topicOperation(kubernetesClient).inNamespace(NAMESPACE).withName("my-topic").get();
        assertEquals(3, testTopic.getStatus().getConditions().size());
        assertEquals("True", testTopic.getStatus().getConditions().get(0).getStatus());

        var warning1 = testTopic.getStatus().getConditions().get(1);
        assertEquals("Property cleanup.policy is ignored according to alterable config", warning1.getMessage());
        assertEquals(BatchingTopicController.INVALID_CONFIG, warning1.getReason());
        assertEquals("True", warning1.getStatus());

        var warning2 = testTopic.getStatus().getConditions().get(2);
        assertEquals("Property segment.bytes is ignored according to alterable config", warning2.getMessage());
        assertEquals(BatchingTopicController.INVALID_CONFIG, warning2.getReason());
        assertEquals("True", warning2.getStatus());
    }
    
    @ParameterizedTest
    @ValueSource(strings = { "compression.type, max.message.bytes, message.timestamp.difference.max.ms, message.timestamp.type, retention.bytes, retention.ms" })
    public void shouldReconcileWithAlterableConfigOnUpdate(String alterableConfig) throws InterruptedException, ExecutionException {
        var kafkaAdminClientSpy = Mockito.spy(kafkaAdminClient);
        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(alterableConfig).when(config).alterableTopicConfig();

        // setup topic in Kafka
        kafkaAdminClient.createTopics(List.of(new NewTopic("my-topic", 2, (short) 1).configs(Map.of(
            TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2"
        )))).all().get();

        // setup topic in Kube
        var testTopic = Crds.topicOperation(kubernetesClient).resource(
            new KafkaTopicBuilder()
                .withNewMetadata()
                    .withName("my-topic")
                    .withNamespace(NAMESPACE)
                    .addToLabels("key", "VALUE")
                .endMetadata()
                .withNewSpec()
                    .withConfig(Map.of(
                        TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy",
                        TopicConfig.CLEANUP_POLICY_CONFIG, "compact",
                        TopicConfig.SEGMENT_BYTES_CONFIG, "1073741824"))
                    .withPartitions(2)
                    .withReplicas(1)
                .endSpec()
                .build()).create();

        // run test
        var metricsHolder = new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry()));
        var controller = new BatchingTopicController(config, Map.of("key", "VALUE"),
            new KubernetesHandler(config, metricsHolder, kubernetesClient),
            new KafkaHandler(config, metricsHolder, kafkaAdminClientSpy), metricsHolder,
            new CruiseControlHandler(config, metricsHolder, TopicOperatorUtil.createCruiseControlClient(config)));
        controller.onUpdate(List.of(new ReconcilableTopic(new Reconciliation("test", KafkaTopic.RESOURCE_KIND, NAMESPACE, "my-topic"), testTopic, "my-topic")));

        Mockito.verify(kafkaAdminClientSpy, Mockito.times(1)).incrementalAlterConfigs(any());

        testTopic = Crds.topicOperation(kubernetesClient).inNamespace(NAMESPACE).withName("my-topic").get();
        assertEquals(3, testTopic.getStatus().getConditions().size());
        assertEquals("True", testTopic.getStatus().getConditions().get(0).getStatus());

        var warning1 = testTopic.getStatus().getConditions().get(1);
        assertEquals("Property cleanup.policy is ignored according to alterable config", warning1.getMessage());
        assertEquals(BatchingTopicController.INVALID_CONFIG, warning1.getReason());
        assertEquals("True", warning1.getStatus());

        var warning2 = testTopic.getStatus().getConditions().get(2);
        assertEquals("Property segment.bytes is ignored according to alterable config", warning2.getMessage());
        assertEquals(BatchingTopicController.INVALID_CONFIG, warning2.getReason());
        assertEquals("True", warning2.getStatus());

        // remove warning condition
        testTopic = Crds.topicOperation(kubernetesClient).resource(
            new KafkaTopicBuilder()
                .withNewMetadata()
                    .withName("my-topic")
                    .withNamespace(NAMESPACE)
                    .addToLabels("key", "VALUE")
                .endMetadata()
                .withNewSpec()
                    .withConfig(Map.of(
                        TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy"))
                    .withPartitions(2)
                    .withReplicas(1)
                .endSpec()
                .build()).update();
        controller.onUpdate(List.of(new ReconcilableTopic(
            new Reconciliation("test", KafkaTopic.RESOURCE_KIND, NAMESPACE, "my-topic"), testTopic, "my-topic")));

        testTopic = Crds.topicOperation(kubernetesClient).inNamespace(NAMESPACE).withName("my-topic").get();
        assertEquals(1, testTopic.getStatus().getConditions().size());
        assertEquals("True", testTopic.getStatus().getConditions().get(0).getStatus());
    }
    
    @ParameterizedTest
    @ValueSource(strings = { "ALL", "" })
    public void shouldReconcileWithAllOrEmptyAlterableConfig(String alterableConfig) throws InterruptedException, ExecutionException {
        var kafkaAdminClientSpy = Mockito.spy(kafkaAdminClient);
        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(alterableConfig).when(config).alterableTopicConfig();

        // setup topic in Kafka
        kafkaAdminClient.createTopics(List.of(new NewTopic("my-topic", 2, (short) 1).configs(Map.of(
            TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy"
        )))).all().get();

        // setup topic in Kube
        var testTopic = Crds.topicOperation(kubernetesClient).resource(
            new KafkaTopicBuilder()
                .withNewMetadata()
                    .withName("my-topic")
                    .withNamespace(NAMESPACE)
                    .addToLabels("key", "VALUE")
                .endMetadata()
                .withNewSpec()
                    .withConfig(Map.of(
                        TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy",
                        TopicConfig.CLEANUP_POLICY_CONFIG, "compact"))
                    .withPartitions(2)
                    .withReplicas(1)
                .endSpec()
                .build()).create();

        // run test
        var metricsHolder = new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry()));
        var controller = new BatchingTopicController(config, Map.of("key", "VALUE"),
            new KubernetesHandler(config, metricsHolder, kubernetesClient),
            new KafkaHandler(config, metricsHolder, kafkaAdminClientSpy), metricsHolder,
            new CruiseControlHandler(config, metricsHolder, TopicOperatorUtil.createCruiseControlClient(config)));
        controller.onUpdate(List.of(new ReconcilableTopic(new Reconciliation("test", KafkaTopic.RESOURCE_KIND, NAMESPACE, "my-topic"), testTopic, "my-topic")));

        Mockito.verify(kafkaAdminClientSpy, Mockito.times(1)).incrementalAlterConfigs(any());

        testTopic = Crds.topicOperation(kubernetesClient).inNamespace(NAMESPACE).withName("my-topic").get();
        assertEquals(1, testTopic.getStatus().getConditions().size());
        assertEquals("True", testTopic.getStatus().getConditions().get(0).getStatus());
    }
    
    @ParameterizedTest
    @ValueSource(strings = { "NONE" })
    public void shouldIgnoreAndWarnWithNoneAlterableConfig(String alterableConfig) throws InterruptedException, ExecutionException {
        var kafkaAdminClientSpy = Mockito.spy(kafkaAdminClient);
        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(alterableConfig).when(config).alterableTopicConfig();

        // setup topic in Kafka
        kafkaAdminClient.createTopics(List.of(new NewTopic("my-topic", 2, (short) 1).configs(Map.of(
            TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy"
        )))).all().get();

        // setup topic in Kube
        var testTopic = Crds.topicOperation(kubernetesClient).resource(
            new KafkaTopicBuilder()
                .withNewMetadata()
                    .withName("my-topic")
                    .withNamespace(NAMESPACE)
                    .addToLabels("key", "VALUE")
                .endMetadata()
                .withNewSpec()
                    .withConfig(Map.of(
                        TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy",
                        TopicConfig.CLEANUP_POLICY_CONFIG, "compact"))
                    .withPartitions(2)
                    .withReplicas(1)
                .endSpec()
                .build()).create();

        // run test
        var metricsHolder = new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry()));
        var controller = new BatchingTopicController(config, Map.of("key", "VALUE"),
            new KubernetesHandler(config, metricsHolder, kubernetesClient),
            new KafkaHandler(config, metricsHolder, kafkaAdminClientSpy), metricsHolder,
            new CruiseControlHandler(config, metricsHolder, TopicOperatorUtil.createCruiseControlClient(config)));
        controller.onUpdate(List.of(new ReconcilableTopic(new Reconciliation("test", KafkaTopic.RESOURCE_KIND, NAMESPACE, "my-topic"), testTopic, "my-topic")));

        Mockito.verify(kafkaAdminClientSpy, Mockito.never()).incrementalAlterConfigs(any());

        testTopic = Crds.topicOperation(kubernetesClient).inNamespace(NAMESPACE).withName("my-topic").get();
        assertEquals(3, testTopic.getStatus().getConditions().size());
        assertEquals("True", testTopic.getStatus().getConditions().get(0).getStatus());

        var warning1 = testTopic.getStatus().getConditions().get(1);
        assertEquals("Property cleanup.policy is ignored according to alterable config", warning1.getMessage());
        assertEquals(BatchingTopicController.INVALID_CONFIG, warning1.getReason());
        assertEquals("True", warning1.getStatus());

        var warning2 = testTopic.getStatus().getConditions().get(2);
        assertEquals("Property compression.type is ignored according to alterable config", warning2.getMessage());
        assertEquals(BatchingTopicController.INVALID_CONFIG, warning2.getReason());
        assertEquals("True", warning2.getStatus());
    }
    
    @ParameterizedTest
    @ValueSource(strings = { "invalid", "compression.type; cleanup.policy" })
    public void shouldIgnoreAndWarnWithInvalidAlterableConfig(String alterableConfig) throws InterruptedException, ExecutionException {
        var kafkaAdminClientSpy = Mockito.spy(kafkaAdminClient);
        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(alterableConfig).when(config).alterableTopicConfig();

        // setup topic in Kafka
        kafkaAdminClient.createTopics(List.of(new NewTopic("my-topic", 2, (short) 1).configs(Map.of(
            TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy"
        )))).all().get();

        // setup topic in Kube
        var testTopic = Crds.topicOperation(kubernetesClient).resource(
            new KafkaTopicBuilder()
                .withNewMetadata()
                    .withName("my-topic")
                    .withNamespace(NAMESPACE)
                    .addToLabels("key", "VALUE")
                .endMetadata()
                .withNewSpec()
                    .withConfig(Map.of(
                        TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy",
                        TopicConfig.CLEANUP_POLICY_CONFIG, "compact"))
                    .withPartitions(2)
                    .withReplicas(1)
                .endSpec()
                .build()).create();

        // run test
        var metricsHolder = new TopicOperatorMetricsHolder(KafkaTopic.RESOURCE_KIND, null, new TopicOperatorMetricsProvider(new SimpleMeterRegistry()));
        var controller = new BatchingTopicController(config, Map.of("key", "VALUE"),
            new KubernetesHandler(config, metricsHolder, kubernetesClient),
            new KafkaHandler(config, metricsHolder, kafkaAdminClientSpy), metricsHolder,
            new CruiseControlHandler(config, metricsHolder, TopicOperatorUtil.createCruiseControlClient(config)));
        controller.onUpdate(List.of(new ReconcilableTopic(new Reconciliation("test", KafkaTopic.RESOURCE_KIND, NAMESPACE, "my-topic"), testTopic, "my-topic")));

        Mockito.verify(kafkaAdminClientSpy, Mockito.never()).incrementalAlterConfigs(any());

        testTopic = Crds.topicOperation(kubernetesClient).inNamespace(NAMESPACE).withName("my-topic").get();
        assertEquals(3, testTopic.getStatus().getConditions().size());
        assertEquals("True", testTopic.getStatus().getConditions().get(0).getStatus());

        var warning1 = testTopic.getStatus().getConditions().get(1);
        assertEquals("Property cleanup.policy is ignored according to alterable config", warning1.getMessage());
        assertEquals(BatchingTopicController.INVALID_CONFIG, warning1.getReason());
        assertEquals("True", warning1.getStatus());

        var warning2 = testTopic.getStatus().getConditions().get(2);
        assertEquals("Property compression.type is ignored according to alterable config", warning2.getMessage());
        assertEquals(BatchingTopicController.INVALID_CONFIG, warning2.getReason());
        assertEquals("True", warning2.getStatus());
    }
}
