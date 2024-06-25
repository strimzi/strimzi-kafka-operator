/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicBuilder;
import io.strimzi.api.kafka.model.topic.KafkaTopicStatusBuilder;
import io.strimzi.api.kafka.model.topic.ReplicasChangeStatusBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsHolder;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsProvider;
import io.strimzi.operator.topic.model.Either;
import io.strimzi.operator.topic.model.Pair;
import io.strimzi.operator.topic.model.PartitionedByError;
import io.strimzi.operator.topic.model.ReconcilableTopic;
import io.strimzi.operator.topic.model.TopicOperatorException;
import io.strimzi.operator.topic.model.TopicState;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static io.strimzi.api.kafka.model.topic.KafkaTopic.RESOURCE_KIND;
import static io.strimzi.api.kafka.model.topic.ReplicasChangeState.PENDING;
import static io.strimzi.operator.topic.TopicOperatorUtil.topicName;
import static io.strimzi.operator.topic.model.TopicOperatorException.Reason.INVALID_CONFIG;
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

/**
 * This test is not intended to provide lots of coverage of the {@link BatchingTopicController}
 * (see {@link TopicControllerIT} for that), rather it aims to cover some parts that a difficult
 * to test via {@link TopicControllerIT}
 */
@ExtendWith(KafkaClusterExtension.class)
class BatchingTopicControllerTest {
    private static final Logger LOGGER = LogManager.getLogger(BatchingTopicControllerTest.class);
    private static final String NAMESPACE = "topic-operator-test";

    private BatchingTopicController controller;
    private KubernetesClient client;

    private final Admin[] admin = new Admin[] {null};

    private TopicOperatorMetricsHolder metrics;

    private static <T> KafkaFuture<T> interruptedFuture() throws ExecutionException, InterruptedException {
        var future = Mockito.mock(KafkaFuture.class);
        doThrow(new InterruptedException()).when(future).get();
        return future;
    }

    private String namespace(String ns) {
        return TopicOperatorTestUtil.namespace(client, ns);
    }

    private KafkaTopic createResource() {
        return createResource(1);
    }
    
    private KafkaTopic createResource(int replicas) {
        var kt = Crds.topicOperation(client).resource(
            new KafkaTopicBuilder()
                .withNewMetadata()
                    .withName("my-topic")
                    .withNamespace(namespace(NAMESPACE))
                    .addToLabels("key", "VALUE")
                .endMetadata()
                .withNewSpec()
                    .withPartitions(2)
                    .withReplicas(replicas)
                .endSpec().build()).create();
        return kt;
    }

    @BeforeAll
    public static void setupKubeCluster(TestInfo testInfo) {
        TopicOperatorTestUtil.setupKubeCluster(testInfo, NAMESPACE);
    }

    @AfterAll
    public static void teardownKubeCluster() {
        TopicOperatorTestUtil.teardownKubeCluster(NAMESPACE);
    }

    @BeforeEach
    public void beforeEach() {
        this.client = new KubernetesClientBuilder().build();
        TopicOperatorMetricsProvider metricsProvider = new TopicOperatorMetricsProvider(new SimpleMeterRegistry());
        this.metrics = new TopicOperatorMetricsHolder(RESOURCE_KIND, null, metricsProvider);
    }

    @AfterEach
    public void after(TestInfo testInfo) throws InterruptedException {
        LOGGER.debug("Cleaning up after test {}", TopicOperatorTestUtil.testName(testInfo));
        if (admin[0] != null) {
            admin[0].close();
        }

        String pop = NAMESPACE;
        TopicOperatorTestUtil.cleanupNamespace(client, testInfo, pop);

        client.close();
        LOGGER.debug("Cleaned up after test {}", TopicOperatorTestUtil.testName(testInfo));
    }

    private void assertOnUpdateThrowsInterruptedException(KubernetesClient client, Admin admin, KafkaTopic kt) {
        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(true).when(config).useFinalizer();
        Mockito.doReturn(false).when(config).enableAdditionalMetrics();
        var replicasChangeHandler = Mockito.mock(ReplicasChangeHandler.class);
        
        controller = new BatchingTopicController(config, Map.of("key", "VALUE"), admin, client, metrics, replicasChangeHandler);
        List<ReconcilableTopic> batch = List.of(new ReconcilableTopic(new Reconciliation("test", "KafkaTopic", NAMESPACE, "my-topic"), kt, "my-topic"));
        assertThrows(InterruptedException.class, () -> controller.onUpdate(batch));
    }

    @Test
    public void shouldHandleInterruptedExceptionFromDescribeTopics(KafkaCluster cluster) throws ExecutionException, InterruptedException {
        admin[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        Admin adminSpy = Mockito.spy(admin[0]);
        var result = Mockito.mock(DescribeTopicsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).allTopicNames();
        Mockito.doReturn(Map.of("my-topic", interruptedFuture())).when(result).topicNameValues();
        Mockito.doReturn(result).when(adminSpy).describeTopics(any(Collection.class));

        KafkaTopic kt = createResource();
        assertOnUpdateThrowsInterruptedException(client, adminSpy, kt);
    }

    @Test
    public void shouldHandleInterruptedExceptionFromDescribeConfigs(KafkaCluster cluster) throws ExecutionException, InterruptedException {
        admin[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        Admin adminSpy = Mockito.spy(admin[0]);
        var result = Mockito.mock(DescribeConfigsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).all();
        Mockito.doReturn(Map.of(topicConfigResource(), interruptedFuture())).when(result).values();
        Mockito.doReturn(result).when(adminSpy).describeConfigs(Mockito.argThat(a -> a.stream().anyMatch(x -> x.type() == ConfigResource.Type.TOPIC)));

        KafkaTopic kt = createResource();
        assertOnUpdateThrowsInterruptedException(client, adminSpy, kt);
    }

    private static ConfigResource topicConfigResource() {
        return new ConfigResource(ConfigResource.Type.TOPIC, "my-topic");
    }

    @Test
    public void shouldHandleInterruptedExceptionFromCreateTopics(KafkaCluster cluster) throws ExecutionException, InterruptedException {
        admin[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        Admin adminSpy = Mockito.spy(admin[0]);
        var result = Mockito.mock(CreateTopicsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).all();
        Mockito.doReturn(Map.of("my-topic", interruptedFuture())).when(result).values();
        Mockito.doReturn(result).when(adminSpy).createTopics(any());

        KafkaTopic kt = createResource();
        assertOnUpdateThrowsInterruptedException(client, adminSpy, kt);
    }

    @Test
    public void shouldHandleInterruptedExceptionFromIncrementalAlterConfigs(KafkaCluster cluster) throws ExecutionException, InterruptedException {
        admin[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        admin[0].createTopics(List.of(new NewTopic("my-topic", 1, (short) 1).configs(Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy")))).all().get();
        Admin adminSpy = Mockito.spy(admin[0]);
        var result = Mockito.mock(AlterConfigsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).all();
        Mockito.doReturn(Map.of(topicConfigResource(), interruptedFuture())).when(result).values();
        Mockito.doReturn(result).when(adminSpy).incrementalAlterConfigs(any());
        KafkaTopic kt = createResource();
        assertOnUpdateThrowsInterruptedException(client, adminSpy, kt);
    }

    @Test
    public void shouldHandleInterruptedExceptionFromCreatePartitions(KafkaCluster cluster) throws ExecutionException, InterruptedException {
        admin[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        admin[0].createTopics(List.of(new NewTopic("my-topic", 1, (short) 1))).all().get();

        Admin adminSpy = Mockito.spy(admin[0]);
        var result = Mockito.mock(CreatePartitionsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).all();
        Mockito.doReturn(Map.of("my-topic", interruptedFuture())).when(result).values();
        Mockito.doReturn(result).when(adminSpy).createPartitions(any());
        KafkaTopic kt = createResource();
        assertOnUpdateThrowsInterruptedException(client, adminSpy, kt);
    }

    @Test
    public void shouldHandleInterruptedExceptionFromListReassignments(
            @BrokerCluster(numBrokers = 2)
            KafkaCluster cluster) throws ExecutionException, InterruptedException {
        admin[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        admin[0].createTopics(List.of(new NewTopic("my-topic", 2, (short) 2))).all().get();

        Admin adminSpy = Mockito.spy(admin[0]);
        var result = Mockito.mock(ListPartitionReassignmentsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).reassignments();
        Mockito.doReturn(result).when(adminSpy).listPartitionReassignments(any(Set.class));
        KafkaTopic kt = createResource();
        assertOnUpdateThrowsInterruptedException(client, adminSpy, kt);
    }

    @Test
    public void shouldHandleInterruptedExceptionFromDeleteTopics(KafkaCluster cluster) throws ExecutionException, InterruptedException {
        admin[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        admin[0].createTopics(List.of(new NewTopic("my-topic", 1, (short) 1))).all().get();
        createResource();
        Crds.topicOperation(client).inNamespace(NAMESPACE).withName("my-topic").edit(theKt -> 
            new KafkaTopicBuilder(theKt).editOrNewMetadata().addToFinalizers(BatchingTopicController.FINALIZER).endMetadata().build());
        Crds.topicOperation(client).inNamespace(NAMESPACE).withName("my-topic").delete();
        var withDeletionTimestamp = Crds.topicOperation(client).inNamespace(NAMESPACE).withName("my-topic").get();

        Admin adminSpy = Mockito.spy(admin[0]);
        var result = Mockito.mock(DeleteTopicsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).all();
        Mockito.doReturn(Map.of("my-topic", interruptedFuture())).when(result).topicNameValues();
        Mockito.doReturn(result).when(adminSpy).deleteTopics(any(TopicCollection.TopicNameCollection.class));
        assertOnUpdateThrowsInterruptedException(client, adminSpy, withDeletionTimestamp);
    }

    // TODO kube client interrupted exceptions

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void replicasChangeShouldBeReconciled(boolean cruiseControlEnabled) {
        int replicationFactor = 1;

        // setup
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
        Mockito.doReturn(KafkaFuture.completedFuture(Map.of(topicPartition, partitionReassignment))).when(partitionReassignmentResult).reassignments();

        var admin = Mockito.mock(Admin.class);
        Mockito.doReturn(describeClusterResult).when(admin).describeCluster();
        Mockito.doReturn(partitionReassignmentResult).when(admin).listPartitionReassignments(any(Set.class));
        
        var topicDescription = Mockito.mock(TopicDescription.class);
        var topicPartitionInfo = Mockito.mock(TopicPartitionInfo.class);
        Mockito.doReturn(List.of(topicPartitionInfo)).when(topicDescription).partitions();
        
        var currentState = Mockito.mock(TopicState.class);
        Mockito.doReturn(replicationFactor).when(currentState).uniqueReplicationFactor();
        Mockito.doReturn(topicDescription).when(currentState).description();
        
        var inputKt = new KafkaTopicBuilder()
            .withNewMetadata()
                .withName("my-topic")
                .withNamespace(namespace(NAMESPACE))
                .addToLabels("key", "VALUE")
            .endMetadata()
            .withNewSpec()
                .withPartitions(25)
                .withReplicas(++replicationFactor)
            .endSpec()
            .build();
        var inputRt = new ReconcilableTopic(
            new Reconciliation("test", RESOURCE_KIND, NAMESPACE, "my-topic"), inputKt, topicName(inputKt));

        var reconcilableTopics = List.of(inputRt);
        var currentStatesOrError = new PartitionedByError<>(
            List.of(new Pair<>(inputRt, Either.ofRight(currentState))), List.of());
        
        var outputKt = new KafkaTopicBuilder(inputKt)
            .withStatus(new KafkaTopicStatusBuilder()
                .withReplicasChange(new ReplicasChangeStatusBuilder()
                    .withState(PENDING)
                    .withTargetReplicas(replicationFactor)
                    .build())
                .build())
            .build();
        var outputRt = new ReconcilableTopic(
            new Reconciliation("test", "KafkaTopic", NAMESPACE, "my-topic"), outputKt, topicName(outputKt));

        var replicasChangeHandler = Mockito.mock(ReplicasChangeHandler.class);
        Mockito.doReturn(List.of(outputRt)).when(replicasChangeHandler).requestPendingChanges(anyList());
        Mockito.doReturn(List.of()).when(replicasChangeHandler).requestOngoingChanges(anyList());

        // test
        var results = new BatchingTopicController(config, Map.of("key", "VALUE"), admin, client, metrics, replicasChangeHandler)
            .handleReplicasChanges(reconcilableTopics, currentStatesOrError);

        if (cruiseControlEnabled) {
            assertThat(results.ok().count(), is(1L));
            assertThat(results.ok().findFirst().get().getKey(), is(outputRt));
        } else {
            assertThat(results.errors().count(), is(1L));
            assertThat(results.errors().findFirst().get().getValue(), instanceOf(TopicOperatorException.NotSupported.class));
        }
    }
    
    @Test
    public void replicasChangeShouldCompleteWhenSpecIsReverted() {
        int replicationFactor = 3;

        // setup: pending with error and .spec.replicas == uniqueReplicationFactor
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
        Mockito.doReturn(KafkaFuture.completedFuture(Map.of(topicPartition, partitionReassignment))).when(partitionReassignmentResult).reassignments();

        var admin = Mockito.mock(Admin.class);
        Mockito.doReturn(describeClusterResult).when(admin).describeCluster();
        Mockito.doReturn(partitionReassignmentResult).when(admin).listPartitionReassignments(any(Set.class));

        var topicDescription = Mockito.mock(TopicDescription.class);
        var topicPartitionInfo = Mockito.mock(TopicPartitionInfo.class);
        Mockito.doReturn(List.of(topicPartitionInfo)).when(topicDescription).partitions();

        var currentState = Mockito.mock(TopicState.class);
        Mockito.doReturn(replicationFactor).when(currentState).uniqueReplicationFactor();
        Mockito.doReturn(topicDescription).when(currentState).description();
        
        var kafkaTopic = new KafkaTopicBuilder()
            .withNewMetadata()
                .withName("my-topic")
                .withNamespace(namespace(NAMESPACE))
                .addToLabels("key", "VALUE")
            .endMetadata()
            .withNewSpec()
                .withPartitions(25)
                .withReplicas(replicationFactor)
            .endSpec()
            .withStatus(new KafkaTopicStatusBuilder()
                .withReplicasChange(new ReplicasChangeStatusBuilder()
                        .withMessage("Error message")
                        .withState(PENDING)
                        .withTargetReplicas(replicationFactor)
                    .build())
                .build())
            .build();

        var reconcilableTopic = new ReconcilableTopic(
            new Reconciliation("test", RESOURCE_KIND, NAMESPACE, "my-topic"), kafkaTopic, topicName(kafkaTopic));

        var replicasChangeHandler = Mockito.mock(ReplicasChangeHandler.class);
        Mockito.doReturn(List.of()).when(replicasChangeHandler).requestPendingChanges(anyList());
        Mockito.doReturn(List.of()).when(replicasChangeHandler).requestOngoingChanges(anyList());
        
        var reconcilableTopics = List.of(reconcilableTopic);
        PartitionedByError<ReconcilableTopic, TopicState> currentStatesOrError
            = new PartitionedByError<>(List.of(), List.of());
        
        // run test
        var results = new BatchingTopicController(config, Map.of("key", "VALUE"), admin, client, metrics, replicasChangeHandler)
            .handleReplicasChanges(reconcilableTopics, currentStatesOrError);

        assertThat(results.ok().count(), is(1L));
        assertThat(results.ok().findFirst().get().getKey().kt().getStatus().getReplicasChange(), is(nullValue()));
    }

    @Test
    public void replicasChangeShouldCompleteWhenCruiseControlRestarts() {
        int replicationFactor = 1;

        // setup: pending with .spec.replicas == uniqueReplicationFactor
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
        Mockito.doReturn(KafkaFuture.completedFuture(Map.of(topicPartition, partitionReassignment))).when(partitionReassignmentResult).reassignments();

        var admin = Mockito.mock(Admin.class);
        Mockito.doReturn(describeClusterResult).when(admin).describeCluster();
        Mockito.doReturn(partitionReassignmentResult).when(admin).listPartitionReassignments(any(Set.class));

        var topicDescription = Mockito.mock(TopicDescription.class);
        var topicPartitionInfo = Mockito.mock(TopicPartitionInfo.class);
        Mockito.doReturn(List.of(topicPartitionInfo)).when(topicDescription).partitions();

        var currentState = Mockito.mock(TopicState.class);
        Mockito.doReturn(replicationFactor).when(currentState).uniqueReplicationFactor();
        Mockito.doReturn(topicDescription).when(currentState).description();

        var kafkaTopic = new KafkaTopicBuilder()
            .withNewMetadata()
                .withName("my-topic")
                .withNamespace(namespace(NAMESPACE))
                .addToLabels("key", "VALUE")
            .endMetadata()
            .withNewSpec()
                .withPartitions(25)
                .withReplicas(replicationFactor)
                .endSpec()
            .withStatus(new KafkaTopicStatusBuilder()
                .withReplicasChange(new ReplicasChangeStatusBuilder()
                        .withState(PENDING)
                        .withTargetReplicas(replicationFactor)
                    .build())
                .build())
            .build();

        var reconcilableTopic = new ReconcilableTopic(
            new Reconciliation("test", RESOURCE_KIND, NAMESPACE, "my-topic"), kafkaTopic, topicName(kafkaTopic));

        var replicasChangeHandler = Mockito.mock(ReplicasChangeHandler.class);
        Mockito.doReturn(List.of()).when(replicasChangeHandler).requestPendingChanges(anyList());
        Mockito.doReturn(List.of()).when(replicasChangeHandler).requestOngoingChanges(anyList());

        var reconcilableTopics = List.of(reconcilableTopic);
        PartitionedByError<ReconcilableTopic, TopicState> currentStatesOrError = new PartitionedByError<>(List.of(), List.of());

        // run test
        var results = new BatchingTopicController(config, Map.of("key", "VALUE"), admin, client, metrics, replicasChangeHandler)
            .handleReplicasChanges(reconcilableTopics, currentStatesOrError);
        
        assertThat(results.ok().count(), is(1L));
        assertThat(results.ok().findFirst().get().getKey().kt().getStatus().getReplicasChange(), is(nullValue()));
    }

    @Test
    public void shouldNotCallGetClusterConfigWhenDisabled() {
        var admin = Mockito.mock(Admin.class);

        var config = TopicOperatorConfig.buildFromMap(Map.of(
              TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
              TopicOperatorConfig.NAMESPACE.key(), "some-namespace",
              TopicOperatorConfig.SASL_ENABLED.key(), "true",
              TopicOperatorConfig.SKIP_CLUSTER_CONFIG_REVIEW.key(), "true"
        ));

        new BatchingTopicController(config, Map.of("key", "VALUE"), admin, client, metrics, new ReplicasChangeHandler(config, metrics));

        verifyNoInteractions(admin);
    }

    @Test
    public void shouldIgnoreWithCruiseControlThrottleConfigInKafka(KafkaCluster cluster) throws InterruptedException, ExecutionException {
        admin[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        var adminSpy = Mockito.spy(admin[0]);
        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(true).when(config).cruiseControlEnabled();

        // setup topic in Kafka
        admin[0].createTopics(List.of(new NewTopic("my-topic", 2, (short) 1).configs(Map.of(
            "leader.replication.throttled.replicas", "13:0,13:1,45:0,45:1",
            "follower.replication.throttled.replicas", "13:0,13:1,45:0,45:1"
        )))).all().get();

        // setup topic in Kube
        var testTopic = Crds.topicOperation(client).resource(
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
        controller = new BatchingTopicController(config, Map.of("key", "VALUE"), adminSpy, client, metrics, new ReplicasChangeHandler(config, metrics));
        controller.onUpdate(List.of(new ReconcilableTopic(new Reconciliation("test", RESOURCE_KIND, NAMESPACE, "my-topic"), testTopic, "my-topic")));

        Mockito.verify(adminSpy, Mockito.never()).incrementalAlterConfigs(any());

        testTopic = Crds.topicOperation(client).inNamespace(NAMESPACE).withName("my-topic").get();
        assertEquals(1, testTopic.getStatus().getConditions().size());
        assertEquals("True", testTopic.getStatus().getConditions().get(0).getStatus());
    }

    @Test
    public void shouldReconcileAndWarnWithThrottleConfigInKube(KafkaCluster cluster) throws InterruptedException, ExecutionException {
        admin[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        var adminSpy = Mockito.spy(admin[0]);
        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(true).when(config).cruiseControlEnabled();

        // setup topic in Kafka
        admin[0].createTopics(List.of(new NewTopic("my-topic", 2, (short) 1).configs(Map.of(
            "leader.replication.throttled.replicas", "13:0,13:1,45:0,45:1",
            "follower.replication.throttled.replicas", "13:0,13:1,45:0,45:1"
        )))).all().get();

        // setup topic in Kube
        var testTopic = Crds.topicOperation(client).resource(
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
        controller = new BatchingTopicController(config, Map.of("key", "VALUE"), adminSpy, client, metrics, new ReplicasChangeHandler(config, metrics));
        controller.onUpdate(List.of(new ReconcilableTopic(new Reconciliation("test", RESOURCE_KIND, NAMESPACE, "my-topic"), testTopic, "my-topic")));

        Mockito.verify(adminSpy, Mockito.times(1)).incrementalAlterConfigs(any());

        testTopic = Crds.topicOperation(client).inNamespace(NAMESPACE).withName("my-topic").get();
        assertEquals(3, testTopic.getStatus().getConditions().size());
        assertEquals("True", testTopic.getStatus().getConditions().get(0).getStatus());

        var warning1 = testTopic.getStatus().getConditions().get(1);
        assertEquals("Property follower.replication.throttled.replicas may conflict with throttled rebalances", warning1.getMessage());
        assertEquals(INVALID_CONFIG.value, warning1.getReason());
        assertEquals("True", warning1.getStatus());

        var warning2 = testTopic.getStatus().getConditions().get(2);
        assertEquals("Property leader.replication.throttled.replicas may conflict with throttled rebalances", warning2.getMessage());
        assertEquals(INVALID_CONFIG.value, warning2.getReason());
        assertEquals("True", warning2.getStatus());

        // remove warning condition
        testTopic = Crds.topicOperation(client).resource(
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
        controller.onUpdate(List.of(new ReconcilableTopic(new Reconciliation("test", RESOURCE_KIND, NAMESPACE, "my-topic"), testTopic, "my-topic")));

        testTopic = Crds.topicOperation(client).inNamespace(NAMESPACE).withName("my-topic").get();
        assertEquals(1, testTopic.getStatus().getConditions().size());
        assertEquals("True", testTopic.getStatus().getConditions().get(0).getStatus());
    }

    @ParameterizedTest
    @ValueSource(strings = { "min.insync.replicas, compression.type" })
    public void shouldIgnoreAndWarnWithAlterableConfigOnCreation(String alterableConfig, KafkaCluster cluster) throws InterruptedException {
        admin[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        var adminSpy = Mockito.spy(admin[0]);
        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(alterableConfig).when(config).alterableTopicConfig();

        // setup topic in Kube
        var testTopic = Crds.topicOperation(client).resource(
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
        controller = new BatchingTopicController(config, Map.of("key", "VALUE"), adminSpy, client, metrics, new ReplicasChangeHandler(config, metrics));
        controller.onUpdate(List.of(new ReconcilableTopic(new Reconciliation("test", RESOURCE_KIND, NAMESPACE, "my-topic"), testTopic, "my-topic")));

        Mockito.verify(adminSpy, Mockito.never()).incrementalAlterConfigs(any());

        testTopic = Crds.topicOperation(client).inNamespace(NAMESPACE).withName("my-topic").get();
        assertEquals(3, testTopic.getStatus().getConditions().size());
        assertEquals("True", testTopic.getStatus().getConditions().get(0).getStatus());

        var warning1 = testTopic.getStatus().getConditions().get(1);
        assertEquals("Property cleanup.policy is ignored according to alterable config", warning1.getMessage());
        assertEquals(INVALID_CONFIG.value, warning1.getReason());
        assertEquals("True", warning1.getStatus());

        var warning2 = testTopic.getStatus().getConditions().get(2);
        assertEquals("Property segment.bytes is ignored according to alterable config", warning2.getMessage());
        assertEquals(INVALID_CONFIG.value, warning2.getReason());
        assertEquals("True", warning2.getStatus());
    }
    
    @ParameterizedTest
    @ValueSource(strings = { "compression.type, max.message.bytes, message.timestamp.difference.max.ms, message.timestamp.type, retention.bytes, retention.ms" })
    public void shouldReconcileWithAlterableConfigOnUpdate(String alterableConfig, KafkaCluster cluster) throws InterruptedException, ExecutionException {
        admin[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        var adminSpy = Mockito.spy(admin[0]);
        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(alterableConfig).when(config).alterableTopicConfig();

        // setup topic in Kafka
        admin[0].createTopics(List.of(new NewTopic("my-topic", 2, (short) 1).configs(Map.of(
            TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2"
        )))).all().get();

        // setup topic in Kube
        var testTopic = Crds.topicOperation(client).resource(
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
        controller = new BatchingTopicController(config, Map.of("key", "VALUE"), adminSpy, client, metrics, new ReplicasChangeHandler(config, metrics));
        controller.onUpdate(List.of(new ReconcilableTopic(new Reconciliation("test", RESOURCE_KIND, NAMESPACE, "my-topic"), testTopic, "my-topic")));

        Mockito.verify(adminSpy, Mockito.times(1)).incrementalAlterConfigs(any());

        testTopic = Crds.topicOperation(client).inNamespace(NAMESPACE).withName("my-topic").get();
        assertEquals(3, testTopic.getStatus().getConditions().size());
        assertEquals("True", testTopic.getStatus().getConditions().get(0).getStatus());

        var warning1 = testTopic.getStatus().getConditions().get(1);
        assertEquals("Property cleanup.policy is ignored according to alterable config", warning1.getMessage());
        assertEquals(INVALID_CONFIG.value, warning1.getReason());
        assertEquals("True", warning1.getStatus());

        var warning2 = testTopic.getStatus().getConditions().get(2);
        assertEquals("Property segment.bytes is ignored according to alterable config", warning2.getMessage());
        assertEquals(INVALID_CONFIG.value, warning2.getReason());
        assertEquals("True", warning2.getStatus());
        
        // remove warning condition
        testTopic = Crds.topicOperation(client).resource(
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
        controller.onUpdate(List.of(new ReconcilableTopic(new Reconciliation("test", RESOURCE_KIND, NAMESPACE, "my-topic"), testTopic, "my-topic")));

        testTopic = Crds.topicOperation(client).inNamespace(NAMESPACE).withName("my-topic").get();
        assertEquals(1, testTopic.getStatus().getConditions().size());
        assertEquals("True", testTopic.getStatus().getConditions().get(0).getStatus());
    }

    @ParameterizedTest
    @ValueSource(strings = { "ALL", "" })
    public void shouldReconcileWithAllOrEmptyAlterableConfig(String alterableConfig, KafkaCluster cluster) throws InterruptedException, ExecutionException {
        admin[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        var adminSpy = Mockito.spy(admin[0]);
        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(alterableConfig).when(config).alterableTopicConfig();

        // setup topic in Kafka
        admin[0].createTopics(List.of(new NewTopic("my-topic", 2, (short) 1).configs(Map.of(
            TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy"
        )))).all().get();

        // setup topic in Kube
        var testTopic = Crds.topicOperation(client).resource(
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
        controller = new BatchingTopicController(config, Map.of("key", "VALUE"), adminSpy, client, metrics, new ReplicasChangeHandler(config, metrics));
        controller.onUpdate(List.of(new ReconcilableTopic(new Reconciliation("test", RESOURCE_KIND, NAMESPACE, "my-topic"), testTopic, "my-topic")));

        Mockito.verify(adminSpy, Mockito.times(1)).incrementalAlterConfigs(any());

        testTopic = Crds.topicOperation(client).inNamespace(NAMESPACE).withName("my-topic").get();
        assertEquals(1, testTopic.getStatus().getConditions().size());
        assertEquals("True", testTopic.getStatus().getConditions().get(0).getStatus());
    }

    @ParameterizedTest
    @ValueSource(strings = { "NONE" })
    public void shouldIgnoreAndWarnWithNoneAlterableConfig(String alterableConfig, KafkaCluster cluster) throws InterruptedException, ExecutionException {
        admin[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        var adminSpy = Mockito.spy(admin[0]);
        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(alterableConfig).when(config).alterableTopicConfig();

        // setup topic in Kafka
        admin[0].createTopics(List.of(new NewTopic("my-topic", 2, (short) 1).configs(Map.of(
            TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy"
        )))).all().get();

        // setup topic in Kube
        var testTopic = Crds.topicOperation(client).resource(
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
        controller = new BatchingTopicController(config, Map.of("key", "VALUE"), adminSpy, client, metrics, new ReplicasChangeHandler(config, metrics));
        controller.onUpdate(List.of(new ReconcilableTopic(new Reconciliation("test", RESOURCE_KIND, NAMESPACE, "my-topic"), testTopic, "my-topic")));

        Mockito.verify(adminSpy, Mockito.never()).incrementalAlterConfigs(any());

        testTopic = Crds.topicOperation(client).inNamespace(NAMESPACE).withName("my-topic").get();
        assertEquals(2, testTopic.getStatus().getConditions().size());
        assertEquals("True", testTopic.getStatus().getConditions().get(0).getStatus());

        var warning1 = testTopic.getStatus().getConditions().get(1);
        assertEquals("All properties are ignored according to alterable config", warning1.getMessage());
        assertEquals(INVALID_CONFIG.value, warning1.getReason());
        assertEquals("True", warning1.getStatus());
    }

    @ParameterizedTest
    @ValueSource(strings = { "invalid", "compression.type; cleanup.policy" })
    public void shouldIgnoreAndWarnWithInvalidAlterableConfig(String alterableConfig, KafkaCluster cluster) throws InterruptedException, ExecutionException {
        admin[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        var adminSpy = Mockito.spy(admin[0]);
        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(alterableConfig).when(config).alterableTopicConfig();

        // setup topic in Kafka
        admin[0].createTopics(List.of(new NewTopic("my-topic", 2, (short) 1).configs(Map.of(
            TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy"
        )))).all().get();

        // setup topic in Kube
        var testTopic = Crds.topicOperation(client).resource(
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
        controller = new BatchingTopicController(config, Map.of("key", "VALUE"), adminSpy, client, metrics, new ReplicasChangeHandler(config, metrics));
        controller.onUpdate(List.of(new ReconcilableTopic(new Reconciliation("test", RESOURCE_KIND, NAMESPACE, "my-topic"), testTopic, "my-topic")));

        Mockito.verify(adminSpy, Mockito.never()).incrementalAlterConfigs(any());

        testTopic = Crds.topicOperation(client).inNamespace(NAMESPACE).withName("my-topic").get();
        assertEquals(3, testTopic.getStatus().getConditions().size());
        assertEquals("True", testTopic.getStatus().getConditions().get(0).getStatus());

        var warning1 = testTopic.getStatus().getConditions().get(1);
        assertEquals("Property cleanup.policy is ignored according to alterable config", warning1.getMessage());
        assertEquals(INVALID_CONFIG.value, warning1.getReason());
        assertEquals("True", warning1.getStatus());

        var warning2 = testTopic.getStatus().getConditions().get(2);
        assertEquals("Property compression.type is ignored according to alterable config", warning2.getMessage());
        assertEquals(INVALID_CONFIG.value, warning2.getReason());
        assertEquals("True", warning2.getStatus());
    }
}
