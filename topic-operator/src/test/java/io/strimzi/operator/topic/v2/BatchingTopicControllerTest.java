/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.v2;

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
import io.strimzi.operator.topic.v2.metrics.TopicOperatorMetricsHolder;
import io.strimzi.operator.topic.v2.metrics.TopicOperatorMetricsProvider;
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
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicCollection;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static io.strimzi.api.kafka.model.common.ReplicasChangeState.ONGOING;
import static io.strimzi.api.kafka.model.common.ReplicasChangeState.PENDING;
import static io.strimzi.api.kafka.model.topic.KafkaTopic.RESOURCE_KIND;
import static io.strimzi.operator.topic.v2.TopicOperatorUtil.topicName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/**
 * This test is not intended to provide lots of coverage of the {@link BatchingTopicController}
 * (see {@link TopicControllerIT} for that), rather it aims to cover some parts that a difficult
 * to test via {@link TopicControllerIT}
 */
@ExtendWith(KafkaClusterExtension.class)
class BatchingTopicControllerTest {
    private static final Logger LOGGER = LogManager.getLogger(BatchingTopicControllerTest.class);

    static final String NAMESPACE = "ns";
    static final String NAME = "foo";

    private BatchingTopicController controller;
    private KubernetesClient client;

    private Admin[] admin = new Admin[] {null};

    private TopicOperatorMetricsHolder metrics;

    private static <T> KafkaFuture<T> interruptedFuture() throws ExecutionException, InterruptedException {
        var future = mock(KafkaFuture.class);
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
        var kt = Crds.topicOperation(client).resource(new KafkaTopicBuilder().withNewMetadata()
                .withName(NAME)
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
        var replicasChangeClient = Mockito.mock(ReplicasChangeClient.class);
        
        controller = new BatchingTopicController(config, Map.of("key", "VALUE"), admin, client, metrics, replicasChangeClient);
        List<ReconcilableTopic> batch = List.of(new ReconcilableTopic(new Reconciliation("test", "KafkaTopic", NAMESPACE, NAME), kt, topicName(kt)));
        assertThrows(InterruptedException.class, () -> controller.onUpdate(batch));
    }

    @Test
    public void shouldHandleInterruptedExceptionFromDescribeTopics(KafkaCluster cluster) throws ExecutionException, InterruptedException {
        admin[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        Admin adminSpy = Mockito.spy(admin[0]);
        var result = Mockito.mock(DescribeTopicsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).allTopicNames();
        Mockito.doReturn(Map.of(NAME, interruptedFuture())).when(result).topicNameValues();
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
        return new ConfigResource(ConfigResource.Type.TOPIC, NAME);
    }

    @Test
    public void shouldHandleInterruptedExceptionFromCreateTopics(KafkaCluster cluster) throws ExecutionException, InterruptedException {
        admin[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        Admin adminSpy = Mockito.spy(admin[0]);
        var result = Mockito.mock(CreateTopicsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).all();
        Mockito.doReturn(Map.of(NAME, interruptedFuture())).when(result).values();
        Mockito.doReturn(result).when(adminSpy).createTopics(any());

        KafkaTopic kt = createResource();
        assertOnUpdateThrowsInterruptedException(client, adminSpy, kt);
    }

    @Test
    public void shouldHandleInterruptedExceptionFromIncrementalAlterConfigs(KafkaCluster cluster) throws ExecutionException, InterruptedException {
        admin[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        admin[0].createTopics(List.of(new NewTopic(NAME, 1, (short) 1).configs(Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy")))).all().get();
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
        admin[0].createTopics(List.of(new NewTopic(NAME, 1, (short) 1))).all().get();

        Admin adminSpy = Mockito.spy(admin[0]);
        var result = Mockito.mock(CreatePartitionsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).all();
        Mockito.doReturn(Map.of(NAME, interruptedFuture())).when(result).values();
        Mockito.doReturn(result).when(adminSpy).createPartitions(any());
        KafkaTopic kt = createResource();
        assertOnUpdateThrowsInterruptedException(client, adminSpy, kt);
    }

    @Test
    public void shouldHandleInterruptedExceptionFromListReassignments(
            @BrokerCluster(numBrokers = 2)
            KafkaCluster cluster) throws ExecutionException, InterruptedException {
        admin[0] = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()));
        admin[0].createTopics(List.of(new NewTopic(NAME, 2, (short) 2))).all().get();

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
        admin[0].createTopics(List.of(new NewTopic(NAME, 1, (short) 1))).all().get();
        var kt = createResource();
        var withFinalizer = Crds.topicOperation(client).inNamespace(NAMESPACE).withName(NAME).edit(theKt -> {
            return new KafkaTopicBuilder(theKt).editOrNewMetadata().addToFinalizers(BatchingTopicController.FINALIZER).endMetadata().build();
        });
        Crds.topicOperation(client).inNamespace(NAMESPACE).withName(NAME).delete();
        var withDeletionTimestamp = Crds.topicOperation(client).inNamespace(NAMESPACE).withName(NAME).get();

        Admin adminSpy = Mockito.spy(admin[0]);
        var result = Mockito.mock(DeleteTopicsResult.class);
        Mockito.doReturn(interruptedFuture()).when(result).all();
        Mockito.doReturn(Map.of(NAME, interruptedFuture())).when(result).topicNameValues();
        Mockito.doReturn(result).when(adminSpy).deleteTopics(any(TopicCollection.TopicNameCollection.class));
        assertOnUpdateThrowsInterruptedException(client, adminSpy, withDeletionTimestamp);
    }

    // TODO kube client interrupted exceptions

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void shouldResetStatusWhenCruiseControlEnabledAndSpecReplicasReverted(boolean cruiseControlEnabled) {
        int replicationFactor = 1;

        // setup reverted change
        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(true).when(config).useFinalizer();
        Mockito.doReturn(false).when(config).enableAdditionalMetrics();
        Mockito.doReturn(cruiseControlEnabled).when(config).cruiseControlEnabled();

        var describeClusterResult = Mockito.mock(DescribeClusterResult.class);
        Mockito.doReturn(KafkaFuture.completedFuture(List.of())).when(describeClusterResult).nodes();
        var admin = Mockito.mock(Admin.class);
        Mockito.doReturn(describeClusterResult).when(admin).describeCluster();

        var currentState = Mockito.mock(BatchingTopicController.CurrentState.class);
        Mockito.doReturn(++replicationFactor).when(currentState).uniqueReplicationFactor();

        var status = new KafkaTopicStatusBuilder()
            .withReplicasChange(new ReplicasChangeStatusBuilder()
                .withMessage("Error message")
                .withState(PENDING)
                .withTargetReplicas(replicationFactor)
                .build())
            .build();

        var kafkaTopic = new KafkaTopicBuilder()
            .withNewMetadata()
            .withName(NAME)
            .withNamespace(namespace(NAMESPACE))
            .addToLabels("key", "VALUE")
            .endMetadata()
            .withNewSpec()
            .withPartitions(2)
            .withReplicas(replicationFactor)
            .endSpec()
            .withStatus(status)
            .build();

        var reconcilableTopic = new ReconcilableTopic(
            new Reconciliation("test", RESOURCE_KIND, NAMESPACE, NAME), kafkaTopic, topicName(kafkaTopic));

        var replicasChangeClient = Mockito.mock(ReplicasChangeClient.class);
        Mockito.doReturn(List.of()).when(replicasChangeClient).requestPendingChanges(anyList());
        Mockito.doReturn(List.of()).when(replicasChangeClient).requestOngoingChanges(anyList());

        // test reverted change
        var currentStatesOrError = new BatchingTopicController.PartitionedByError<>(
            List.of(new BatchingTopicController.Pair<>(reconcilableTopic, Either.ofRight(currentState))), List.of());
        new BatchingTopicController(config, Map.of("key", "VALUE"), admin, client, metrics, replicasChangeClient)
            .maybeResetReplicasChangeStatuses(currentStatesOrError);

        if (cruiseControlEnabled) {
            assertThat(kafkaTopic.getStatus().getReplicasChange(), is(nullValue()));
        } else {
            assertThat(kafkaTopic.getStatus(), is(status));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void shouldUpdateStatusWhenCruiseControlEnabledAndSpecReplicasChanged(boolean cruiseControlEnabled) {
        int replicationFactor = 1;

        // setup pending change
        var describeClusterResult = Mockito.mock(DescribeClusterResult.class);
        Mockito.doReturn(KafkaFuture.completedFuture(List.of())).when(describeClusterResult).nodes();
        var admin = Mockito.mock(Admin.class);
        Mockito.doReturn(describeClusterResult).when(admin).describeCluster();

        var currentState = Mockito.mock(BatchingTopicController.CurrentState.class);
        Mockito.doReturn(replicationFactor).when(currentState).uniqueReplicationFactor();
        
        var config = Mockito.mock(TopicOperatorConfig.class);
        Mockito.doReturn(NAMESPACE).when(config).namespace();
        Mockito.doReturn(true).when(config).useFinalizer();
        Mockito.doReturn(false).when(config).enableAdditionalMetrics();
        Mockito.doReturn(cruiseControlEnabled).when(config).cruiseControlEnabled();
        
        var inputKt = new KafkaTopicBuilder()
            .withNewMetadata()
            .withName(NAME)
            .withNamespace(namespace(NAMESPACE))
            .addToLabels("key", "VALUE")
            .endMetadata()
            .withNewSpec()
            .withPartitions(2)
            .withReplicas(++replicationFactor)
            .endSpec()
            .build();
        var inputRt = new ReconcilableTopic(
            new Reconciliation("test", RESOURCE_KIND, NAMESPACE, NAME), inputKt, topicName(inputKt));

        var outputStatus = new KafkaTopicStatusBuilder()
            .withReplicasChange(new ReplicasChangeStatusBuilder()
                .withSessionId("1aa418ca-53ed-4b93-b0a4-58413c4fc0cb")
                .withState(ONGOING)
                .withTargetReplicas(replicationFactor)
                .build())
            .build();
        var outputKt = new KafkaTopicBuilder(inputKt).withStatus(outputStatus).build();
        var outputRt = new ReconcilableTopic(
            new Reconciliation("test", "KafkaTopic", NAMESPACE, NAME), outputKt, topicName(outputKt));
        
        var replicasChangeClient = Mockito.mock(ReplicasChangeClient.class);
        Mockito.doReturn(List.of(outputRt)).when(replicasChangeClient).requestPendingChanges(anyList());
        Mockito.doReturn(List.of(outputRt)).when(replicasChangeClient).requestOngoingChanges(anyList());
        
        // test pending change
        List<ReconcilableTopic> topics = List.of(inputRt);
        Map<ReconcilableTopic, Either<TopicOperatorException, Object>> results = new HashMap<>();
        var currentStatesOrError = new BatchingTopicController.PartitionedByError<>(
            List.of(new BatchingTopicController.Pair<>(inputRt, Either.ofRight(currentState))), List.of());
        new BatchingTopicController(config, Map.of("key", "VALUE"), admin, client, metrics, replicasChangeClient)
            .maybeCheckReplicasChanges(topics, results, currentStatesOrError);

        if (cruiseControlEnabled) {
            assertThat(results.keySet().stream().findFirst().get(), is(outputRt));
        } else {
            assertThat(results.keySet(), empty());
        }
    }
}
