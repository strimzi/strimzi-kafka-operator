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
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.topic.cruisecontrol.CruiseControlHandler;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsHolder;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsProvider;
import io.strimzi.operator.topic.model.ReconcilableTopic;
import io.strimzi.test.container.StrimziKafkaCluster;
import io.strimzi.test.interfaces.TestSeparator;
import io.strimzi.test.mockkube3.MockKube3;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

class InterruptedExceptionsIT implements TestSeparator {
    private static final String NAMESPACE = TestUtil.namespaceName(InterruptedExceptionsIT.class);

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
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
            TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
            TopicOperatorConfig.USE_FINALIZERS.key(), "true",
            TopicOperatorConfig.ENABLE_ADDITIONAL_METRICS.key(), "false"
        ));

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
}
