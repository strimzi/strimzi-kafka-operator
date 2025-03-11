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
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
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
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verifyNoInteractions;

class AlterableTopicConfigIT implements TestSeparator {
    private static final String NAMESPACE = TestUtil.namespaceName(AlterableTopicConfigIT.class);

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

    @Test
    public void shouldNotCallGetClusterConfigWhenDisabled() {
        var kafkaAdmin = Mockito.mock(Admin.class);
        var config = TopicOperatorConfig.buildFromMap(Map.of(
              TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
              TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
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
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
            TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
            TopicOperatorConfig.CRUISE_CONTROL_ENABLED.key(), "true",
            TopicOperatorConfig.CRUISE_CONTROL_HOSTNAME.key(), "my-cruise-control",
            TopicOperatorConfig.CRUISE_CONTROL_PORT.key(), "9090"
        ));

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
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
            TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
            TopicOperatorConfig.CRUISE_CONTROL_ENABLED.key(), "true",
            TopicOperatorConfig.CRUISE_CONTROL_HOSTNAME.key(), "my-cruise-control",
            TopicOperatorConfig.CRUISE_CONTROL_PORT.key(), "9090"
        ));

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
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
            TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
            TopicOperatorConfig.ALTERABLE_TOPIC_CONFIG.key(), alterableConfig
        ));

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
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
            TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
            TopicOperatorConfig.ALTERABLE_TOPIC_CONFIG.key(), alterableConfig
        ));

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
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
            TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
            TopicOperatorConfig.ALTERABLE_TOPIC_CONFIG.key(), alterableConfig
        ));

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
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
            TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
            TopicOperatorConfig.ALTERABLE_TOPIC_CONFIG.key(), alterableConfig
        ));

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
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), "localhost:1234",
            TopicOperatorConfig.NAMESPACE.key(), NAMESPACE,
            TopicOperatorConfig.ALTERABLE_TOPIC_CONFIG.key(), alterableConfig
        ));

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
