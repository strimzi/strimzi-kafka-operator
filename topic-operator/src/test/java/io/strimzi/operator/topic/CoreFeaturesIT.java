/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.ResourceAnnotations;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicBuilder;
import io.strimzi.api.kafka.model.topic.KafkaTopicStatus;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.topic.model.KubeRef;
import io.strimzi.operator.topic.model.TopicOperatorException;
import io.strimzi.test.TestUtils;
import io.strimzi.test.container.StrimziKafkaCluster;
import io.strimzi.test.interfaces.TestSeparator;
import io.strimzi.test.mockkube3.MockKube3;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

@SuppressWarnings("checkstyle:ClassFanOutComplexity")
class CoreFeaturesIT implements TestSeparator {
    private static final Logger LOGGER = LogManager.getLogger(CoreFeaturesIT.class);

    private static final String NAMESPACE = TestUtil.namespaceName(CoreFeaturesIT.class);
    private static final Map<String, String> SELECTOR = Map.of("foo", "FOO", "bar", "BAR");
    private static final Map<String, Object> TEST_TOPIC_CONFIG = Map.of(
        TopicConfig.CLEANUP_POLICY_CONFIG, List.of("compact"),
        TopicConfig.COMPRESSION_TYPE_CONFIG, "producer",
        TopicConfig.FLUSH_MS_CONFIG, 1234L,
        TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, 1234,
        TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, 0.6,
        TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, true
    );

    private static MockKube3 mockKube;
    private static KubernetesClient kubernetesClient;
    
    TopicOperatorMain operator;
    Stack<String> namespaces = new Stack<>();
    private TopicOperatorConfig operatorConfig;
    private StrimziKafkaCluster kafkaCluster;
    private Admin kafkaAdminClientOp; // operator's client
    private Admin kafkaAdminClient; // test client

    @BeforeAll
    public static void beforeAll() {
        mockKube = new MockKube3.MockKube3Builder()
            .withKafkaTopicCrd()
            .withDeletionController()
            .withNamespaces(NAMESPACE)
            .build();
        mockKube.start();
        kubernetesClient = mockKube.client();
        TestUtil.setupKubeCluster(kubernetesClient, NAMESPACE);
    }

    @AfterAll
    public static void afterAll() {
        mockKube.stop();
    }

    @AfterEach
    public void afterEach() {
        if (operator != null) {
            assertTrue(operator.queue.isAlive());
            assertTrue(operator.queue.isReady());
        }

        if (operator != null) {
            operator.stop();
            operator = null;
        }

        if (kafkaAdminClient != null) {
            kafkaAdminClient.close();
            kafkaAdminClient = null;
        }

        if (kafkaCluster != null) {
            kafkaCluster.stop();
            kafkaCluster = null;
        }

        while (!namespaces.isEmpty()) {
            TestUtil.cleanupNamespace(kubernetesClient, namespaces.pop());
        }
    }

    private void startKafkaCluster(int brokersNum, int internalTopicReplicationFactor, Map<String, String> additionalKafkaConfiguration) {
        kafkaCluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withKraft()
                .withNumberOfBrokers(brokersNum)
                .withInternalTopicReplicationFactor(internalTopicReplicationFactor)
                .withAdditionalKafkaConfiguration(additionalKafkaConfiguration)
                .withSharedNetwork()
                .build();
        kafkaCluster.start();
    }

    private String createNamespace(String name) {
        namespaces.push(name);
        TestUtil.createNamespace(kubernetesClient, name);
        return name;
    }

    private static Predicate<KafkaTopic> hasConditionMatching(String description,
                                                              Predicate<Condition> conditionPredicate) {
        return new Predicate<>() {
            @Override
            public boolean test(KafkaTopic kt) {
                return kt.getStatus() != null
                    && kt.getMetadata() != null
                    && kt.getStatus().getConditions() != null
                    && kt.getStatus().getConditions().stream()
                    .anyMatch(conditionPredicate);
            }

            public String toString() {
                return "status.condition which matches " + description;
            }
        };
    }

    private static Predicate<KafkaTopic> isReconcilatedAndHasConditionMatching(String description,
                                                                               Predicate<Condition> conditionPredicate) {
        return new Predicate<>() {
            @Override
            public boolean test(KafkaTopic kt) {
                return kt.getStatus() != null
                    && kt.getMetadata() != null
                    && kt.getMetadata().getGeneration().equals(kt.getStatus().getObservedGeneration())
                    && kt.getStatus().getConditions() != null
                    && kt.getStatus().getConditions().stream()
                    .anyMatch(conditionPredicate);
            }

            public String toString() {
                return "metadata.generation == status.observedGeneration and a status.condition which matches " + description;
            }
        };
    }

    private static Predicate<KafkaTopic> isPausedAndHasConditionMatching(String description,
                                                                         Predicate<Condition> conditionPredicate) {
        return new Predicate<>() {
            @Override
            public boolean test(KafkaTopic kt) {
                return kt.getStatus() != null
                    && kt.getMetadata() != null
                    && kt.getMetadata().getGeneration() != null
                    && kt.getStatus().getConditions() != null
                    && kt.getStatus().getConditions().stream()
                    .anyMatch(conditionPredicate);
            }

            public String toString() {
                return "status.generation and status.condition which matches " + description;
            }
        };
    }

    private static Predicate<KafkaTopic> readyIsTrue() {
        Predicate<Condition> conditionPredicate = condition ->
            "Ready".equals(condition.getType())
                && "True".equals(condition.getStatus());
        return isReconcilatedAndHasConditionMatching("Ready=True", conditionPredicate);
    }

    private static Predicate<KafkaTopic> pausedIsTrue() {
        Predicate<Condition> conditionPredicate = condition ->
            "ReconciliationPaused".equals(condition.getType())
                && "True".equals(condition.getStatus());
        return isPausedAndHasConditionMatching("ReconciliationPaused=True", conditionPredicate);
    }

    private static Predicate<KafkaTopic> readyIsFalse() {
        Predicate<Condition> conditionPredicate = condition ->
            "Ready".equals(condition.getType())
                && "False".equals(condition.getStatus());
        return isReconcilatedAndHasConditionMatching("Ready=False", conditionPredicate);
    }

    private static Predicate<KafkaTopic> readyIsFalseAndReasonIs(String requiredReason, String requiredMessage) {
        Predicate<Condition> conditionPredicate = condition ->
            "Ready".equals(condition.getType())
                && "False".equals(condition.getStatus())
                && requiredReason.equals(condition.getReason())
                && (requiredMessage == null || requiredMessage.equals(condition.getMessage()));
        String description = "Ready=False and Reason=" + requiredReason;
        if (requiredMessage != null) {
            description += " and Message=" + requiredMessage;
        }
        return isReconcilatedAndHasConditionMatching(description, conditionPredicate);
    }

    private static Predicate<KafkaTopic> readyIsTrueOrFalse() {
        return typeIsTrueOrFalse("Ready");
    }

    private static Predicate<KafkaTopic> unmanagedIsTrueOrFalse() {
        return typeIsTrueOrFalse("Unmanaged");
    }

    private static Predicate<KafkaTopic> typeIsTrueOrFalse(String type) {
        Predicate<Condition> conditionPredicate = condition ->
            type.equals(condition.getType())
                    && "True".equals(condition.getStatus())
                    || "False".equals(condition.getStatus());
        return isReconcilatedAndHasConditionMatching(type + "=True or False", conditionPredicate);
    }

    private static Predicate<KafkaTopic> unmanagedIsTrue() {
        Predicate<Condition> conditionPredicate = condition ->
            "Unmanaged".equals(condition.getType())
                && "True".equals(condition.getStatus());
        return hasConditionMatching("Unmanaged=True", conditionPredicate);
    }

    private KafkaTopic waitUntil(KafkaTopic kt, Predicate<KafkaTopic> condition) {
        Resource<KafkaTopic> resource = Crds.topicOperation(kubernetesClient).resource(kt);
        return TestUtil.waitUntilCondition(resource, condition);
    }
    
    private void maybeStartOperator(TopicOperatorConfig config) {
        if (kafkaAdminClient == null) {
            Map<String, Object> testConfig = config.adminClientConfig();
            testConfig.replace(AdminClientConfig.CLIENT_ID_CONFIG, config.clientId() + "-test");
            kafkaAdminClient = Admin.create(testConfig);
        }
        if (kafkaAdminClientOp == null) {
            Map<String, Object> adminConfig = config.adminClientConfig();
            adminConfig.replace(AdminClientConfig.CLIENT_ID_CONFIG, config.clientId() + "-operator");
            kafkaAdminClientOp = Admin.create(adminConfig);
        }
        if (operator == null) {
            operatorConfig = config;
            var kubernetesClientOp = new KubernetesClientBuilder().withConfig(kubernetesClient.getConfiguration()).build();
            operator = new TopicOperatorMain(config, kubernetesClientOp, kafkaAdminClientOp);
            assertFalse(operator.queue.isAlive());
            assertFalse(operator.queue.isReady());
            operator.start();
        }
    }

    private void assertNotExistsInKafka(String expectedTopicName) throws InterruptedException {
        try {
            kafkaAdminClient.describeTopics(Set.of(expectedTopicName)).topicNameValues().get(expectedTopicName).get();
            fail("Expected topic not to exist in Kafka, but describeTopics({" + expectedTopicName + "}) succeeded");
        } catch (ExecutionException e) {
            assertInstanceOf(UnknownTopicOrPartitionException.class, e.getCause());
        }
    }

    private void waitNotExistsInKafka(String expectedTopicName) throws InterruptedException, TimeoutException, ExecutionException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            try {
                kafkaAdminClient.describeTopics(Set.of(expectedTopicName)).topicNameValues().get(expectedTopicName).get();
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                    return;
                }
                throw e;
            }
            //noinspection BusyWait
            Thread.sleep(100L);
        }
        throw new TimeoutException("Waiting for " + expectedTopicName + " to not exist in Kafka");
    }

    private static Set<Integer> replicationFactors(TopicDescription topicDescription) {
        return topicDescription.partitions().stream().map(replica -> replica.replicas().size()).collect(Collectors.toSet());
    }

    private static int numPartitions(TopicDescription topicDescription) {
        return topicDescription.partitions().size();
    }

    private Map<String, String> topicConfigMap(String topicName) throws InterruptedException, ExecutionException {
        ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        Config topicConfig = null;
        do {
            try {
                topicConfig = kafkaAdminClient.describeConfigs(Set.of(topicResource)).all().get().get(topicResource);
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                    throw e;
                }
            }
        } while (topicConfig == null);
        return topicConfig.entries().stream()
            .filter(ce -> ce.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG)
            .collect(Collectors.toMap(
                ConfigEntry::name,
                ConfigEntry::value
            ));
    }

    private static KafkaTopic kafkaTopic(String ns,
                                         String metadataName,
                                         Boolean managed,
                                         String topicName,
                                         Integer partitions,
                                         Integer replicas) {
        return kafkaTopic(ns, metadataName, SELECTOR, null, managed, topicName, partitions, replicas, null);
    }

    private static KafkaTopic kafkaTopic(String ns,
                                         String metadataName,
                                         Map<String, String> labels,
                                         Map<String, String> annotations,
                                         Boolean managed,
                                         String topicName,
                                         Integer partitions,
                                         Integer replicas,
                                         Map<String, Object> configs) {

        var metadataBuilder = new KafkaTopicBuilder()
            .withNewMetadata()
            .withName(metadataName)
            .withNamespace(ns)
            .withLabels(labels)
            .withAnnotations(annotations);
        if (managed != null) {
            metadataBuilder = metadataBuilder.addToAnnotations(TopicOperatorUtil.MANAGED, managed.toString());
        }
        return metadataBuilder.endMetadata()
            .withNewSpec()
            .withTopicName(topicName)
            .withPartitions(partitions)
            .withReplicas(replicas)
            .withConfig(configs)
            .endSpec()
            .build();
    }

    private static KafkaTopic kafkaTopicWithNoSpec(String metadataName, boolean spec) {
        var builder = new KafkaTopicBuilder()
            .withNewMetadata()
            .withName(metadataName)
            .withNamespace(NAMESPACE)
            .withLabels(SELECTOR)
            .addToAnnotations(TopicOperatorUtil.MANAGED, "true")
            .endMetadata();
        if (spec) {
            builder = builder.editOrNewSpec().endSpec();
        }
        return builder.build();
    }

    static List<KafkaTopic> managedKafkaTopics() {
        var topicName = "topic" + System.nanoTime();

        return List.of(
                kafkaTopic(NAMESPACE, topicName + "a", true, topicName + "a", 2, 1),
                kafkaTopic(NAMESPACE, topicName + "b", true, null, 2, 1),
                kafkaTopic(NAMESPACE, topicName + "c", true, topicName + "c".toUpperCase(Locale.ROOT), 2, 1),
                kafkaTopic(NAMESPACE, topicName + "d", null, topicName + "d", 2, 1),
                kafkaTopic(NAMESPACE, topicName + "e", null, null, 2, 1),
                kafkaTopic(NAMESPACE, topicName + "f", null, topicName + "f".toUpperCase(Locale.ROOT), 2, 1),
                // With a superset of the selector mappings
                kafkaTopic(NAMESPACE, topicName + "g", Map.of("foo", "FOO", "bar", "BAR", "quux", "QUUX"), null, true, topicName + "g", 2, 1, null)
        );
    }

    static List<KafkaTopic> managedKafkaTopicsWithConfigs() {
        var topicName = "topic" + System.nanoTime();

        return List.of(
                kafkaTopic(NAMESPACE, topicName + "a", SELECTOR, null, true, topicName + "a", 2, 1, TEST_TOPIC_CONFIG),
                kafkaTopic(NAMESPACE, topicName + "b", SELECTOR, null, true, null, 2, 1, TEST_TOPIC_CONFIG),
                kafkaTopic(NAMESPACE, topicName + "c", SELECTOR, null, true, topicName + "c".toUpperCase(Locale.ROOT), 2, 1, TEST_TOPIC_CONFIG),
                kafkaTopic(NAMESPACE, topicName + "d", SELECTOR, null, null, topicName + "d", 2, 1, TEST_TOPIC_CONFIG),
                kafkaTopic(NAMESPACE, topicName + "e", SELECTOR, null, null, null, 2, 1, TEST_TOPIC_CONFIG),
                kafkaTopic(NAMESPACE, topicName + "f", SELECTOR, null, null, topicName + "f".toUpperCase(Locale.ROOT), 2, 1, TEST_TOPIC_CONFIG)
        );
    }

    static List<KafkaTopic> unselectedKafkaTopics() {
        var topicName = "topic" + System.nanoTime();
        return List.of(
                kafkaTopic(NAMESPACE, topicName + "-a", Map.of(), null, true, topicName + "-a", 2, 1, null),
                kafkaTopic(NAMESPACE, topicName + "-b", Map.of("foo", "FOO"), null, true, topicName + "-b", 2, 1, null),
                kafkaTopic(NAMESPACE, topicName + "-c", Map.of("quux", "QUUX"), null, true, null, 2, 1, null)
        );
    }

    private void assertCreateSuccess(KafkaTopic kt, KafkaTopic reconciled) throws InterruptedException, ExecutionException, TimeoutException {
        assertCreateSuccess(kt, reconciled, Map.of());
    }

    private void assertCreateSuccess(KafkaTopic kt, KafkaTopic reconciled,
                                     Map<String, String> expectedConfigs) throws InterruptedException, ExecutionException, TimeoutException {
        assertCreateSuccess(kt, reconciled,
            kt.getSpec().getPartitions(),
            kt.getSpec().getReplicas(),
            expectedConfigs);
    }

    private void assertCreateSuccess(KafkaTopic kt, KafkaTopic reconciled,
                                     int expectedPartitions,
                                     int expectedReplicas,
                                     Map<String, String> expectedConfigs) throws InterruptedException, ExecutionException, TimeoutException {
        waitUntil(kt, readyIsTrue());
        var expectedTopicName = TopicOperatorUtil.topicName(kt);

        // Check updates to the KafkaTopic
        assertNotNull(reconciled.getMetadata().getFinalizers());
        assertEquals(operatorConfig.useFinalizer(), reconciled.getMetadata().getFinalizers().contains(KubernetesHandler.FINALIZER_STRIMZI_IO_TO));
        assertEquals(expectedTopicName, reconciled.getStatus().getTopicName());
        assertNotNull(reconciled.getStatus().getTopicId());

        // Check topic in Kafka
        var topicDescription = awaitTopicDescription(expectedTopicName);
        assertEquals(expectedPartitions, numPartitions(topicDescription));
        assertEquals(Set.of(expectedReplicas), replicationFactors(topicDescription));
        assertEquals(expectedConfigs, topicConfigMap(expectedTopicName));
    }

    private KafkaTopic createTopic(StrimziKafkaCluster kc, KafkaTopic kt) {
        return createTopic(kc, kt, TopicOperatorUtil.isManaged(kt) ? readyIsTrueOrFalse() : unmanagedIsTrue());
    }

    private KafkaTopic createTopic(StrimziKafkaCluster kc, KafkaTopic kt, Predicate<KafkaTopic> condition) {
        String ns = createNamespace(kt.getMetadata().getNamespace());
        maybeStartOperator(topicOperatorConfig(ns, kc));

        // Create resource and await readiness
        var created = Crds.topicOperation(kubernetesClient).resource(kt).create();
        LOGGER.info("Test created KafkaTopic {} with resourceVersion {}",
            created.getMetadata().getName(), TopicOperatorUtil.resourceVersion(created));
        return waitUntil(created, condition);
    }

    private List<KafkaTopic> createTopicsConcurrently(StrimziKafkaCluster kc, KafkaTopic... kts) throws InterruptedException {
        if (kts == null || kts.length == 0) {
            throw new IllegalArgumentException("You need pass at least one topic to be created");
        }
        String ns = createNamespace(kts[0].getMetadata().getNamespace());
        maybeStartOperator(topicOperatorConfig(ns, kc));
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        CountDownLatch latch = new CountDownLatch(kts.length);
        List<KafkaTopic> result = new ArrayList<>();
        for (KafkaTopic kt : kts) {
            executor.submit(() -> {
                try {
                    var created = Crds.topicOperation(kubernetesClient).resource(kt).create();
                    LOGGER.info("Test created KafkaTopic {} with creationTimestamp {}",
                        created.getMetadata().getName(),
                        created.getMetadata().getCreationTimestamp());
                    var reconciled = waitUntil(created, readyIsTrueOrFalse());
                    result.add(reconciled);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                latch.countDown();
            });
        }
        latch.await(1, TimeUnit.MINUTES);
        try {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            if (!executor.isTerminated()) {
                executor.shutdownNow();
            }
        }
        return result;
    }

    private KafkaTopic pauseTopic(String namespace, String topicName) {
        var current = Crds.topicOperation(kubernetesClient).inNamespace(namespace).withName(topicName).get();
        var kafkaTopic = Crds.topicOperation(kubernetesClient).resource(new KafkaTopicBuilder(current)
            .editMetadata()
                .withAnnotations(Map.of(ResourceAnnotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true"))
            .endMetadata()
            .build()).update();
        LOGGER.info("Test paused KafkaTopic {} with resourceVersion {}",
            kafkaTopic.getMetadata().getName(), TopicOperatorUtil.resourceVersion(kafkaTopic));
        return waitUntil(kafkaTopic, pausedIsTrue());
    }

    private KafkaTopic unpauseTopic(String namespace, String topicName) {
        var current = Crds.topicOperation(kubernetesClient).inNamespace(namespace).withName(topicName).get();
        var kafkaTopic = Crds.topicOperation(kubernetesClient).resource(new KafkaTopicBuilder(current)
            .editMetadata()
                .withAnnotations(Map.of(ResourceAnnotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "false"))
            .endMetadata()
            .build()).update();
        LOGGER.info("Test unpaused KafkaTopic {} with resourceVersion {}",
            kafkaTopic.getMetadata().getName(), TopicOperatorUtil.resourceVersion(kafkaTopic));
        return waitUntil(kafkaTopic, readyIsTrue());
    }

    private KafkaTopic unmanageTopic(String namespace, String topicName) {
        var current = Crds.topicOperation(kubernetesClient).inNamespace(namespace).withName(topicName).get();
        var kafkaTopic = Crds.topicOperation(kubernetesClient).resource(new KafkaTopicBuilder(current)
            .editMetadata()
                .withAnnotations(Map.of(TopicOperatorUtil.MANAGED, "false"))
            .endMetadata()
            .build()).update();
        LOGGER.info("Test unmanaged KafkaTopic {} with resourceVersion {}",
            kafkaTopic.getMetadata().getName(), TopicOperatorUtil.resourceVersion(kafkaTopic));
        return waitUntil(kafkaTopic, unmanagedIsTrue());
    }

    private KafkaTopic manageTopic(String namespace, String topicName) {
        var current = Crds.topicOperation(kubernetesClient).inNamespace(namespace).withName(topicName).get();
        var kafkaTopic = Crds.topicOperation(kubernetesClient).resource(new KafkaTopicBuilder(current)
            .editMetadata()
                .withAnnotations(Map.of(TopicOperatorUtil.MANAGED, "true"))
            .endMetadata()
            .build()).update();
        LOGGER.info("Test managed KafkaTopic {} with resourceVersion {}",
            kafkaTopic.getMetadata().getName(), TopicOperatorUtil.resourceVersion(kafkaTopic));
        return waitUntil(kafkaTopic, readyIsTrue());
    }

    private TopicDescription awaitTopicDescription(String expectedTopicName) throws InterruptedException, ExecutionException, TimeoutException {
        long deadline = System.nanoTime() + 30_000_000_000L;
        TopicDescription td = null;
        do {
            try {
                td = kafkaAdminClient.describeTopics(Set.of(expectedTopicName)).allTopicNames().get().get(expectedTopicName);
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                    throw e;
                }
            }
            if (System.nanoTime() > deadline) {
                throw new TimeoutException();
            }
        } while (td == null);
        return td;
    }

    private void assertUnknownTopic(String expectedTopicName) throws ExecutionException, InterruptedException {
        try {
            kafkaAdminClient.describeTopics(Set.of(expectedTopicName)).allTopicNames().get();
            fail("Expected topic '" + expectedTopicName + "' to not exist");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                return;
            }
            throw e;
        }
    }

    private KafkaTopic createTopicAndAssertSuccess(StrimziKafkaCluster kc, KafkaTopic kt)
        throws ExecutionException, InterruptedException, TimeoutException {
        var created = createTopic(kc, kt);
        assertCreateSuccess(kt, created);
        return created;
    }

    @Test
    public void shouldCreateTopicInKafkaWhenManagedTopicCreatedInKube() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        List<KafkaTopic> topics = managedKafkaTopics();

        for (KafkaTopic kt : topics) {
            createTopicAndAssertSuccess(kafkaCluster, kt);
        }
    }

    @Test
    public void shouldCreateTopicInKafkaWhenKafkaTopicHasOnlyPartitions() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false", "num.partitions", "4", "default.replication.factor", "1"));

        KafkaTopic kt = new KafkaTopicBuilder()
            .withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName("my-topic")
                .withLabels(SELECTOR)
            .endMetadata()
            .withNewSpec()
                .withPartitions(5)
            .endSpec()
            .build();
        var created = createTopic(kafkaCluster, kt);
        assertCreateSuccess(kt, created, 5, 1, Map.of());
    }

    @Test
    public void shouldCreateTopicInKafkaWhenKafkaTopicHasOnlyReplicas() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false", "num.partitions", "4", "default.replication.factor", "1"));

        KafkaTopic kt = new KafkaTopicBuilder()
            .withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName("my-topic")
                .withLabels(SELECTOR)
            .endMetadata()
            .withNewSpec()
                .withReplicas(1)
            .endSpec()
            .build();
        var created = createTopic(kafkaCluster, kt);
        assertCreateSuccess(kt, created, 4, 1, Map.of());
    }

    @Test
    public void shouldCreateTopicInKafkaWhenKafkaTopicHasOnlyConfigs() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false", "num.partitions", "4", "default.replication.factor", "1"));

        KafkaTopic kt = new KafkaTopicBuilder()
            .withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName("my-topic")
                .withLabels(SELECTOR)
            .endMetadata()
            .withNewSpec()
                .addToConfig(TopicConfig.FLUSH_MS_CONFIG, "1000")
            .endSpec()
            .build();
        var created = createTopic(kafkaCluster, kt);
        assertCreateSuccess(kt, created, 4, 1, Map.of(TopicConfig.FLUSH_MS_CONFIG, "1000"));
    }

    @Test
    public void shouldNotCreateTopicInKafkaWhenUnmanagedTopicCreatedInKube() throws ExecutionException, InterruptedException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        List<KafkaTopic> topics = List.of(
                kafkaTopic(NAMESPACE, "unmanaged-topic-a", false, "unmanaged-topic-a", 2, 1),
                kafkaTopic(NAMESPACE, "unmanaged-topic-b", false, null, 2, 1),
                kafkaTopic(NAMESPACE, "unmanaged-topic-c", false, "unmanaged-topic-c".toUpperCase(Locale.ROOT), 2, 1)
        );

        for (KafkaTopic kafkaTopic : topics) {
            createTopic(kafkaCluster, kafkaTopic);
            assertNotExistsInKafka(TopicOperatorUtil.topicName(kafkaTopic));
        }
    }

    @Test
    public void shouldNotCreateTopicInKafkaWhenUnselectedTopicCreatedInKube() throws InterruptedException, TimeoutException {
        // The difference between unmanaged and unselected is the former means the operator doesn't touch it
        // (presumably it's intended for another operator instance), but the latter does get a status update

        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        List<KafkaTopic> topics = unselectedKafkaTopics();

        for (KafkaTopic kt : topics) {
            // given
            String ns = createNamespace(kt.getMetadata().getNamespace());
            maybeStartOperator(topicOperatorConfig(ns, kafkaCluster));

            // when

            // then
            try (var ignored = LogCaptor.logMessageMatches(BatchingTopicController.LOGGER,
                    org.apache.logging.log4j.Level.DEBUG,
                    "Ignoring KafkaTopic .*? not selected by selector",
                    5L,
                    TimeUnit.SECONDS)) {
                var created = Crds.topicOperation(kubernetesClient).resource(kt).create();
                LOGGER.info("Test created KafkaTopic {} with resourceVersion {}",
                        created.getMetadata().getName(), TopicOperatorUtil.resourceVersion(created));
            }
            KafkaTopic kafkaTopic = Crds.topicOperation(kubernetesClient).inNamespace(ns).withName(kt.getMetadata().getName()).get();
            assertNull(kafkaTopic.getStatus());
        }
    }

    @Test
    public void shouldNotUpdateTopicInKafkaWhenKafkaTopicBecomesUnselected() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        KafkaTopic kt = kafkaTopic(NAMESPACE, "not-update-unselected", true, "not-update-unselected", 2, 1);

        Map<String, String> unmatchedLabels = Map.of("foo", "FOO");
        assertFalse(BatchingTopicController.matchesSelector(SELECTOR, unmatchedLabels));

        // given
        var expectedTopicName = TopicOperatorUtil.topicName(kt);
        KafkaTopic unmanaged;
        try (var ignored = LogCaptor.logMessageMatches(BatchingTopicController.LOGGER,
                org.apache.logging.log4j.Level.DEBUG,
                "Ignoring KafkaTopic .*? not selected by selector",
                5L,
                TimeUnit.SECONDS)) {
            createTopicAndAssertSuccess(kafkaCluster, kt);
            assertTrue(operator.controller.topicRefs.containsKey(expectedTopicName)
                            || operator.controller.topicRefs.containsKey(expectedTopicName.toUpperCase(Locale.ROOT)),
                    "Expect selected resource to be present in topics map");

            // when
            LOGGER.debug("##Modifying");
            unmanaged = TestUtil.changeTopic(kubernetesClient, kt, theKt -> {
                theKt.getMetadata().setLabels(unmatchedLabels);
                theKt.getSpec().setPartitions(3);
                return theKt;
            });

            // then
            LOGGER.debug("##Checking");
        }
        assertNotNull(unmanaged.getMetadata().getFinalizers());
        assertTrue(unmanaged.getMetadata().getFinalizers().contains(KubernetesHandler.FINALIZER_STRIMZI_IO_TO));
        assertNotNull(unmanaged.getStatus().getTopicName(), "Expect status.topicName to be unchanged from post-creation state");

        var topicDescription = awaitTopicDescription(expectedTopicName);
        assertEquals(kt.getSpec().getPartitions(), numPartitions(topicDescription));
        assertEquals(Set.of(kt.getSpec().getReplicas()), replicationFactors(topicDescription));
        assertEquals(Map.of(), topicConfigMap(expectedTopicName));

        Map<String, List<KubeRef>> topicsRefs = new HashMap<>(operator.controller.topicRefs);
        assertFalse(topicsRefs.containsKey(expectedTopicName)
                        || topicsRefs.containsKey(expectedTopicName.toUpperCase(Locale.ROOT)),
                "Transition to a non-selected resource should result in removal from topics map: " + topicsRefs);
    }

    @Test
    public void shouldUpdateTopicInKafkaWhenKafkaTopicBecomesSelected() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        List<KafkaTopic> topics = unselectedKafkaTopics();

        for (KafkaTopic kt : topics) {
            Map<String, String> unmatchedLabels = kt.getMetadata().getLabels();
            assertFalse(BatchingTopicController.matchesSelector(SELECTOR, unmatchedLabels));

            // given
            var ns = createNamespace(kt.getMetadata().getNamespace());
            var expectedTopicName = TopicOperatorUtil.topicName(kt);
            maybeStartOperator(topicOperatorConfig(ns, kafkaCluster));

            KafkaTopic created;
            try (var ignored = LogCaptor.logMessageMatches(BatchingTopicController.LOGGER,
                    org.apache.logging.log4j.Level.DEBUG,
                    "Ignoring KafkaTopic .*? not selected by selector",
                    5L,
                    TimeUnit.SECONDS)) {
                created = Crds.topicOperation(kubernetesClient).resource(kt).create();
                LOGGER.info("Test created KafkaTopic {} with resourceVersion {}",
                        created.getMetadata().getName(), TopicOperatorUtil.resourceVersion(created));
            }
            assertUnknownTopic(expectedTopicName);
            assertNull(created.getStatus(), "Expect status not to be set");
            assertTrue(created.getMetadata().getFinalizers().isEmpty());
            assertFalse(operator.controller.topicRefs.containsKey(expectedTopicName)
                            || operator.controller.topicRefs.containsKey(expectedTopicName.toUpperCase(Locale.ROOT)),
                    "Expect unselected resource to be absent from topics map");

            // when
            var managed = modifyTopicAndAwait(kt,
                    theKt -> {
                        theKt.getMetadata().setLabels(SELECTOR);
                        theKt.getSpec().setPartitions(3);
                        return theKt;
                    },
                    readyIsTrue());

            // then
            assertTrue(operator.controller.topicRefs.containsKey(expectedTopicName)
                            || operator.controller.topicRefs.containsKey(expectedTopicName.toUpperCase(Locale.ROOT)),
                    "Expect selected resource to be present in topics map");

            assertNotNull(managed.getMetadata().getFinalizers());
            assertTrue(managed.getMetadata().getFinalizers().contains(KubernetesHandler.FINALIZER_STRIMZI_IO_TO));
            assertNotNull(managed.getStatus().getTopicName(), "Expect status.topicName to be unchanged from post-creation state");
            var topicDescription = awaitTopicDescription(expectedTopicName);
            assertEquals(3, numPartitions(topicDescription));

            assertTrue(operator.controller.topicRefs.containsKey(expectedTopicName)
                            || operator.controller.topicRefs.containsKey(expectedTopicName.toUpperCase(Locale.ROOT)),
                    "Expect selected resource to be present in topics map");
        }
    }

    private void shouldUpdateTopicInKafkaWhenConfigChangedInKube(
        StrimziKafkaCluster kc, KafkaTopic kt, UnaryOperator<KafkaTopic> changer, UnaryOperator<Map<String, String>> expectedChangedConfigs
    ) throws ExecutionException, InterruptedException, TimeoutException {
        // given
        var expectedTopicName = TopicOperatorUtil.topicName(kt);
        var expectedCreateConfigs = TEST_TOPIC_CONFIG.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue() instanceof List
                ? String.join(",", (List) e.getValue()) : String.valueOf(e.getValue())));
        Map<String, String> expectedConfigs = expectedChangedConfigs.apply(expectedCreateConfigs);
        assertNotEquals(expectedCreateConfigs, expectedConfigs);

        var created = createTopic(kc, kt);
        assertCreateSuccess(kt, created, expectedCreateConfigs);

        // when
        modifyTopicAndAwait(kt, changer, readyIsTrue());

        // then with dynamic wait (ensuring that race-condition never happen)
        TestUtils.waitFor("config update", Duration.ofMillis(500).toMillis(), Duration.ofSeconds(30).toMillis(), () -> {
            try {
                return topicConfigMap(expectedTopicName).equals(expectedConfigs);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void shouldUpdateTopicInKafkaWhenStringConfigChangedInKube() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        List<KafkaTopic> topics = managedKafkaTopicsWithConfigs();

        for (KafkaTopic kt : topics) {
            shouldUpdateTopicInKafkaWhenConfigChangedInKube(kafkaCluster, kt,
                    CoreFeaturesIT::setSnappyCompression,
                    expectedCreateConfigs -> {
                        Map<String, String> expectedUpdatedConfigs = new LinkedHashMap<>(expectedCreateConfigs);
                        expectedUpdatedConfigs.put(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy");
                        return expectedUpdatedConfigs;
                    });
        }
    }

    @Test
    public void shouldUpdateTopicInKafkaWhenIntConfigChangedInKube() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        List<KafkaTopic> topics = managedKafkaTopicsWithConfigs();

        for (KafkaTopic kt : topics) {
            shouldUpdateTopicInKafkaWhenConfigChangedInKube(kafkaCluster, kt,
                    theKt -> {
                        theKt.getSpec().getConfig().put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, 5678);
                        return theKt;
                    },
                    expectedCreateConfigs -> {
                        Map<String, String> expectedUpdatedConfigs = new LinkedHashMap<>(expectedCreateConfigs);
                        expectedUpdatedConfigs.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, "5678");
                        return expectedUpdatedConfigs;
                    });
        }
    }

    @Test
    public void shouldUpdateTopicInKafkaWhenLongConfigChangedInKube() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        List<KafkaTopic> topics = managedKafkaTopicsWithConfigs();

        for (KafkaTopic kt : topics) {
            shouldUpdateTopicInKafkaWhenConfigChangedInKube(kafkaCluster, kt,
                    theKt -> {
                        theKt.getSpec().getConfig().put(TopicConfig.FLUSH_MS_CONFIG, 9876L);
                        return theKt;
                    },
                    expectedCreateConfigs -> {
                        Map<String, String> expectedUpdatedConfigs = new LinkedHashMap<>(expectedCreateConfigs);
                        expectedUpdatedConfigs.put(TopicConfig.FLUSH_MS_CONFIG, "9876");
                        return expectedUpdatedConfigs;
                    });
        }
    }

    @Test
    public void shouldUpdateTopicInKafkaWhenDoubleConfigChangedInKube() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        List<KafkaTopic> topics = managedKafkaTopicsWithConfigs();

        for (KafkaTopic kt : topics) {
            shouldUpdateTopicInKafkaWhenConfigChangedInKube(kafkaCluster, kt,
                    theKt -> {
                        theKt.getSpec().getConfig().put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, 0.1);
                        return theKt;
                    },
                    expectedCreateConfigs -> {
                        Map<String, String> expectedUpdatedConfigs = new LinkedHashMap<>(expectedCreateConfigs);
                        expectedUpdatedConfigs.put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.1");
                        return expectedUpdatedConfigs;
                    });
        }
    }

    @Test
    public void shouldUpdateTopicInKafkaWhenBooleanConfigChangedInKube() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        List<KafkaTopic> topics = managedKafkaTopicsWithConfigs();

        for (KafkaTopic kt : topics) {
            shouldUpdateTopicInKafkaWhenConfigChangedInKube(kafkaCluster, kt,
                    theKt -> {
                        theKt.getSpec().getConfig().put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, false);
                        return theKt;
                    },
                    expectedCreateConfigs -> {
                        Map<String, String> expectedUpdatedConfigs = new LinkedHashMap<>(expectedCreateConfigs);
                        expectedUpdatedConfigs.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false");
                        return expectedUpdatedConfigs;
                    });
        }
    }

    @Test
    public void shouldUpdateTopicInKafkaWhenListConfigChangedInKube() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        List<KafkaTopic> topics = managedKafkaTopicsWithConfigs();

        for (KafkaTopic kt : topics) {
            shouldUpdateTopicInKafkaWhenConfigChangedInKube(kafkaCluster, kt,
                    theKt -> {
                        theKt.getSpec().getConfig().put(TopicConfig.CLEANUP_POLICY_CONFIG, List.of("compact", "delete"));
                        return theKt;
                    },
                    expectedCreateConfigs -> {
                        Map<String, String> expectedUpdatedConfigs = new LinkedHashMap<>(expectedCreateConfigs);
                        expectedUpdatedConfigs.put(TopicConfig.CLEANUP_POLICY_CONFIG, "compact,delete");
                        return expectedUpdatedConfigs;
                    });
        }
    }

    @Test
    public void shouldUpdateTopicInKafkaWhenConfigRemovedInKube() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        List<KafkaTopic> topics = managedKafkaTopicsWithConfigs();

        for (KafkaTopic kt : topics) {
            shouldUpdateTopicInKafkaWhenConfigChangedInKube(kafkaCluster, kt,
                    theKt -> {
                        theKt.getSpec().getConfig().remove(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG);
                        return theKt;
                    },
                    expectedCreateConfigs -> {
                        var expectedUpdatedConfigs = new LinkedHashMap<>(expectedCreateConfigs);
                        expectedUpdatedConfigs.remove(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG);
                        return expectedUpdatedConfigs;
                    });
        }
    }

    @Test
    public void shouldNotRemoveInheritedConfigs() throws ExecutionException, InterruptedException, TimeoutException {
        // Scenario from https://github.com/strimzi/strimzi-kafka-operator/pull/8627#issuecomment-1600852809
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false", "compression.type", "snappy"));
        kafkaAdminClient = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers()));

        // given: a range of broker configs coming from a variety of sources
        kafkaAdminClient.incrementalAlterConfigs(Map.of(
            new ConfigResource(ConfigResource.Type.BROKER, "0"), List.of(
                new AlterConfigOp(new ConfigEntry("log.cleaner.delete.retention.ms", "" + (1000L * 60 * 60)), AlterConfigOp.OpType.SET)),
            new ConfigResource(ConfigResource.Type.BROKER, ""), List.of(
                new AlterConfigOp(new ConfigEntry("log.segment.delete.delay.ms", "" + (1000L * 60 * 60)), AlterConfigOp.OpType.SET)))).all().get();

        var config = topicOperatorConfig(NAMESPACE, kafkaCluster, true, 500);
        kafkaAdminClientOp = Mockito.spy(Admin.create(config.adminClientConfig()));
        maybeStartOperator(config);

        KafkaTopic kt = kafkaTopic(NAMESPACE, "bar", SELECTOR, null, null, null, 1, 1,
            Map.of("flush.messages", "1234"));
        var barKt = createTopic(kafkaCluster, kt);
        assertCreateSuccess(kt, barKt, Map.of("flush.messages", "1234"));
        postSyncBarrier();

        // when: resync
        try (var ignored = LogCaptor.logMessageMatches(BatchingLoop.LoopRunnable.LOGGER,
            Level.INFO,
            "\\[Batch #[0-9]+\\] Batch reconciliation completed",
            5L,
            TimeUnit.SECONDS)) {
            LOGGER.debug("Waiting for a full resync");
        }

        // then: verify that only the expected methods were called on the admin (e.g. no incrementalAlterConfigs)
        Mockito.verify(kafkaAdminClientOp, Mockito.never()).incrementalAlterConfigs(any());
        Mockito.verify(kafkaAdminClientOp, Mockito.never()).incrementalAlterConfigs(any(), any());
    }

    private static void postSyncBarrier() throws TimeoutException, InterruptedException {
        var uuid = UUID.randomUUID();
        try (var ignored = LogCaptor.logEventMatches(LOGGER,
            Level.DEBUG,
            LogCaptor.messageContainsMatch("Post sync barrier " + uuid),
            5L,
            TimeUnit.SECONDS)) {
            LOGGER.debug("Post sync barrier {}", uuid);
        }
    }

    @Test
    public void shouldUpdateTopicInKafkaWhenPartitionsIncreasedInKube() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        List<KafkaTopic> topics = managedKafkaTopics();

        for (KafkaTopic kt : topics) {
            // given
            var expectedTopicName = TopicOperatorUtil.topicName(kt);
            int specPartitions = kt.getSpec().getPartitions();
            createTopicAndAssertSuccess(kafkaCluster, kt);

            // when: partitions is increased
            modifyTopicAndAwait(kt,
                    CoreFeaturesIT::incrementPartitions,
                    readyIsTrue());

            // then
            var topicDescription = awaitTopicDescription(expectedTopicName);
            assertEquals(specPartitions + 1, numPartitions(topicDescription));
        }
    }

    @Test
    public void shouldFailDecreaseInPartitions() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        KafkaTopic kt = kafkaTopic(NAMESPACE, "fail-decrease-partition-count", true, "fail-decrease-partition-count", 2, 1);

        final int specPartitions = kt.getSpec().getPartitions();
        assertEquals(2, specPartitions);
        shouldFailOnModification(kafkaCluster, kt,
                theKt -> {
                    theKt.getSpec().setPartitions(1);
                    return theKt;
                },
                operated -> {
                    assertEquals("Decreasing partitions not supported", assertExactlyOneCondition(operated).getMessage());
                    assertEquals(TopicOperatorException.Reason.NOT_SUPPORTED.value, assertExactlyOneCondition(operated).getReason());
                },
                theKt -> {
                    theKt.getSpec().setPartitions(specPartitions);
                    return theKt;
                });
    }

    @Test
    public void shouldFailDecreaseInPartitionsWithConfigChange() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        List<KafkaTopic> topics = managedKafkaTopics();

        for (KafkaTopic kt : topics) {
            final int specPartitions = kt.getSpec().getPartitions();
            assertEquals(2, specPartitions);
            shouldFailOnModification(kafkaCluster, kt,
                    theKt ->
                            new KafkaTopicBuilder(theKt).editOrNewSpec().withPartitions(1).addToConfig(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy").endSpec().build(),
                    operated -> {
                        assertEquals("Decreasing partitions not supported", assertExactlyOneCondition(operated).getMessage());
                        assertEquals(TopicOperatorException.Reason.NOT_SUPPORTED.value, assertExactlyOneCondition(operated).getReason());
                        try {
                            assertEquals("snappy", topicConfigMap(TopicOperatorUtil.topicName(kt)).get(TopicConfig.COMPRESSION_TYPE_CONFIG),
                                    "Expect the config to have been changed even if the #partitions couldn't be decreased");
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    },
                    theKt -> {
                        theKt.getSpec().setPartitions(specPartitions);
                        return theKt;
                    });
        }
    }

    private static Condition assertExactlyOneCondition(KafkaTopic operated) {
        KafkaTopicStatus status = operated.getStatus();
        assertNotNull(status);
        List<Condition> conditions = status.getConditions();
        assertNotNull(conditions);
        assertEquals(1, conditions.size());
        return conditions.get(0);
    }

    @Test
    public void shouldNotUpdateTopicInKafkaWhenUnmanagedTopicUpdatedInKube() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        List<KafkaTopic> topics = managedKafkaTopics();

        for (KafkaTopic kt : topics) {
            // given
            var expectedTopicName = TopicOperatorUtil.topicName(kt);
            createTopicAndAssertSuccess(kafkaCluster, kt);

            // when
            var unmanaged = modifyTopicAndAwait(kt, theKt ->
                            new KafkaTopicBuilder(theKt)
                                    .editOrNewMetadata()
                                    .addToAnnotations(TopicOperatorUtil.MANAGED, "false")
                                    .endMetadata()
                                    .editOrNewSpec()
                                    .withPartitions(3)
                                    .endSpec()
                                    .build(),
                    new Predicate<>() {
                        @Override
                        public boolean test(KafkaTopic theKt) {
                            return theKt.getStatus().getConditions().get(0).getType().equals("Unmanaged");
                        }

                        @Override
                        public String toString() {
                            return "status=Unmanaged";
                        }
                    });

            // then
            assertNull(unmanaged.getStatus().getTopicName());
            var topicDescription = awaitTopicDescription(expectedTopicName);
            assertEquals(kt.getSpec().getPartitions(), numPartitions(topicDescription));
            assertEquals(Set.of(kt.getSpec().getReplicas()), replicationFactors(topicDescription));
            assertEquals(Map.of(), topicConfigMap(expectedTopicName));
        }
    }

    @Test
    public void shouldRestoreFinalizerIfRemoved() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        List<KafkaTopic> topics = managedKafkaTopics();

        for (KafkaTopic kt : topics) {
            // given
            var created = createTopic(kafkaCluster, kt, TopicOperatorUtil.isManaged(kt) ? readyIsTrueOrFalse() : unmanagedIsTrueOrFalse());
            if (TopicOperatorUtil.isManaged(kt)) {
                assertCreateSuccess(kt, created);
            }

            // when: The finalizer is removed
            LOGGER.debug("Removing finalizer");
            var postUpdate = TestUtil.changeTopic(kubernetesClient, created, theKt1 -> {
                theKt1.getMetadata().getFinalizers().remove(KubernetesHandler.FINALIZER_STRIMZI_IO_TO);
                return theKt1;
            });
            var postUpdateGeneration = postUpdate.getMetadata().getGeneration();
            LOGGER.debug("Removed finalizer; generation={}", postUpdateGeneration);

            // then: We expect the operator to revert the finalizer
            waitUntil(postUpdate, theKt ->
                    theKt.getStatus().getObservedGeneration() >= postUpdateGeneration
                            && theKt.getMetadata().getFinalizers().contains(KubernetesHandler.FINALIZER_STRIMZI_IO_TO));
        }
    }

    @Test
    public void shouldDeleteTopicFromKafkaWhenManagedTopicDeletedFromKube() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        List<KafkaTopic> topics = managedKafkaTopics();

        for (KafkaTopic kt : topics) {
            // given
            createTopicAndAssertSuccess(kafkaCluster, kt);

            // when
            Crds.topicOperation(kubernetesClient).resource(kt).delete();
            LOGGER.info("Test deleted KafkaTopic {} with resourceVersion {}",
                    kt.getMetadata().getName(), TopicOperatorUtil.resourceVersion(kt));
            Resource<KafkaTopic> resource = Crds.topicOperation(kubernetesClient).resource(kt);
            TestUtil.waitUntilCondition(resource, Objects::isNull);

            // then
            waitNotExistsInKafka(TopicOperatorUtil.topicName(kt));
        }
    }

    @Test
    public void shouldNotDeleteTopicWhenTopicDeletionDisabledInKafka() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false", "delete.topic.enable", "false"));
        List<KafkaTopic> topics = managedKafkaTopics();

        for (KafkaTopic kt : topics) {
            // given
            createTopicAndAssertSuccess(kafkaCluster, kt);

            // when
            Crds.topicOperation(kubernetesClient).resource(kt).delete();
            LOGGER.info("Test delete KafkaTopic {} with resourceVersion {}",
                    kt.getMetadata().getName(), TopicOperatorUtil.resourceVersion(kt));
            Resource<KafkaTopic> resource = Crds.topicOperation(kubernetesClient).resource(kt);
            var unready = TestUtil.waitUntilCondition(resource, readyIsFalse());

            // then
            Condition condition = assertExactlyOneCondition(unready);
            assertEquals(TopicOperatorException.Reason.KAFKA_ERROR.value, condition.getReason());
            assertEquals("org.apache.kafka.common.errors.TopicDeletionDisabledException: Topic deletion is disabled.",
                    condition.getMessage());
        }
    }

    @Test
    public void shouldDeleteTopicFromKafkaWhenManagedTopicDeletedFromKubeAndFinalizersDisabled() 
            throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        List<KafkaTopic> topics = managedKafkaTopics();

        for (KafkaTopic kt : topics) {
            // given
            maybeStartOperator(topicOperatorConfig(kt.getMetadata().getNamespace(), kafkaCluster, false));
            createTopicAndAssertSuccess(kafkaCluster, kt);

            // when
            Crds.topicOperation(kubernetesClient).resource(kt).delete();
            LOGGER.info("Test deleted KafkaTopic {} with resourceVersion {}",
                    kt.getMetadata().getName(), TopicOperatorUtil.resourceVersion(kt));
            Resource<KafkaTopic> resource = Crds.topicOperation(kubernetesClient).resource(kt);
            TestUtil.waitUntilCondition(resource, Objects::isNull);

            // then
            waitNotExistsInKafka(TopicOperatorUtil.topicName(kt));
        }
    }

    private static TopicOperatorConfig topicOperatorConfig(String ns, StrimziKafkaCluster kafkaCluster) {
        return topicOperatorConfig(ns, kafkaCluster, true);
    }

    private static TopicOperatorConfig topicOperatorConfig(String ns, StrimziKafkaCluster kafkaCluster, boolean useFinalizer) {
        return topicOperatorConfig(ns, kafkaCluster, useFinalizer, 10_000);
    }

    private static TopicOperatorConfig topicOperatorConfig(String ns, StrimziKafkaCluster kafkaCluster, boolean useFinalizer, long fullReconciliationIntervalMs) {
        return TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.NAMESPACE.key(), ns,
            TopicOperatorConfig.RESOURCE_LABELS.key(), Labels.fromMap(SELECTOR).toSelectorString(),
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), kafkaCluster.getBootstrapServers(),
            TopicOperatorConfig.CLIENT_ID.key(), CoreFeaturesIT.class.getSimpleName(),
            TopicOperatorConfig.FULL_RECONCILIATION_INTERVAL_MS.key(), String.valueOf(fullReconciliationIntervalMs),
            TopicOperatorConfig.USE_FINALIZERS.key(), String.valueOf(useFinalizer),
            TopicOperatorConfig.MAX_QUEUE_SIZE.key(), "100",
            TopicOperatorConfig.MAX_BATCH_SIZE.key(), "100",
            TopicOperatorConfig.MAX_BATCH_LINGER_MS.key(), "10"
        ));
    }

    @Test
    public void shouldNotDeleteTopicFromKafkaWhenManagedTopicDeletedFromKubeAndFinalizersDisabledButDeletionDisabledInKafka() 
            throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false", "delete.topic.enable", "false"));
        List<KafkaTopic> topics = managedKafkaTopics();

        for (KafkaTopic kt : topics) {
            // given
            maybeStartOperator(topicOperatorConfig(kt.getMetadata().getNamespace(), kafkaCluster, false));
            createTopicAndAssertSuccess(kafkaCluster, kt);

            postSyncBarrier();

            // when
            try (var ignored = LogCaptor.logMessageMatches(BatchingLoop.LoopRunnable.LOGGER,
                    Level.INFO,
                    "\\[Batch #[0-9]+\\] Batch reconciliation completed",
                    5L,
                    TimeUnit.SECONDS)) {
                try (var ignored1 = LogCaptor.logMessageMatches(BatchingTopicController.LOGGER,
                        Level.WARN,
                        "Unable to delete topic '" + TopicOperatorUtil.topicName(kt) + "' from Kafka because topic deletion is disabled on the Kafka controller.",
                        5L,
                        TimeUnit.SECONDS)) {

                    Crds.topicOperation(kubernetesClient).resource(kt).delete();
                    LOGGER.info("Test deleted KafkaTopic {} with resourceVersion {}",
                            kt.getMetadata().getName(), TopicOperatorUtil.resourceVersion(kt));
                    Resource<KafkaTopic> resource = Crds.topicOperation(kubernetesClient).resource(kt);
                    TestUtil.waitUntilCondition(resource, Objects::isNull);
                }
            }

            // then
            awaitTopicDescription(TopicOperatorUtil.topicName(kt));
        }
    }

    @Test
    public void shouldNotDeleteTopicFromKafkaWhenUnmanagedTopicDeletedFromKube() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        List<KafkaTopic> topics = managedKafkaTopics();

        for (KafkaTopic kt : topics) {
            // given
            var expectedTopicName = TopicOperatorUtil.topicName(kt);
            int specPartitions = kt.getSpec().getPartitions();
            int specReplicas = kt.getSpec().getReplicas();

            createTopicAndAssertSuccess(kafkaCluster, kt);
            TestUtil.changeTopic(kubernetesClient, kt, theKt -> 
                new KafkaTopicBuilder(theKt).editOrNewMetadata().addToAnnotations(TopicOperatorUtil.MANAGED, "false").endMetadata().build());

            // when
            Crds.topicOperation(kubernetesClient).resource(kt).delete();
            LOGGER.info("Test created KafkaTopic {} with resourceVersion {}",
                    kt.getMetadata().getName(), TopicOperatorUtil.resourceVersion(kt));
            Resource<KafkaTopic> resource = Crds.topicOperation(kubernetesClient).resource(kt);
            TestUtil.waitUntilCondition(resource, Objects::isNull);

            // then
            var topicDescription = awaitTopicDescription(expectedTopicName);
            assertEquals(specPartitions, numPartitions(topicDescription));
            assertEquals(Set.of(specReplicas), replicationFactors(topicDescription));
            assertEquals(Map.of(), topicConfigMap(expectedTopicName));
        }
    }

    @Test
    public void shouldNotDeleteTopicWhenUnmanagedTopicDeletedAndFinalizersDisabled() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        List<KafkaTopic> topics = managedKafkaTopics();

        for (KafkaTopic kt : topics) {
            // given
            maybeStartOperator(topicOperatorConfig(kt.getMetadata().getNamespace(), kafkaCluster, false));
            var expectedTopicName = TopicOperatorUtil.topicName(kt);
            int specPartitions = kt.getSpec().getPartitions();
            int specReplicas = kt.getSpec().getReplicas();

            createTopicAndAssertSuccess(kafkaCluster, kt);
            TestUtil.changeTopic(kubernetesClient, kt, theKt ->
                    new KafkaTopicBuilder(theKt).editOrNewMetadata().addToAnnotations(TopicOperatorUtil.MANAGED, "false").endMetadata().build());

            Crds.topicOperation(kubernetesClient).resource(kt).delete();
            LOGGER.info("Test deleted KafkaTopic {} with resourceVersion {}", kt.getMetadata().getName(), TopicOperatorUtil.resourceVersion(kt));
            Resource<KafkaTopic> resource = Crds.topicOperation(kubernetesClient).resource(kt);
            TestUtil.waitUntilCondition(resource, Objects::isNull);

            // then
            var topicDescription = awaitTopicDescription(expectedTopicName);
            assertEquals(specPartitions, numPartitions(topicDescription));
            assertEquals(Set.of(specReplicas), replicationFactors(topicDescription));
            assertEquals(Map.of(), topicConfigMap(expectedTopicName));
        }
    }

    static List<List<KafkaTopic>> collidingManagedTopics_sameNamespace() {
        return List.of(
                // both use spec.topicName
                List.of(kafkaTopic(NAMESPACE, "kt1", true, "collide", 1, 1),
                        kafkaTopic(NAMESPACE, "kt2", true, "collide", 1, 1)),
                // only second uses spec.topicName
                List.of(kafkaTopic(NAMESPACE, "kt1", true, null, 1, 1),
                        kafkaTopic(NAMESPACE, "kt2", true, "kt1", 1, 1)),
                // only first uses spec.topicName
                List.of(kafkaTopic(NAMESPACE, "kt1", true, "collide", 1, 1),
                        kafkaTopic(NAMESPACE, "collide", true, null, 1, 1))
        );
    }

    @ParameterizedTest
    @MethodSource("collidingManagedTopics_sameNamespace")
    public void shouldDetectMultipleResourcesManagingSameTopicInKafka(List<KafkaTopic> topics) 
            throws ExecutionException, InterruptedException, TimeoutException {
        KafkaTopic kt1 = topics.get(0);
        KafkaTopic kt2 = topics.get(1);

        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));

        // given
        assertEquals(TopicOperatorUtil.topicName(kt1), TopicOperatorUtil.topicName(kt2));
        assertNotEquals(kt1.getMetadata().getName(), kt2.getMetadata().getName());

        // when
        createTopicAndAssertSuccess(kafkaCluster, kt1);
        var st1 = waitUntil(kt1, readyIsTrue()).getStatus();
        Thread.sleep(1_000L);
        createTopic(kafkaCluster, kt2);
        var st2 = waitUntil(kt2, readyIsTrueOrFalse()).getStatus();
        waitUntil(kt2, readyIsTrueOrFalse());

        // then
        assertNull(st1.getConditions().get(0).getReason());
        assertEquals(TopicOperatorException.Reason.RESOURCE_CONFLICT.value, st2.getConditions().get(0).getReason());
        assertEquals(String.format("Managed by Ref{namespace='%s', name='%s'}", NAMESPACE, "kt1"),
            st2.getConditions().get(0).getMessage());
    }

    @Test
    public void shouldFailCreationIfMoreReplicasThanBrokers() {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));

        // given
        var topicName = "my-topic";
        var kt = kafkaTopic(NAMESPACE, topicName, true, topicName, 1, (int) Short.MAX_VALUE);
        // and kafkaCluster.numBrokers <= Short.MAX_VALUE

        // when
        var created = createTopic(kafkaCluster, kt);

        // then
        assertTrue(readyIsFalse().test(created));
        Condition condition = assertExactlyOneCondition(created);
        assertEquals(TopicOperatorException.Reason.KAFKA_ERROR.value, condition.getReason());
        assertEquals("org.apache.kafka.common.errors.InvalidReplicationFactorException: Unable to replicate the partition " +
            "32767 time(s): The target replication factor of 32767 cannot be reached because only 1 broker(s) are registered.", 
            condition.getMessage());
    }

    @Test
    public void shouldFailCreationIfUnknownConfig() {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));

        // given
        var topicName = "my-topic";
        var kt = kafkaTopic(NAMESPACE,
            topicName,
            SELECTOR,
            null,
            true,
            topicName,
            1,
            1,
            Map.of("unknown.config.parameter", "????"));

        // when
        var created = createTopic(kafkaCluster, kt);

        // then
        assertTrue(readyIsFalse().test(created));
        Condition condition = assertExactlyOneCondition(created);
        assertEquals(TopicOperatorException.Reason.KAFKA_ERROR.value, condition.getReason());
        assertEquals("org.apache.kafka.common.errors.InvalidConfigurationException: Unknown topic config name: unknown.config.parameter", condition.getMessage());
    }

    @Test
    public void shouldFailCreationIfIllegalTopicName() {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        List<KafkaTopic> topics = List.of(
                kafkaTopic(NAMESPACE, "invalid-topic-a", true, "..", 2, 1),
                kafkaTopic(NAMESPACE, "invalid-topic-b", true, ".", 2, 1),
                kafkaTopic(NAMESPACE, "invalid-topic-c", null, "invalid-topic-c{}", 2, 1),
                kafkaTopic(NAMESPACE, "invalid-topic-d", null, "x".repeat(256), 2, 1)
                );

        for (KafkaTopic kt: topics) {
            // given

            // when
            var created = createTopic(kafkaCluster, kt);

            // then
            assertTrue(readyIsFalse().test(created));
            Condition condition = assertExactlyOneCondition(created);
            assertEquals(TopicOperatorException.Reason.KAFKA_ERROR.value, condition.getReason());
            assertTrue(condition.getMessage().startsWith("org.apache.kafka.common.errors.InvalidTopicException: Topic name is invalid:"),
                    condition.getMessage());
        }
    }

    @Test
    public void shouldFailChangeToSpecTopicName() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        List<KafkaTopic> topics = managedKafkaTopics();

        for (KafkaTopic kafkaTopic : topics) {
            var expectedTopicName = TopicOperatorUtil.topicName(kafkaTopic);
            shouldFailOnModification(kafkaCluster, kafkaTopic,
                    theKt -> {
                        theKt.getSpec().setTopicName("CHANGED-" + expectedTopicName);
                        return theKt;
                    },
                    operated -> {
                        assertEquals("Changing spec.topicName is not supported", assertExactlyOneCondition(operated).getMessage());
                        assertEquals(TopicOperatorException.Reason.NOT_SUPPORTED.value, assertExactlyOneCondition(operated).getReason());
                        assertEquals(expectedTopicName, operated.getStatus().getTopicName());
                    },
                    theKt -> {
                        theKt.getSpec().setTopicName(expectedTopicName);
                        return theKt;
                    });
        }
    }

    private void shouldFailOnModification(StrimziKafkaCluster kc, KafkaTopic kt,
                                          UnaryOperator<KafkaTopic> changer,
                                          Consumer<KafkaTopic> asserter,
                                          UnaryOperator<KafkaTopic> reverter
    ) throws ExecutionException, InterruptedException, TimeoutException {
        // given
        createTopicAndAssertSuccess(kc, kt);

        // when
        KafkaTopic broken = modifyTopicAndAwait(kt, changer, readyIsFalse());

        // then
        asserter.accept(broken);

        // and when
        var fixed  = modifyTopicAndAwait(kt, reverter, readyIsTrue());

        // then
        assertNull(assertExactlyOneCondition(fixed).getMessage());
        assertNull(assertExactlyOneCondition(fixed).getReason());
    }

    private KafkaTopic modifyTopicAndAwait(KafkaTopic kt, UnaryOperator<KafkaTopic> changer, Predicate<KafkaTopic> predicate) {
        var edited = TestUtil.changeTopic(kubernetesClient, kt, changer);
        var postUpdateGeneration = edited.getMetadata().getGeneration();
        Predicate<KafkaTopic> topicWasSyncedAndMatchesPredicate = new Predicate<>() {
            @Override
            public boolean test(KafkaTopic theKt) {
                return theKt.getStatus() != null
                    && (theKt.getStatus().getObservedGeneration() == postUpdateGeneration 
                        || !TopicOperatorUtil.isManaged(theKt) || TopicOperatorUtil.isPaused(theKt))
                    && predicate.test(theKt);
            }

            @Override
            public String toString() {
                return "observedGeneration is correct and " + predicate;
            }
        };
        return waitUntil(edited, topicWasSyncedAndMatchesPredicate);
    }

    @Test
    public void shouldFailChangeToRf() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        KafkaTopic kt = kafkaTopic(NAMESPACE, "fail-change-rf", true, "fail-change-rf", 2, 1);

        int specReplicas = kt.getSpec().getReplicas();
        shouldFailOnModification(kafkaCluster, kt,
                theKt -> {
                    theKt.getSpec().setReplicas(specReplicas + 1);
                    return theKt;
                },
                operated -> {
                    assertEquals("Replication factor change not supported, but required for partitions [0, 1]", assertExactlyOneCondition(operated).getMessage());
                    assertEquals(TopicOperatorException.Reason.NOT_SUPPORTED.value, assertExactlyOneCondition(operated).getReason());
                },
                theKt -> {
                    theKt.getSpec().setReplicas(specReplicas);
                    return theKt;
                });
    }

    @Test
    public void shouldAccountForReassigningPartitionsNoRfChange() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(3, 1, Map.of("auto.create.topics.enable", "false"));

        try (Producer<String, String> producer = new KafkaProducer<>(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers(), 
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer", 
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer"))) {
            var topicName = "my-topic";
            var kt = kafkaTopic(NAMESPACE, topicName, true, topicName, 1, 1);
            accountForReassigningPartitions(kafkaCluster, producer, kt,
                    initialReplicas -> {
                        assertEquals(1, initialReplicas.size());
                        var replacementReplica = (initialReplicas.iterator().next() + 1) % 3;
                        return List.of(replacementReplica);
                    },
                    readyIsTrue(),
                    readyIsTrue());
        }
    }

    @Test
    @Disabled("Seems to fail with Strimzi test container for unknown reason -> possibly the same as the next test already disabled?")
    public void shouldAccountForReassigningPartitionsIncreasingRf() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(3, 1, Map.of("auto.create.topics.enable", "false"));

        try (Producer<String, String> producer = new KafkaProducer<>(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers(), 
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer", 
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer"))) {
            var topicName = "my-topic";
            var kt = kafkaTopic(NAMESPACE, topicName, true, topicName, 1, 1);
            accountForReassigningPartitions(kafkaCluster, producer, kt,
                    initialReplicas -> {
                        assertEquals(1, initialReplicas.size());
                        Integer initialReplica = initialReplicas.iterator().next();
                        var replacementReplica = (initialReplica + 1) % 3;
                        return List.of(initialReplica, replacementReplica);
                    },
                    readyIsFalseAndReasonIs("NotSupported", "Replication factor change not supported, but required for partitions [0]"),
                    readyIsFalseAndReasonIs("NotSupported", "Replication factor change not supported, but required for partitions [0]"));
        }
    }

    @Test
    @Disabled("Throttles don't provide a way to ensure that reconciliation happens when the UTO will observe a non-empty removing set")
    public void shouldAccountForReassigningPartitionsDecreasingRf() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(3, 1, Map.of("auto.create.topics.enable", "false"));

        try (Producer<String, String> producer = new KafkaProducer<>(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers(), 
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer", 
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer"))) {
            var topicName = "my-topic";
            var kt = kafkaTopic(NAMESPACE, topicName, true, topicName, 1, 2);
            accountForReassigningPartitions(kafkaCluster, producer, kt,
                    initialReplicas -> {
                        assertEquals(2, initialReplicas.size());
                        return List.of(initialReplicas.get(0));
                    },
                    readyIsFalseAndReasonIs("NotSupported", "Replication factor change not supported, but required for partitions [0]"),
                    readyIsFalseAndReasonIs("NotSupported", "Replication factor change not supported, but required for partitions [0]"));
        }
    }

    private void accountForReassigningPartitions(
        StrimziKafkaCluster kafkaCluster,
        Producer<String, String> producer,
        KafkaTopic kt,
        Function<List<Integer>, List<Integer>> newReplicasFn,
        Predicate<KafkaTopic> duringReassignmentPredicate,
        Predicate<KafkaTopic> postReassignmentPredicate)
        throws ExecutionException, InterruptedException, TimeoutException {
        // given
        assertEquals(1, kt.getSpec().getPartitions());
        var topicName = TopicOperatorUtil.topicName(kt);
        var tp = new TopicPartition(topicName, 0);
        var created = createTopicAndAssertSuccess(kafkaCluster, kt);

        List<Future<RecordMetadata>> futs = new ArrayList<>(1000);
        for (int i = 0; i < 1000; i++) {
            futs.add(producer.send(new ProducerRecord<>(topicName, "X".repeat(1000), "Y".repeat(1000))));
        }
        for (var f : futs) {
            f.get();
        }

        TopicPartitionInfo pi = awaitTopicDescription(topicName).partitions().get(0);
        var initialReplicas = pi.replicas().stream().map(Node::id).toList();
        var newReplicas = newReplicasFn.apply(initialReplicas);
        var initialLeader = pi.leader().id();
        var addedReplicas = new HashSet<>(newReplicas);
        initialReplicas.forEach(addedReplicas::remove);
        var removingReplicas = new HashSet<>(initialReplicas);
        newReplicas.forEach(removingReplicas::remove);

        var throttledRate = "1";
        Map<ConfigResource, Collection<AlterConfigOp>> throttles = buildThrottles(initialLeader, addedReplicas, throttledRate, tp, AlterConfigOp.OpType.SET);
        Map<ConfigResource, Collection<AlterConfigOp>> removeThrottles = buildThrottles(initialLeader, addedReplicas, throttledRate, tp, AlterConfigOp.OpType.DELETE);
        LOGGER.debug("Initial leader {}", initialLeader);
        LOGGER.debug("Initial replicas {}", initialReplicas);
        LOGGER.debug("New replicas {}", newReplicas);
        LOGGER.debug("Added replicas {}", addedReplicas);
        LOGGER.debug("Removing replicas {}", removingReplicas);
        LOGGER.debug("Throttles {}", throttles);
        LOGGER.debug("Remove throttles {}", removeThrottles);

        // throttle replication to zero. This is to ensure the operator will actually observe the topic state
        // during reassignment
        kafkaAdminClient.incrementalAlterConfigs(throttles).all().get();

        // when: reassignment is on-going

        var reassignStartResult = kafkaAdminClient.alterPartitionReassignments(
            Map.of(
                tp, Optional.of(new NewPartitionReassignment(newReplicas))
            )
        );
        reassignStartResult.all().get();

        assertFalse(kafkaAdminClient.listPartitionReassignments(Set.of(tp)).reassignments().get().isEmpty(),
            "Expect on-going reassignment prior to reconcile");

        // then
        // trigger reconciliation by change a config
        var modified = modifyTopicAndAwait(created,
            CoreFeaturesIT::setSnappyCompression,
            duringReassignmentPredicate);

        assertFalse(kafkaAdminClient.listPartitionReassignments(Set.of(tp)).reassignments().get().isEmpty(),
            "Expect on-going reassignment after reconcile");

        // let reassignment complete normally by removing the throttles
        kafkaAdminClient.incrementalAlterConfigs(removeThrottles).all().get();

        long deadline = System.currentTimeMillis() + 30_000;
        while (!kafkaAdminClient.listPartitionReassignments(Set.of(tp)).reassignments().get().isEmpty()) {
            if (System.currentTimeMillis() > deadline) {
                throw new TimeoutException("Expecting reassignment to complete after removing throttles");
            }
            TimeUnit.MILLISECONDS.sleep(1_000);
        }

        // trigger reconciliation by changing a config again
        modifyTopicAndAwait(modified,
            CoreFeaturesIT::setGzipCompression,
            postReassignmentPredicate);
    }

    private static HashMap<ConfigResource, Collection<AlterConfigOp>> buildThrottles(
        int initialLeader,
        HashSet<Integer> addedReplicas,
        String throttledRate,
        TopicPartition tp,
        AlterConfigOp.OpType set) {
        var throttles = new LinkedHashMap<ConfigResource, Collection<AlterConfigOp>>();
        throttles.put(new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(initialLeader)),
            List.of(new AlterConfigOp(new ConfigEntry("leader.replication.throttled.rate", throttledRate), set)));
        addedReplicas.forEach(addedReplica ->
            throttles.put(new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(addedReplica)),
                List.of(new AlterConfigOp(new ConfigEntry("follower.replication.throttled.rate", throttledRate), set))));
        throttles.put(new ConfigResource(ConfigResource.Type.TOPIC, tp.topic()),
            List.of(new AlterConfigOp(new ConfigEntry("leader.replication.throttled.replicas", "%d:%d".formatted(tp.partition(), initialLeader)), set)));
        addedReplicas.forEach(addedReplica -> throttles.put(new ConfigResource(ConfigResource.Type.TOPIC, tp.topic()),
            List.of(new AlterConfigOp(new ConfigEntry("follower.replication.throttled.replicas", "%d:%d".formatted(tp.partition(), addedReplica)), set))));
        return throttles;
    }

    @Test
    public void shouldFailCreationIfNoTopicAuthz() {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        KafkaTopic kt = kafkaTopic(NAMESPACE, "fail-crreation-if-no-topic-authz", true, "fail-creation-if-no-topic-authz", 2, 1);
        topicCreationFailsDueToAdminException(kt, kafkaCluster, new TopicAuthorizationException("not allowed"), "KafkaError");
    }

    @Test
    public void shouldFailCreationIfNpe() {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        KafkaTopic kt = kafkaTopic(NAMESPACE, "fail-creation-if-npe", true, "fail-creation-if-npe", 2, 1);

        topicCreationFailsDueToAdminException(kt, kafkaCluster, new NullPointerException(), "InternalError");
    }

    private void topicCreationFailsDueToAdminException(KafkaTopic kt,
                                                       StrimziKafkaCluster kafkaCluster,
                                                       Throwable exception,
                                                       String expectedReason) {
        // given
        var config = topicOperatorConfig(NAMESPACE, kafkaCluster);
        kafkaAdminClientOp = Mockito.spy(Admin.create(config.adminClientConfig()));
        var ctr = mock(CreateTopicsResult.class);
        Mockito.doReturn(failedFuture(exception)).when(ctr).all();
        Mockito.doReturn(Map.of(TopicOperatorUtil.topicName(kt), failedFuture(exception))).when(ctr).values();
        Mockito.doReturn(ctr).when(kafkaAdminClientOp).createTopics(any());
        maybeStartOperator(config);

        //when
        var created = createTopic(kafkaCluster, kt);

        // then
        assertTrue(readyIsFalse().test(created));
        var condition = assertExactlyOneCondition(created);
        assertEquals(expectedReason, condition.getReason());
        assertEquals(exception.toString(), condition.getMessage());
    }

    @Test
    public void shouldFailAlterConfigIfNoTopicAuthz() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        KafkaTopic kt = kafkaTopic(NAMESPACE, "fail-after-config-if-no-topic-authz", true, "fail-after-config-if-no-topic-authz", 2, 1);

        var config = topicOperatorConfig(NAMESPACE, kafkaCluster);
        kafkaAdminClientOp = Mockito.spy(Admin.create(config.adminClientConfig()));
        var ctr = mock(AlterConfigsResult.class);
        Mockito.doReturn(failedFuture(new TopicAuthorizationException("not allowed"))).when(ctr).all();
        Mockito.doReturn(Map.of(new ConfigResource(ConfigResource.Type.TOPIC, TopicOperatorUtil.topicName(kt)), 
            failedFuture(new TopicAuthorizationException("not allowed")))).when(ctr).values();
        Mockito.doReturn(ctr).when(kafkaAdminClientOp).incrementalAlterConfigs(any());

        maybeStartOperator(config);
        createTopicAndAssertSuccess(kafkaCluster, kt);

        var modified = modifyTopicAndAwait(kt, CoreFeaturesIT::setSnappyCompression, readyIsFalse());
        var condition = assertExactlyOneCondition(modified);
        assertEquals("KafkaError", condition.getReason());
        assertEquals("org.apache.kafka.common.errors.TopicAuthorizationException: not allowed", condition.getMessage());
    }
    
    @Test
    public void shouldFailTheReconciliationWithNullConfig() {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));

        invalidConfigFailsReconciliation(
                kafkaCluster,
                null,
                "KafkaError",
                "org.apache.kafka.common.errors.InvalidConfigurationException: Null value not supported for topic configs: cleanup.policy");
    }

    @Test
    public void shouldFailTheReconciliationWithUnexpectedConfig() {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));

        invalidConfigFailsReconciliation(
                kafkaCluster,
                Map.of("foo", 12),
                "InternalError",
                "io.strimzi.operator.common.model.InvalidResourceException: Invalid value for topic config 'cleanup.policy': {foo=12}");
    }

    private void invalidConfigFailsReconciliation(
            StrimziKafkaCluster kafkaCluster,
            Map<String, Integer> policy,
            String expectedReasons,
            String expectedMessage
    ) {
        Map<String, Object> configs = new HashMap<>();
        configs.put("cleanup.policy", policy);
        var kafkaTopic = new KafkaTopicBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName("my-topic")
                    .withLabels(SELECTOR)
                .endMetadata()
                .withNewSpec()
                    .withConfig(configs)
                    .withPartitions(1)
                    .withReplicas(1)
                .endSpec()
                .build();
        var created = createTopic(kafkaCluster, kafkaTopic);
        var condition = assertExactlyOneCondition(created);
        assertEquals(expectedReasons, condition.getReason());
        assertEquals(expectedMessage, condition.getMessage());
    }

    private static KafkaTopic setGzipCompression(KafkaTopic kt) {
        return setCompression(kt, "gzip");
    }

    private static KafkaTopic setSnappyCompression(KafkaTopic kt) {
        return setCompression(kt, "snappy");
    }

    private static KafkaTopic setCompression(KafkaTopic kt, String gzip) {
        return new KafkaTopicBuilder(kt).editOrNewSpec().addToConfig(TopicConfig.COMPRESSION_TYPE_CONFIG, gzip).endSpec().build();
    }

    @Test
    public void shouldFailAddPartitionsIfNoTopicAuthz() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        KafkaTopic kt = kafkaTopic(NAMESPACE, "fail-add-partitions-if-no-topic-authz", true, "fail-add-partitions-if-no-topic-authz", 2, 1);

        var config = topicOperatorConfig(NAMESPACE, kafkaCluster);
        kafkaAdminClientOp = Mockito.spy(Admin.create(config.adminClientConfig()));
        var ctr = mock(CreatePartitionsResult.class);
        Mockito.doReturn(failedFuture(new TopicAuthorizationException("not allowed"))).when(ctr).all();
        Mockito.doReturn(Map.of(TopicOperatorUtil.topicName(kt), failedFuture(new TopicAuthorizationException("not allowed")))).when(ctr).values();
        Mockito.doReturn(ctr).when(kafkaAdminClientOp).createPartitions(any());

        maybeStartOperator(config);
        createTopicAndAssertSuccess(kafkaCluster, kt);

        var modified = modifyTopicAndAwait(kt,
                CoreFeaturesIT::incrementPartitions,
                readyIsFalse());
        var condition = assertExactlyOneCondition(modified);
        assertEquals("KafkaError", condition.getReason());
        assertEquals("org.apache.kafka.common.errors.TopicAuthorizationException: not allowed", condition.getMessage());
    }

    private static KafkaTopic incrementPartitions(KafkaTopic theKt) {
        theKt.getSpec().setPartitions(theKt.getSpec().getPartitions() + 1);
        return theKt;
    }

    @Test
    public void shouldFailDeleteIfNoTopicAuthz() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));
        KafkaTopic kt = kafkaTopic(NAMESPACE, "fail-delete-if-no-topic-authz", true, "fail-delete-if-no-topic-authz", 2, 1);

        // given
        var config = topicOperatorConfig(NAMESPACE, kafkaCluster);
        kafkaAdminClientOp = Mockito.spy(Admin.create(config.adminClientConfig()));
        var ctr = mock(DeleteTopicsResult.class);
        Mockito.doReturn(failedFuture(new TopicAuthorizationException("not allowed"))).when(ctr).all();
        Mockito.doReturn(Map.of(TopicOperatorUtil.topicName(kt), failedFuture(new TopicAuthorizationException("not allowed")))).when(ctr).topicNameValues();
        Mockito.doReturn(ctr).when(kafkaAdminClientOp).deleteTopics(any(TopicCollection.TopicNameCollection.class));

        maybeStartOperator(config);
        createTopicAndAssertSuccess(kafkaCluster, kt);

        // when
        Crds.topicOperation(kubernetesClient).resource(kt).delete();
        LOGGER.info("Test deleted KafkaTopic {} with resourceVersion {}",
                kt.getMetadata().getName(), TopicOperatorUtil.resourceVersion(kt));
        Resource<KafkaTopic> resource = Crds.topicOperation(kubernetesClient).resource(kt);
        var deleted = TestUtil.waitUntilCondition(resource, readyIsFalse());

        // then
        var condition = assertExactlyOneCondition(deleted);
        assertEquals("KafkaError", condition.getReason());
        assertEquals("org.apache.kafka.common.errors.TopicAuthorizationException: not allowed", condition.getMessage());
    }

    @Test
    public void shouldFailIfNumPartitionsDivergedWithConfigChange() throws ExecutionException, InterruptedException, TimeoutException {
        // scenario from https://github.com/strimzi/strimzi-kafka-operator/pull/8627#pullrequestreview-1477513413
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));

        var firstTopicName = "first";
        var secondTopicName = "second";

        // create topic
        LOGGER.info("Create {}", firstTopicName);
        var firstTopic = kafkaTopic(NAMESPACE, firstTopicName, null, null, 1, 1);
        firstTopic = createTopicAndAssertSuccess(kafkaCluster, firstTopic);

        // create conflicting topic
        LOGGER.info("Create conflicting {}", secondTopicName);
        var secondTopic = kafkaTopic(NAMESPACE, secondTopicName, SELECTOR, null, null, firstTopicName,
            1, 1, Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy"));
        secondTopic = createTopic(kafkaCluster, secondTopic);
        assertTrue(readyIsFalse().test(secondTopic));
        var condition = assertExactlyOneCondition(secondTopic);
        assertEquals(TopicOperatorException.Reason.RESOURCE_CONFLICT.value, condition.getReason());
        assertEquals(String.format("Managed by Ref{namespace='%s', name='%s'}", NAMESPACE, firstTopicName), condition.getMessage());

        // increase partitions of topic
        LOGGER.info("Increase partitions of {}", firstTopicName);
        var editedFoo = modifyTopicAndAwait(firstTopic, theKt ->
                new KafkaTopicBuilder(theKt).editSpec().withPartitions(3).endSpec().build(),
            readyIsTrue());

        // unmanage topic
        LOGGER.info("Unmanage {}", firstTopicName);
        var unmanagedFoo = modifyTopicAndAwait(editedFoo, theKt ->
                new KafkaTopicBuilder(theKt).editMetadata().addToAnnotations(TopicOperatorUtil.MANAGED, "false").endMetadata().build(),
            readyIsTrue());

        // when: delete topic
        LOGGER.info("Delete {}", firstTopicName);
        Crds.topicOperation(kubernetesClient).resource(unmanagedFoo).delete();
        LOGGER.info("Test deleted KafkaTopic {} with resourceVersion {}", unmanagedFoo.getMetadata().getName(), TopicOperatorUtil.resourceVersion(unmanagedFoo));
        Resource<KafkaTopic> resource = Crds.topicOperation(kubernetesClient).resource(unmanagedFoo);
        TestUtil.waitUntilCondition(resource, Objects::isNull);

        // then: expect conflicting topic's unreadiness to be due to mismatching #partitions
        waitUntil(secondTopic, readyIsFalseAndReasonIs(
            TopicOperatorException.Reason.NOT_SUPPORTED.value,
            "Decreasing partitions not supported"));
    }

    @RepeatedTest(10)
    public void shouldDetectConflictingKafkaTopicCreations() throws InterruptedException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));

        var foo = kafkaTopic(NAMESPACE, "foo", null, null, 1, 1);
        var bar = kafkaTopic(NAMESPACE, "bar", SELECTOR, null, null, "foo", 1, 1,
            Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy"));

        LOGGER.info("Create conflicting topics: foo and bar");
        var reconciledTopics = createTopicsConcurrently(kafkaCluster, foo, bar);
        var reconciledFoo = TestUtil.findKafkaTopicByName(reconciledTopics, "foo");
        var reconciledBar = TestUtil.findKafkaTopicByName(reconciledTopics, "bar");

        // only one resource with the same topicName should be reconciled
        var fooFailed = readyIsFalse().test(reconciledFoo);
        var barFailed = readyIsFalse().test(reconciledBar);
        assertTrue(fooFailed ^ barFailed);

        if (fooFailed) {
            assertKafkaTopicConflict(reconciledFoo, reconciledBar);
        } else {
            assertKafkaTopicConflict(reconciledBar, reconciledFoo);
        }
    }

    private void assertKafkaTopicConflict(KafkaTopic failed, KafkaTopic ready) {
        // the error message should refer to the ready resource name
        var condition = assertExactlyOneCondition(failed);
        assertEquals(TopicOperatorException.Reason.RESOURCE_CONFLICT.value, condition.getReason());
        assertEquals(String.format("Managed by Ref{namespace='%s', name='%s'}",
            ready.getMetadata().getNamespace(), ready.getMetadata().getName()), condition.getMessage());

        // the failed resource should become ready after we unmanage and delete the other
        LOGGER.info("Unmanage {}", ready.getMetadata().getName());
        var unmanagedBar = modifyTopicAndAwait(ready, theKt ->
                new KafkaTopicBuilder(theKt).editMetadata().addToAnnotations(TopicOperatorUtil.MANAGED, "false").endMetadata().build(),
            readyIsTrue());

        LOGGER.info("Delete {}", ready.getMetadata().getName());
        Crds.topicOperation(kubernetesClient).resource(unmanagedBar).delete();
        Resource<KafkaTopic> resource = Crds.topicOperation(kubernetesClient).resource(unmanagedBar);
        TestUtil.waitUntilCondition(resource, Objects::isNull);

        waitUntil(failed, readyIsTrue());
    }

    private static <T> KafkaFuture<T> failedFuture(Throwable error) {
        var future = new KafkaFutureImpl<T>();
        future.completeExceptionally(error);
        return future;
    }

    @Test
    public void shouldLogWarningIfAutoCreateTopicsIsEnabled() throws Exception {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "true"));

        try (var ignored = LogCaptor.logMessageMatches(BatchingTopicController.LOGGER,
            Level.WARN,
            "It is recommended that " + KafkaHandler.AUTO_CREATE_TOPICS_ENABLE + " is set to 'false' " +
                "to avoid races between the operator and Kafka applications auto-creating topics",
            5L,
            TimeUnit.SECONDS)) {
            maybeStartOperator(topicOperatorConfig(NAMESPACE, kafkaCluster));
        }
    }

    @Test
    public void shouldTerminateIfQueueFull() throws InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false", "num.partitions", "4", "default.replication.factor", "1"));

        // given
        var ns = createNamespace(NAMESPACE);
        var config = TopicOperatorConfig.buildFromMap(Map.of(
            TopicOperatorConfig.NAMESPACE.key(), ns,
            TopicOperatorConfig.RESOURCE_LABELS.key(), Labels.fromMap(SELECTOR).toSelectorString(),
            TopicOperatorConfig.BOOTSTRAP_SERVERS.key(), kafkaCluster.getBootstrapServers(),
            TopicOperatorConfig.CLIENT_ID.key(), CoreFeaturesIT.class.getSimpleName(),
            TopicOperatorConfig.FULL_RECONCILIATION_INTERVAL_MS.key(), "10000",
            TopicOperatorConfig.USE_FINALIZERS.key(), "true",
            TopicOperatorConfig.MAX_QUEUE_SIZE.key(), "1",
            TopicOperatorConfig.MAX_BATCH_SIZE.key(), "100",
            TopicOperatorConfig.MAX_BATCH_LINGER_MS.key(), "5000"
        ));

        maybeStartOperator(config);

        assertTrue(operator.isAlive());

        KafkaTopic kt = new KafkaTopicBuilder()
            .withNewMetadata()
            .withNamespace(NAMESPACE)
            .withName("foo")
            .withLabels(SELECTOR)
            .endMetadata()
            .build();

        // when

        // We stop the loop thread, so nothing it taking from the queue, so that the queue length will be exceeded
        operator.queue.stop();

        try (var ignored = LogCaptor.logMessageMatches(BatchingLoop.LOGGER,
            Level.ERROR,
            "Queue length 1 exceeded, stopping operator. Please increase STRIMZI_MAX_QUEUE_SIZE environment variable",
            5L,
            TimeUnit.SECONDS)) {

            Crds.topicOperation(kubernetesClient).resource(kt).create();
            Crds.topicOperation(kubernetesClient).resource(new KafkaTopicBuilder(kt)
                .editMetadata().withName("bar").endMetadata().build()).create();
        }

        // then
        assertNull(operator.shutdownHook, "Expect the operator to shutdown");

        // finally, because the @After method of this class asserts that the operator is running
        // we start a new operator
        kafkaAdminClientOp = null;
        operator = null;
        maybeStartOperator(topicOperatorConfig(NAMESPACE, kafkaCluster));
    }

    @Test
    public void shouldNotReconcilePausedKafkaTopicOnAdd() throws InterruptedException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));

        var topicName = "my-topic";
        var kafkaTopic = createTopic(
            kafkaCluster,
            kafkaTopic(NAMESPACE, topicName, SELECTOR,
                Map.of(ResourceAnnotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, "true"),
                true, topicName, 1, 1, Map.of()),
            pausedIsTrue()
        );

        assertEquals(1, kafkaTopic.getStatus().getObservedGeneration());
        assertNotExistsInKafka(topicName);
    }

    @Test
    public void shouldNotReconcilePausedKafkaTopicOnUpdate() throws ExecutionException, InterruptedException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));

        var topicName = "my-topic";
        createTopic(kafkaCluster,
            kafkaTopic(NAMESPACE, topicName, SELECTOR, null, true, topicName, 1, 1, Map.of()));
        
        var kafkaTopic = pauseTopic(NAMESPACE, topicName);
        
        TestUtil.changeTopic(kubernetesClient, kafkaTopic, theKt -> {
            theKt.getSpec().setConfig(Map.of(TopicConfig.FLUSH_MS_CONFIG, "1000"));
            return theKt;
        });

        assertEquals(1, kafkaTopic.getStatus().getObservedGeneration());
        assertEquals(Map.of(), topicConfigMap(topicName));
    }

    @Test
    public void shouldReconcilePausedKafkaTopicOnDelete() throws InterruptedException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));

        var topicName = "my-topic";
        createTopic(kafkaCluster,
            kafkaTopic(NAMESPACE, topicName, SELECTOR, null, true, topicName, 1, 1, Map.of()));
        
        var kafkaTopic = pauseTopic(NAMESPACE, topicName);

        Crds.topicOperation(kubernetesClient).resource(kafkaTopic).delete();
        LOGGER.info("Test deleted KafkaTopic {} with resourceVersion {}",
            kafkaTopic.getMetadata().getName(), TopicOperatorUtil.resourceVersion(kafkaTopic));
        Resource<KafkaTopic> resource = Crds.topicOperation(kubernetesClient).resource(kafkaTopic);
        TestUtil.waitUntilCondition(resource, Objects::isNull);

        assertNotExistsInKafka(topicName);
    }

    @Test
    public void topicIdShouldBeEmptyOnPausedKafkaTopic() {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));

        var topicName = "my-topic";
        var kafkaTopic = createTopic(kafkaCluster,
            kafkaTopic(NAMESPACE, topicName, SELECTOR, null, true, topicName, 1, 1, Map.of()));

        assertNotNull(kafkaTopic.getStatus().getTopicName());
        assertNotNull(kafkaTopic.getStatus().getTopicId());

        kafkaTopic = pauseTopic(NAMESPACE, topicName);

        assertNotNull(kafkaTopic.getStatus().getTopicName());
        assertNull(kafkaTopic.getStatus().getTopicId());
        
        kafkaTopic = unpauseTopic(NAMESPACE, topicName);

        assertNotNull(kafkaTopic.getStatus().getTopicName());
        assertNotNull(kafkaTopic.getStatus().getTopicId());
    }

    @Test
    public void topicNameAndIdShouldBeEmptyOnUnmanagedKafkaTopic() {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));

        var topicName = "my-topic";

        var kafkaTopic = createTopic(kafkaCluster,
            kafkaTopic(NAMESPACE, topicName, SELECTOR, null, true, topicName, 1, 1, Map.of()));

        assertNotNull(kafkaTopic.getStatus().getTopicName());
        assertNotNull(kafkaTopic.getStatus().getTopicId());

        kafkaTopic = unmanageTopic(NAMESPACE, topicName);

        assertNull(kafkaTopic.getStatus().getTopicName());
        assertNull(kafkaTopic.getStatus().getTopicId());

        kafkaTopic = manageTopic(NAMESPACE, topicName);

        assertNotNull(kafkaTopic.getStatus().getTopicName());
        assertNotNull(kafkaTopic.getStatus().getTopicId());
    }

    @Test
    public void shouldReconcileKafkaTopicWithoutPartitions() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false", "num.partitions", "3"));

        var topicName = "my-topic";

        createTopic(kafkaCluster,
            kafkaTopic(NAMESPACE, topicName, SELECTOR, null, true, topicName, null, 1, Map.of()));

        var topicDescription = awaitTopicDescription(topicName);
        assertEquals(3, numPartitions(topicDescription));
    }

    @Test
    public void shouldReconcileKafkaTopicWithoutReplicas() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false", "default.replication.factor", "1"));

        var topicName = "my-topic";

        createTopic(kafkaCluster,
            kafkaTopic(NAMESPACE, topicName, SELECTOR, null, true, topicName, 1, null, Map.of()));

        var topicDescription = awaitTopicDescription(topicName);
        assertEquals(Set.of(1), replicationFactors(topicDescription));
    }

    @Test
    public void shouldReconcileKafkaTopicWithEmptySpec() throws ExecutionException, InterruptedException, TimeoutException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false", "num.partitions", "3", "default.replication.factor", "1"));

        var topicName = "my-topic";
        createTopic(kafkaCluster, kafkaTopicWithNoSpec(topicName, true));
        var topicDescription = awaitTopicDescription(topicName);
        assertEquals(3, numPartitions(topicDescription));
        assertEquals(Set.of(1), replicationFactors(topicDescription));
    }

    @Test
    public void shouldNotReconcileKafkaTopicWithMissingSpec() throws InterruptedException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false", "num.partitions", "3", "default.replication.factor", "1"));

        var topicName = "my-topic";
        maybeStartOperator(topicOperatorConfig(NAMESPACE, kafkaCluster));

        Crds.topicOperation(kubernetesClient)
            .resource(kafkaTopicWithNoSpec(topicName, false))
            .create();

        assertNotExistsInKafka(topicName);
    }

    @Test
    public void shouldReconcileOnTopicExistsException() {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));

        var topicName = "my-topic";
        var config = topicOperatorConfig(NAMESPACE, kafkaCluster);

        var creteTopicResult = mock(CreateTopicsResult.class);
        var existsException = new TopicExistsException(String.format("Topic '%s' already exists.", topicName));
        Mockito.doReturn(failedFuture(existsException)).when(creteTopicResult).all();
        Mockito.doReturn(Map.of(topicName, failedFuture(existsException))).when(creteTopicResult).values();
        kafkaAdminClientOp = Mockito.spy(Admin.create(config.adminClientConfig()));
        Mockito.doReturn(creteTopicResult).when(kafkaAdminClientOp).createTopics(any());

        KafkaTopic kafkaTopic = createTopic(kafkaCluster, kafkaTopic(NAMESPACE, topicName, true, topicName, 2, 1));
        assertTrue(readyIsTrue().test(kafkaTopic));
    }

    @Test
    public void shouldUpdateAnUnmanagedTopic() throws InterruptedException {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));

        var topicName = "my-topic";

        // create the topic
        var topic = createTopic(kafkaCluster,
                kafkaTopic(NAMESPACE, topicName, SELECTOR, null, null, topicName, 1, 1,
                        Map.of(TopicConfig.RETENTION_MS_CONFIG, "1000")));
        topic = Crds.topicOperation(kubernetesClient).resource(topic).get();

        TestUtil.waitUntilCondition(Crds.topicOperation(kubernetesClient).resource(topic), kt ->
                Optional.of(kt)
                    .map(KafkaTopic::getStatus)
                    .map(KafkaTopicStatus::getConditions)
                    .flatMap(c -> Optional.of(c.get(0)))
                    .map(Condition::getType)
                    .filter("Ready"::equals)
                    .isPresent()
        );

        // set unmanaged
        topic = Crds.topicOperation(kubernetesClient).resource(topic).get();
        topic.setStatus(null);
        topic.getMetadata().getAnnotations().put(TopicOperatorUtil.MANAGED, "false");
        topic = Crds.topicOperation(kubernetesClient).resource(topic).update();

        TestUtil.waitUntilCondition(Crds.topicOperation(kubernetesClient).resource(topic), kt ->
            Optional.of(kt)
                    .map(KafkaTopic::getStatus)
                    .map(KafkaTopicStatus::getConditions)
                    .flatMap(c -> Optional.of(c.get(0)))
                    .map(Condition::getType)
                    .filter("Unmanaged"::equals)
                    .isPresent()
        );

        // apply a change to the unmanaged topic
        topic = Crds.topicOperation(kubernetesClient).resource(topic).get();
        topic.setStatus(null);
        topic.getSpec().getConfig().put(TopicConfig.RETENTION_MS_CONFIG, "1001");
        topic = Crds.topicOperation(kubernetesClient).resource(topic).update();
        var resourceVersionOnUpdate = topic.getMetadata().getResourceVersion();

        TestUtil.waitUntilCondition(Crds.topicOperation(kubernetesClient).resource(topic), kt ->
                !resourceVersionOnUpdate.equals(kt.getMetadata().getResourceVersion())
        );
        topic = Crds.topicOperation(kubernetesClient).resource(topic).get();
        var resourceVersionAfterUpdate = topic.getMetadata().getResourceVersion();

        // Wait a bit to check the resource is not getting updated continuously
        Thread.sleep(500L);
        TestUtil.waitUntilCondition(Crds.topicOperation(kubernetesClient).resource(topic), kt ->
                resourceVersionAfterUpdate.equals(kt.getMetadata().getResourceVersion())
        );
    }

    @Test
    public void shouldUpdateTopicIdIfDeletedWhileUnmanaged() {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));

        TopicOperatorConfig config = topicOperatorConfig(NAMESPACE, kafkaCluster, true, 500);
        kafkaAdminClientOp = Mockito.spy(Admin.create(config.adminClientConfig()));

        var created = createTopic(kafkaCluster,
            kafkaTopic(NAMESPACE, "my-topic", SELECTOR, null, true, "my-topic", 1, 1, Map.of()));

        unmanageTopic(NAMESPACE, "my-topic");

        kafkaAdminClientOp.deleteTopics(Set.of("my-topic"));
        kafkaAdminClientOp.createTopics(Set.of(new NewTopic("my-topic", 1, (short) 1)));

        var updated = manageTopic(NAMESPACE, "my-topic");

        assertNotEquals(created.getStatus().getTopicId(), updated.getStatus().getTopicId());
    }

    @Test
    public void shouldUpdateTopicIdIfDeletedWhilePaused() {
        startKafkaCluster(1, 1, Map.of("auto.create.topics.enable", "false"));

        TopicOperatorConfig config = topicOperatorConfig(NAMESPACE, kafkaCluster, true, 500);
        kafkaAdminClientOp = Mockito.spy(Admin.create(config.adminClientConfig()));

        var created = createTopic(kafkaCluster,
            kafkaTopic(NAMESPACE, "my-topic", SELECTOR, null, true, "my-topic", 1, 1, Map.of()));

        pauseTopic(NAMESPACE, "my-topic");

        kafkaAdminClientOp.deleteTopics(Set.of("my-topic"));
        kafkaAdminClientOp.createTopics(Set.of(new NewTopic("my-topic", 1, (short) 1)));

        var updated = unpauseTopic(NAMESPACE, "my-topic");

        assertNotEquals(created.getStatus().getTopicId(), updated.getStatus().getTopicId());
    }
}
