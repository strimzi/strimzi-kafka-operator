/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import io.micrometer.core.instrument.MeterRegistry;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
import io.strimzi.api.kafka.model.status.KafkaTopicStatus;
import io.strimzi.operator.common.MaxAttemptsExceededException;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.TopicDeletionDisabledException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.fabric8.kubernetes.client.Watcher.Action.ADDED;
import static io.fabric8.kubernetes.client.Watcher.Action.DELETED;
import static io.fabric8.kubernetes.client.Watcher.Action.MODIFIED;
import static java.util.Collections.*;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

@ExtendWith(VertxExtension.class)
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
public class TopicOperatorTest {

    private final Labels labels = Labels.fromString("app=strimzi");

    private final TopicName topicName = new TopicName("my-topic");
    private final ResourceName resourceName = topicName.asKubeName();
    private static Vertx vertx;
    private MockKafka mockKafka;
    private MockTopicStore mockTopicStore = new MockTopicStore();
    private MockK8s mockK8s = new MockK8s();
    private TopicOperator topicOperator;
    private MetricsProvider metrics;
    private final ObjectMeta metadata = new ObjectMeta();

    private static final Map<String, String> MANDATORY_CONFIG = new HashMap<>();

    static {
        MANDATORY_CONFIG.put(Config.ZOOKEEPER_CONNECT.key, "localhost:2181");
        MANDATORY_CONFIG.put(Config.KAFKA_BOOTSTRAP_SERVERS.key, "localhost:9092");
        MANDATORY_CONFIG.put(Config.NAMESPACE.key, "default");
        MANDATORY_CONFIG.put(Config.CLIENT_ID.key, "default-client-id");
        // Not mandatory, but makes the time test quicker
        MANDATORY_CONFIG.put(Config.TOPIC_METADATA_MAX_ATTEMPTS.key, "3");
    }

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx(new VertxOptions().setMetricsOptions(
                new MicrometerMetricsOptions()
                        .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                        .setEnabled(true)
        ));
    }

    @AfterAll
    public static void after() throws InterruptedException, ExecutionException {
        vertx.close().toCompletionStage().toCompletableFuture().get();
    }

    @BeforeEach
    public void setup() {
        mockKafka = new MockKafka();
        mockTopicStore = new MockTopicStore();
        mockK8s = new MockK8s();
        Config config = new Config(new HashMap<>(MANDATORY_CONFIG));
        metrics = createCleanMetricsProvider();
        topicOperator = new TopicOperator(vertx, mockKafka, mockK8s, mockTopicStore, labels, "default-namespace", config, metrics);
        metadata.setName(topicName.toString());
        Map<String, String> lbls = new HashMap<>();
        lbls.put("app", "strimzi");
        metadata.setLabels(lbls);
        metadata.setAnnotations(new HashMap<>());
    }

    @AfterEach
    public void teardown() {
        mockKafka = null;
        mockTopicStore = null;
        mockK8s = null;
        topicOperator = null;
        metrics = null;
    }

    private Map<String, String> map(String... pairs) {
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException();
        }
        Map<String, String> result = new HashMap<>(pairs.length / 2);
        for (int i = 0; i < pairs.length; i += 2) {
            result.put(pairs[i], pairs[i + 1]);
        }
        return result;
    }

    /** Test what happens when a non-topic KafkaTopic gets created in kubernetes */
    @Test
    public void testOnKafkaTopicAdded_ignorable(VertxTestContext context) {
        KafkaTopic kafkaTopic = new KafkaTopicBuilder().withMetadata(new ObjectMetaBuilder().withName("non-topic").build()).build();

        K8sTopicWatcher w = new K8sTopicWatcher(topicOperator, Future.succeededFuture(), () -> { });
        w.eventReceived(ADDED, kafkaTopic);
        mockKafka.assertEmpty(context);
        mockTopicStore.assertEmpty(context);
        context.completeNow();
    }

    /** Test what happens when a non-topic KafkaTopic gets created in kubernetes */
    @Test
    public void testOnKafkaTopicAdded_invalidResource(VertxTestContext context) {
        KafkaTopic kafkaTopic = new KafkaTopicBuilder()
                .withMetadata(new ObjectMetaBuilder().withName("invalid").withLabels(labels.labels()).build())
                .withNewSpec()
                    .withReplicas(1)
                    .withPartitions(1)
                    .withConfig(singletonMap(null, null))
                .endSpec()
            .build();
        String errorMessage = "KafkaTopic's spec.config has invalid entry: The key 'null' of the topic config is invalid: The value corresponding to the key must have a string, number or boolean value but the value was null";
        mockK8s.setGetFromNameResponse(new ResourceName(kafkaTopic), Future.succeededFuture(kafkaTopic));
        LogContext logContext = LogContext.kubeWatch(Watcher.Action.ADDED, kafkaTopic);
        topicOperator.onResourceEvent(logContext, kafkaTopic, ADDED).onComplete(ar -> {
            assertFailed(context, ar);
            context.verify(() -> assertThat(ar.cause(), instanceOf(InvalidTopicException.class)));
            context.verify(() -> assertThat(ar.cause().getMessage(), is(errorMessage)));
            mockKafka.assertEmpty(context);
            mockTopicStore.assertEmpty(context);
            assertNotReadyStatus(context, new InvalidTopicException(null, ar.cause().getMessage()));
            context.verify(() -> {
                MeterRegistry registry = metrics.meterRegistry();

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations").tag("kind", "KafkaTopic").counter().count(), is(0.0));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "KafkaTopic").counter().count(), is(0.0));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "KafkaTopic").counter().count(), is(0.0));

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().count(), is(0L));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().totalTime(TimeUnit.MILLISECONDS), is(0.0));

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "resource.state")
                        .tag("kind", "KafkaTopic")
                        .tag("name", "invalid")
                        .tag("resource-namespace", "default-namespace")
                        .tag("reason", errorMessage)
                        .gauge().value(), is(0.0));
            });
            context.completeNow();

        });
    }

    /**
     * Trigger {@link TopicOperator#onResourceEvent(LogContext, KafkaTopic, io.fabric8.kubernetes.client.Watcher.Action)}
     * and have the Kafka and TopicStore respond with the given exceptions.
     * @return future of resource completion.
     */
    private Future<Void> resourceAdded(VertxTestContext context, Exception storeException) {
        mockKafka.setCreateTopicResponse(topicName.toString(), null);
        mockKafka.setTopicExistsResult(t -> Future.succeededFuture(false));
        mockTopicStore.setCreateTopicResponse(topicName, storeException);

        System.out.println(metadata);
        KafkaTopic kafkaTopic = new KafkaTopicBuilder()
                .withMetadata(metadata)
                .withNewSpec()
                    .withReplicas(2)
                    .withPartitions(10)
                .endSpec()
            .build();
        mockKafka.setTopicMetadataResponses(
            topicName -> Future.succeededFuture(),
            topicName -> Future.succeededFuture(Utils.getTopicMetadata(TopicSerialization.fromTopicResource(kafkaTopic))));
        LogContext logContext = LogContext.kubeWatch(Watcher.Action.ADDED, kafkaTopic);
        mockK8s.setGetFromNameResponse(new ResourceName(kafkaTopic), Future.succeededFuture(kafkaTopic));

        return topicOperator.onResourceEvent(logContext, kafkaTopic, ADDED).onComplete(ar -> {
            if (storeException != null) {
                assertFailed(context, ar);
                Class<? extends Exception> expectedExceptionType;
                expectedExceptionType = Objects.requireNonNullElse(null, storeException).getClass();
                if (!expectedExceptionType.equals(ar.cause().getClass())) {
                    ar.cause().printStackTrace();
                }
                context.verify(() -> assertThat(ar.cause().getMessage(),  ar.cause().getClass().getName(), is(expectedExceptionType.getName())));
                TopicName topicName = TopicSerialization.fromTopicResource(kafkaTopic).getTopicName();
                mockKafka.assertExists(context, topicName);
                mockTopicStore.assertNotExists(context, topicName);
                //TODO mockK8s.assertContainsEvent(context, e -> "Error".equals(e.getKind()));
            } else {
                assertSucceeded(context, ar);
                Topic expectedTopic = TopicSerialization.fromTopicResource(kafkaTopic);
                mockKafka.assertContains(context, expectedTopic);
                mockTopicStore.assertContains(context, expectedTopic);
                mockK8s.assertNoEvents(context);
            }
        });

    }

    /**
     * 1. operator is notified that a KafkaTopic is created
     * 2. operator successfully creates topic in kafka
     * 3. operator successfully creates in topic store
     */
    @Test
    public void testOnKafkaTopicAdded(VertxTestContext context) {
        resourceAdded(context, null).onSuccess(x -> context.completeNow());
    }

    /**
     * 1. operator is notified that a KafkaTopic is created
     * 2. error when creating topic in kafka
     */
    @Test
    public void testOnKafkaTopicAdded_TopicExistsException(VertxTestContext context) {
        Exception createException = new TopicExistsException("");
        resourceAdded(context, createException).onComplete(x -> {
            // TODO check a k8s event got created
            // TODO what happens when we subsequently reconcile?
            assertNotReadyStatus(context, createException);
        })
        .onComplete(x -> context.completeNow());
    }

    void assertNotReadyStatus(VertxTestContext context, Exception createException) {
        List<KafkaTopicStatus> statuses = mockK8s.getStatuses();
        context.verify(() -> assertThat(statuses.size(), is(1)));
        context.verify(() -> assertThat(statuses.get(0).getObservedGeneration(), is(0L)));
        context.verify(() -> assertThat(statuses.get(0).getConditions().stream().anyMatch(
            condition -> "NotReady".equals(condition.getType())
                    && "True".equals(condition.getStatus())
                    && createException.getClass().getSimpleName().equals(condition.getReason())
                    && Objects.equals(createException.getMessage(), condition.getMessage())), is(true)));
    }

    /**
     * 1. operator is notified that a KafkaTopic is created
     * 2. error when creating topic in kafka
     */
    @Test
    public void testOnKafkaTopicAdded_ClusterAuthorizationException(VertxTestContext context) {
        Exception createException = new ClusterAuthorizationException("Test exception");
        resourceAdded(context, createException)
                    .onComplete(x -> assertNotReadyStatus(context, createException))
                    .onComplete(x -> context.completeNow());
        // TODO check a k8s event got created
        // TODO what happens when we subsequently reconcile?
    }

    /**
     * 1. operator is notified that a KafkaTopic is created
     * 2. operator successfully creates topic in kafka
     * 3. error when creating in topic store
     */
    @Test
    public void testOnKafkaTopicAdded_EntityExistsException(VertxTestContext context) {
        TopicStore.EntityExistsException storeException = new TopicStore.EntityExistsException();
        resourceAdded(context, storeException)
                // TODO what happens when we subsequently reconcile?
                .onComplete(x -> assertNotReadyStatus(context, storeException))
                .onComplete(x -> context.completeNow());
    }

    // TODO ^^ but a disconnected/loss of session error

    /**
     * 1. operator is notified that a topic is created
     * 2. operator successfully queries kafka to get topic metadata
     * 3. operator successfully creates KafkaTopic
     * 4. operator successfully creates in topic store
     */
    @Test
    public void testOnTopicCreated(VertxTestContext context) {
        TopicMetadata topicMetadata = Utils.getTopicMetadata(topicName.toString(),
                new org.apache.kafka.clients.admin.Config(Collections.emptyList()));

        mockTopicStore.setCreateTopicResponse(topicName, null);
        mockKafka.setTopicExistsResult(t -> Future.succeededFuture(true));
        mockKafka.setTopicMetadataResponse(topicName, topicMetadata, null);
        mockK8s.setCreateResponse(resourceName, null);
        LogContext logContext = LogContext.zkWatch("///", topicName.toString(), topicOperator.getNamespace(), topicName.toString());
        topicOperator.onTopicCreated(logContext, topicName).onComplete(ar -> {
            assertSucceeded(context, ar);
            mockK8s.assertExists(context, resourceName);
            Topic t = TopicSerialization.fromTopicMetadata(topicMetadata);
            mockTopicStore.assertContains(context, t);
            context.verify(() -> {
                MeterRegistry registry = metrics.meterRegistry();

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "resource.state")
                        .tag("kind", "KafkaTopic")
                        .tag("name", topicName.toString())
                        .tag("resource-namespace", "default-namespace")
                        .gauge().value(), is(1.0));
            });
            context.completeNow();
        });
    }

    /**
     * 1. operator is notified that a topic is created
     * 2. operator initially failed querying kafka to get topic metadata
     * 3. operator is subsequently successful in querying kafka to get topic metadata
     * 4. operator successfully creates KafkaTopic
     * 5. operator successfully creates in topic store
     */
    @Test
    public void testOnTopicCreated_retry(VertxTestContext context) {
        TopicMetadata topicMetadata = Utils.getTopicMetadata(topicName.toString(),
                new org.apache.kafka.clients.admin.Config(Collections.emptyList()));

        mockTopicStore.setCreateTopicResponse(topicName, null);
        AtomicInteger counter = new AtomicInteger();
        mockKafka.setTopicMetadataResponse(t -> {
            int count = counter.getAndIncrement();
            if (count == 3) {
                return Future.succeededFuture(topicMetadata);
            } else if (count < 3) {
                return Future.succeededFuture(null);
            }
            context.failNow(new Throwable("This should never happen"));
            return Future.failedFuture("This should never happen");
        });
        AtomicInteger counter2 = new AtomicInteger();
        mockKafka.setTopicExistsResult(t -> {
            int count = counter2.getAndIncrement();
            if (count == 3) {
                return Future.succeededFuture(false);
            } else if (count < 3) {
                return Future.succeededFuture(true);
            }
            context.failNow(new Throwable("This should never happen"));
            return Future.failedFuture("This should never happen");
        });
        mockK8s.setCreateResponse(resourceName, null);
        LogContext logContext = LogContext.zkWatch("///", topicName.toString(), topicOperator.getNamespace(), topicName.toString());
        topicOperator.onTopicCreated(logContext, topicName).onComplete(ar -> {
            assertSucceeded(context, ar);
            context.verify(() -> assertThat(counter.get(), is(4)));
            mockK8s.assertExists(context, resourceName);
            mockTopicStore.assertContains(context, TopicSerialization.fromTopicMetadata(topicMetadata));
            context.verify(() -> {
                MeterRegistry registry = metrics.meterRegistry();

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations").tag("kind", "KafkaTopic").counter().count(), is(1.0));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "KafkaTopic").counter().count(), is(1.0));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "KafkaTopic").counter().count(), is(0.0));

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().count(), is(1L));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "resource.state")
                        .tag("kind", "KafkaTopic")
                        .tag("name", topicName.toString())
                        .tag("resource-namespace", "default-namespace")
                        .gauge().value(), is(1.0));
            });
            context.completeNow();
        });
    }

    private <T> void assertSucceeded(VertxTestContext context, AsyncResult<T> ar) {
        if (ar.failed()) {
            ar.cause().printStackTrace();
        }
        context.verify(() -> assertThat(ar.cause() != null ? ar.cause().toString() : "", ar.succeeded(), is(true)));
    }

    private <T> void assertFailed(VertxTestContext context, AsyncResult<T> ar) {
        context.verify(() -> assertThat(String.valueOf(ar.result()), ar.succeeded(), is(false)));
    }


    /**
     * 1. operator is notified that a topic is created
     * 2. operator times out getting metadata
     */
    @Test
    public void testOnTopicCreated_retryTimeout(VertxTestContext context) {

        mockKafka.setTopicExistsResult(t -> Future.succeededFuture(true));
        mockKafka.setTopicMetadataResponse(topicName, null, null);
        LogContext logContext = LogContext.zkWatch("///", topicName.toString(), topicOperator.getNamespace(), topicName.toString());
        topicOperator.onTopicCreated(logContext, topicName).onComplete(ar -> {
            assertFailed(context, ar);
            context.verify(() -> assertThat(ar.cause().getClass().getName(), is(MaxAttemptsExceededException.class.getName())));
            mockK8s.assertNotExists(context, resourceName);
            mockTopicStore.assertNotExists(context, topicName);
            context.verify(() -> {
                MeterRegistry registry = metrics.meterRegistry();

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations").tag("kind", "KafkaTopic").counter().count(), is(1.0));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "KafkaTopic").counter().count(), is(0.0));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "KafkaTopic").counter().count(), is(0.0));

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().count(), is(0L));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().totalTime(TimeUnit.MILLISECONDS), is(0.0));
            });
            context.completeNow();
        });
    }

    /**
     * 0. ZK notifies of a change in topic config
     * 1. operator gets updated topic metadata
     * 2. operator updates k8s and topic store.
     */
    @Test
    public void testOnTopicChanged(VertxTestContext context) {
        Topic kubeTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar")).build();
        Topic kafkaTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "baz")).build();
        KafkaTopic resource = TopicSerialization.toTopicResource(kubeTopic, labels);

        mockKafka.setCreateTopicResponse(topicName.toString(), null)
                .createTopic(Reconciliation.DUMMY_RECONCILIATION, kafkaTopic);
        mockKafka.setTopicMetadataResponse(topicName, Utils.getTopicMetadata(kafkaTopic), null);
        //mockKafka.setUpdateTopicResponse(topicName -> Future.succeededFuture());

        mockTopicStore.setCreateTopicResponse(topicName, null)
                .create(kubeTopic);
        mockTopicStore.setUpdateTopicResponse(topicName, null);

        mockK8s.setCreateResponse(resourceName, null)
                .createResource(resource);
        mockK8s.setModifyResponse(resourceName, null);
        LogContext logContext = LogContext.zkWatch("///", topicName.toString(), topicOperator.getNamespace(), topicName.toString());
        topicOperator.onTopicConfigChanged(logContext, topicName).onComplete(ar -> {
            assertSucceeded(context, ar);
            context.verify(() -> assertThat(mockKafka.getTopicState(topicName).getConfig().get("cleanup.policy"), is("baz")));
            mockTopicStore.read(topicName).onComplete(ar2 -> {
                assertSucceeded(context, ar2);
                context.verify(() -> assertThat(ar2.result().getConfig().get("cleanup.policy"), is("baz")));
                context.completeNow();
            });
            mockK8s.getFromName(resourceName).onComplete(ar2 -> {
                assertSucceeded(context, ar2);
                context.verify(() -> assertThat(TopicSerialization.fromTopicResource(ar2.result()).getConfig().get("cleanup.policy"), is("baz")));
                context.completeNow();
            });

            context.verify(() -> {
                MeterRegistry registry = metrics.meterRegistry();

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations").tag("kind", "KafkaTopic").counter().count(), is(1.0));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "KafkaTopic").counter().count(), is(1.0));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "KafkaTopic").counter().count(), is(0.0));

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().count(), is(1L));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "resource.state")
                        .tag("kind", "KafkaTopic")
                        .tag("name", topicName.toString())
                        .tag("resource-namespace", "default-namespace")
                        .gauge().value(), is(1.0));
            });
            context.completeNow();
        });
    }

    // TODO error getting full topic metadata, and then reconciliation
    // TODO error creating KafkaTopic (exists), and then reconciliation

    /**
     * Test reconciliation when a resource has been created while the operator wasn't running
     */
    @Test
    public void testReconcile_withResource_noKafka_noPrivate(VertxTestContext context) {

        Topic kubeTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar"), new ObjectMeta()).build();

        Checkpoint async0 = context.checkpoint();
        mockKafka.setCreateTopicResponse(topicName.toString(), null);
        //mockKafka.setTopicMetadataResponse(topicName, null, null);
        mockKafka.setTopicMetadataResponse(
            topicName -> Future.succeededFuture(Utils.getTopicMetadata(kubeTopic)));

        mockTopicStore.setCreateTopicResponse(topicName, null);
        mockK8s.setCreateResponse(topicName.asKubeName(), null);
        KafkaTopic topicResource = TopicSerialization.toTopicResource(kubeTopic, labels);
        LogContext logContext = LogContext.kubeWatch(Watcher.Action.ADDED, topicResource);
        mockK8s.createResource(topicResource).onComplete(ar -> async0.flag());

        topicOperator.reconcile(reconciliation(logContext), logContext, null, kubeTopic, null, null).onComplete(reconcileResult -> {
            assertSucceeded(context, reconcileResult);
            mockKafka.assertExists(context, kubeTopic.getTopicName());
            mockTopicStore.assertExists(context, kubeTopic.getTopicName());
            mockK8s.assertNoEvents(context);
            mockTopicStore.read(topicName).onComplete(readResult -> {
                assertSucceeded(context, readResult);
                context.verify(() -> assertThat(readResult.result(), is(kubeTopic)));
                context.completeNow();
            });
        });
    }

    /**
     * Test reconciliation when a topic has been deleted while the operator
     * wasn't running
     */
    @Test
    public void testReconcile_withResource_noKafka_withPrivate(VertxTestContext context) {

        Topic kubeTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar"), new ObjectMeta()).build();

        Checkpoint async0 = context.checkpoint(2);
        KafkaTopic topicResource = TopicSerialization.toTopicResource(kubeTopic, labels);
        LogContext logContext = LogContext.kubeWatch(Watcher.Action.DELETED, topicResource);
        mockK8s.setCreateResponse(resourceName, null)
                .createResource(topicResource).onComplete(ar -> async0.flag());
        mockK8s.setDeleteResponse(resourceName, null);
        mockTopicStore.setCreateTopicResponse(topicName, null)
                .create(kubeTopic).onComplete(ar -> async0.flag());
        mockTopicStore.setDeleteTopicResponse(topicName, null);

        topicOperator.reconcile(reconciliation(logContext), logContext, null, kubeTopic, null, kubeTopic).onComplete(reconcileResult -> {
            assertSucceeded(context, reconcileResult);
            mockKafka.assertNotExists(context, kubeTopic.getTopicName());
            mockTopicStore.assertNotExists(context, kubeTopic.getTopicName());
            mockK8s.assertNotExists(context, kubeTopic.getResourceName());
            mockK8s.assertNoEvents(context);
            context.completeNow();
        });
    }

    /**
     * Test reconciliation when a topic has been created while the operator wasn't running
     */
    @Test
    public void testReconcile_noResource_withKafka_noPrivate(VertxTestContext context) {

        Topic kafkaTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar"), metadata).build();

        mockTopicStore.setCreateTopicResponse(topicName, null);
        mockK8s.setCreateResponse(topicName.asKubeName(), null);
        mockKafka.setCreateTopicResponse(topicName -> Future.succeededFuture());
        mockKafka.createTopic(Reconciliation.DUMMY_RECONCILIATION, kafkaTopic);


        LogContext logContext = LogContext.periodic(topicName.toString(), topicOperator.getNamespace(), topicName.toString());

        topicOperator.reconcile(reconciliation(logContext), logContext, null, null, kafkaTopic, null).onComplete(reconcileResult -> {
            assertSucceeded(context, reconcileResult);
            mockTopicStore.assertExists(context, topicName);
            mockK8s.assertExists(context, topicName.asKubeName());
            mockKafka.assertExists(context, topicName);
            mockK8s.assertNoEvents(context);
            mockTopicStore.read(topicName).onComplete(readResult -> {
                assertSucceeded(context, readResult);
                context.verify(() -> assertThat(readResult.result(), is(kafkaTopic)));
            });
            mockK8s.getFromName(topicName.asKubeName()).onComplete(readResult -> {
                assertSucceeded(context, readResult);
                context.verify(() -> assertThat(TopicSerialization.fromTopicResource(readResult.result()), is(kafkaTopic)));
            });

            context.verify(() -> assertThat(mockKafka.getTopicState(topicName), is(kafkaTopic)));
            context.verify(() -> {
                MeterRegistry registry = metrics.meterRegistry();

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations").tag("kind", "KafkaTopic").counter().count(), is(1.0));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "KafkaTopic").counter().count(), is(1.0));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "KafkaTopic").counter().count(), is(0.0));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.locked").tag("kind", "KafkaTopic").counter().count(), is(0.0));

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().count(), is(1L));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));
                context.completeNow();
            });
        });
    }

    TopicOperator.Reconciliation reconciliation(LogContext logContext) {
        return topicOperator.new Reconciliation(logContext, "test", true) {
            @Override
            public Future<Void> execute() {
                return Future.succeededFuture();
            }
        };
    }

    /**
     * Test reconciliation when a resource has been deleted while the operator
     * wasn't running
     */
    @Test
    public void testReconcile_noResource_withKafka_withPrivate(VertxTestContext context) throws InterruptedException {
        Topic kafkaTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar")).build();

        CountDownLatch async0 = new CountDownLatch(2);
        mockKafka.createTopic(Reconciliation.DUMMY_RECONCILIATION, kafkaTopic).onComplete(ar -> async0.countDown());
        mockKafka.setDeleteTopicResponse(topicName, null);
        mockTopicStore.setCreateTopicResponse(topicName, null);
        mockTopicStore.create(kafkaTopic).onComplete(ar -> async0.countDown());
        mockTopicStore.setDeleteTopicResponse(topicName, null);
        async0.await();
        LogContext logContext = LogContext.periodic(topicName.toString(), topicOperator.getNamespace(), topicName.toString());
        topicOperator.reconcile(reconciliation(logContext), logContext, null, null, kafkaTopic, kafkaTopic).onComplete(reconcileResult -> {
            assertSucceeded(context, reconcileResult);
            mockTopicStore.assertNotExists(context, topicName);
            mockK8s.assertNotExists(context, topicName.asKubeName());
            mockKafka.assertNotExists(context, topicName);
            mockK8s.assertNoEvents(context);
            context.completeNow();
        });
    }

    /**
     * Test reconciliation when a resource has been added both in kafka and in k8s while the operator was down, and both
     * topics are identical.
     */
    @Test
    public void testReconcile_withResource_withKafka_noPrivate_matching(VertxTestContext context) throws InterruptedException {
        Topic kubeTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar"), metadata).build();

        CountDownLatch async0 = new CountDownLatch(2);
        mockKafka.setCreateTopicResponse(topicName -> Future.succeededFuture());
        mockKafka.createTopic(Reconciliation.DUMMY_RECONCILIATION, kubeTopic).onComplete(ar -> async0.countDown());
        mockK8s.setCreateResponse(topicName.asKubeName(), null);
        KafkaTopic topicResource = TopicSerialization.toTopicResource(kubeTopic, labels);
        LogContext logContext = LogContext.periodic(topicName.toString(), topicOperator.getNamespace(), topicName.toString());
        mockK8s.createResource(topicResource).onComplete(ar -> async0.countDown());
        mockTopicStore.setCreateTopicResponse(topicName, null);
        async0.await();

        topicOperator.reconcile(reconciliation(logContext), logContext, null, kubeTopic, kubeTopic, null).onComplete(reconcileResult -> {
            assertSucceeded(context, reconcileResult);
            mockTopicStore.assertExists(context, topicName);
            mockK8s.assertExists(context, topicName.asKubeName());
            mockK8s.assertNoEvents(context);
            mockKafka.assertExists(context, topicName);
            mockTopicStore.read(topicName).onComplete(readResult -> {
                assertSucceeded(context, readResult);
                context.verify(() -> assertThat(readResult.result(), is(kubeTopic)));
                context.completeNow();
            });
            context.verify(() -> assertThat(mockKafka.getTopicState(topicName), is(kubeTopic)));
            context.verify(() -> {
                MeterRegistry registry = metrics.meterRegistry();

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations").tag("kind", "KafkaTopic").counter().count(), is(1.0));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "KafkaTopic").counter().count(), is(1.0));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "KafkaTopic").counter().count(), is(0.0));

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().count(), is(1L));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "resource.state")
                        .tag("kind", "KafkaTopic")
                        .tag("name", topicName.toString())
                        .tag("resource-namespace", "default-namespace")
                        .gauge().value(), is(0.0));
            });

        });
    }

    /**
     * Test reconciliation when a resource has been added both in kafka and in k8s while the operator was down, and both
     * topics are identical.
     */
    @Test
    public void testReconcile_withResource_withKafka_noPrivate_overriddenName(VertxTestContext context) throws InterruptedException {
        TopicName topicName = new TopicName("__consumer_offsets");
        ResourceName kubeName = new ResourceName("consumer-offsets");
        Topic kubeTopic = new Topic.Builder(topicName, kubeName, 10, (short) 2, map("cleanup.policy", "bar"), metadata).build();
        Topic kafkaTopic = new Topic.Builder(topicName, 10, (short) 2, map("cleanup.policy", "bar"), metadata).build();

        CountDownLatch async0 = new CountDownLatch(2);
        mockKafka.setCreateTopicResponse(topicName_ -> Future.succeededFuture());
        mockKafka.createTopic(Reconciliation.DUMMY_RECONCILIATION, kafkaTopic).onComplete(ar -> async0.countDown());
        mockK8s.setCreateResponse(kubeName, null);
        KafkaTopic topicResource = TopicSerialization.toTopicResource(kubeTopic, labels);
        LogContext logContext = LogContext.periodic(topicName.toString(), topicOperator.getNamespace(), topicName.toString());
        mockK8s.createResource(topicResource).onComplete(ar -> async0.countDown());
        mockTopicStore.setCreateTopicResponse(topicName, null);
        async0.await();

        topicOperator.reconcile(reconciliation(logContext), logContext, null, kubeTopic, kafkaTopic, null).onComplete(reconcileResult -> {
            assertSucceeded(context, reconcileResult);
            mockTopicStore.assertExists(context, topicName);
            mockK8s.assertExists(context, kubeName);
            mockK8s.assertNotExists(context, topicName.asKubeName());
            mockK8s.assertNoEvents(context);
            mockKafka.assertExists(context, topicName);
            mockTopicStore.read(topicName).onComplete(readResult -> {
                context.verify(() -> assertThat(readResult.result(), is(kubeTopic)));
                context.verify(() -> assertThat(readResult.result().getResourceName(), is(kubeName)));
                context.completeNow();
            });
            context.verify(() -> assertThat(mockKafka.getTopicState(topicName), is(kafkaTopic)));
            context.verify(() -> {
                MeterRegistry registry = metrics.meterRegistry();

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations").tag("kind", "KafkaTopic").counter().count(), is(1.0));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "KafkaTopic").counter().count(), is(1.0));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "KafkaTopic").counter().count(), is(0.0));

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().count(), is(1L));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));
            });

        });
    }

    /**
     * Test reconciliation when a resource has been added both in kafka and in k8s while the operator was down, and
     * the topics are irreconcilably different: Kafka wins
     */
    @Test
    public void testReconcile_withResource_withKafka_noPrivate_configsReconcilable(VertxTestContext context) {
        Topic kubeTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar"), metadata).build();
        Topic kafkaTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("unclean.leader.election.enable", "true"), metadata).build();
        Topic mergedTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("unclean.leader.election.enable", "true", "cleanup.policy", "bar"), metadata).build();

        mockKafka.setCreateTopicResponse(topicName -> Future.succeededFuture());
        mockKafka.createTopic(Reconciliation.DUMMY_RECONCILIATION, kafkaTopic);
        mockKafka.setUpdateTopicResponse(topicName -> Future.succeededFuture());

        KafkaTopic topic = TopicSerialization.toTopicResource(kubeTopic, labels);
        LogContext logContext = LogContext.periodic(topicName.toString(), topicOperator.getNamespace(), topicName.toString());
        mockK8s.setCreateResponse(topicName.asKubeName(), null);
        mockK8s.createResource(topic);
        mockK8s.setModifyResponse(topicName.asKubeName(), null);
        mockTopicStore.setCreateTopicResponse(topicName, null);

        topicOperator.reconcile(reconciliation(logContext), logContext, topic, kubeTopic, kafkaTopic, null).onComplete(reconcileResult -> {
            assertSucceeded(context, reconcileResult);
            mockTopicStore.assertExists(context, topicName);
            mockK8s.assertExists(context, topicName.asKubeName());
            mockKafka.assertExists(context, topicName);
            mockTopicStore.read(topicName).onComplete(readResult -> {
                assertSucceeded(context, readResult);
                context.verify(() -> assertThat(readResult.result(), is(mergedTopic)));
            });
            mockK8s.getFromName(topicName.asKubeName()).onComplete(readResult -> {
                assertSucceeded(context, readResult);
                context.verify(() -> assertThat(TopicSerialization.fromTopicResource(readResult.result()), is(mergedTopic)));
            });

            context.verify(() -> assertThat(mockKafka.getTopicState(topicName), is(mergedTopic)));
            context.verify(() -> {
                MeterRegistry registry = metrics.meterRegistry();

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations").tag("kind", "KafkaTopic").counter().count(), is(1.0));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "KafkaTopic").counter().count(), is(1.0));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "KafkaTopic").counter().count(), is(0.0));

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().count(), is(1L));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));
                context.completeNow();
            });

        });
    }

    /**
     * Test reconciliation when a resource has been added both in kafka and in k8s while the operator was down, and
     * the topics are irreconcilably different: Kafka wins
     */
    @Test
    public void testReconcile_withResource_withKafka_noPrivate_irreconcilable(VertxTestContext context) {
        Topic kubeTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar"), metadata).build();
        Topic kafkaTopic = new Topic.Builder(topicName.toString(), 12, (short) 2, map("cleanup.policy", "baz"), metadata).build();

        mockKafka.setCreateTopicResponse(topicName -> Future.succeededFuture());
        mockKafka.createTopic(Reconciliation.DUMMY_RECONCILIATION, kafkaTopic);

        KafkaTopic topic = TopicSerialization.toTopicResource(kubeTopic, labels);
        LogContext logContext = LogContext.periodic(topicName.toString(), topicOperator.getNamespace(), topicName.toString());
        mockK8s.setCreateResponse(topicName.asKubeName(), null);
        mockK8s.createResource(topic);
        mockK8s.setModifyResponse(topicName.asKubeName(), null);
        mockTopicStore.setCreateTopicResponse(topicName, null);

        topicOperator.reconcile(reconciliation(logContext), logContext, topic, kubeTopic, kafkaTopic, null).onComplete(reconcileResult -> {
            assertSucceeded(context, reconcileResult);
            mockK8s.assertContainsEvent(context, e ->
                    e.getMessage().contains("KafkaTopic is incompatible with the topic metadata. " +
                            "The topic metadata will be treated as canonical."));
            mockTopicStore.assertExists(context, topicName);
            mockK8s.assertExists(context, topicName.asKubeName());
            mockKafka.assertExists(context, topicName);
            mockTopicStore.read(topicName).onComplete(readResult -> {
                assertSucceeded(context, readResult);
                context.verify(() -> assertThat(readResult.result(), is(kafkaTopic)));
            });
            mockK8s.getFromName(topicName.asKubeName()).onComplete(readResult -> {
                assertSucceeded(context, readResult);
                context.verify(() -> assertThat(TopicSerialization.fromTopicResource(readResult.result()), is(kafkaTopic)));
            });

            context.verify(() -> {
                assertThat(mockKafka.getTopicState(topicName), is(kafkaTopic));
                context.completeNow();
            });

        });
    }

    /**
     * Test reconciliation when a resource has been changed both in kafka and in k8s while the operator was down, and
     * a 3 way merge is needed.
     */
    @Test
    public void testReconcile_withResource_withKafka_withPrivate_3WayMerge(VertxTestContext context) {
        Topic kubeTopic = new Topic.Builder(topicName, resourceName, 10, (short) 2, map("cleanup.policy", "bar"), metadata).build();
        Topic kafkaTopic = new Topic.Builder(topicName, resourceName, 12, (short) 2, map("cleanup.policy", "baz"), metadata).build();
        Topic privateTopic = new Topic.Builder(topicName, resourceName, 10, (short) 2, map("cleanup.policy", "baz"), metadata).build();
        Topic resultTopic = new Topic.Builder(topicName, resourceName, 12, (short) 2, map("cleanup.policy", "bar"), metadata).build();

        mockKafka.setCreateTopicResponse(topicName -> Future.succeededFuture());
        mockKafka.createTopic(Reconciliation.DUMMY_RECONCILIATION, kafkaTopic);
        mockKafka.setUpdateTopicResponse(topicName -> Future.succeededFuture());

        KafkaTopic resource = TopicSerialization.toTopicResource(kubeTopic, labels);
        LogContext logContext = LogContext.periodic(topicName.toString(), topicOperator.getNamespace(), topicName.toString());
        mockK8s.setCreateResponse(topicName.asKubeName(), null);
        mockK8s.createResource(resource);
        mockK8s.setModifyResponse(topicName.asKubeName(), null);
        mockTopicStore.setCreateTopicResponse(topicName, null);
        mockTopicStore.create(privateTopic);

        topicOperator.reconcile(reconciliation(logContext), logContext, resource, kubeTopic, kafkaTopic, privateTopic).onComplete(reconcileResult -> {
            assertSucceeded(context, reconcileResult);
            mockK8s.assertNoEvents(context);
            mockTopicStore.read(topicName).onComplete(readResult -> {
                assertSucceeded(context, readResult);
                context.verify(() -> assertThat(readResult.result(), is(resultTopic)));
            });
            mockK8s.getFromName(topicName.asKubeName()).onComplete(readResult -> {
                assertSucceeded(context, readResult);
                context.verify(() -> assertThat(TopicSerialization.fromTopicResource(readResult.result()), is(resultTopic)));
            });
            context.verify(() -> {
                assertThat(mockKafka.getTopicState(topicName), is(resultTopic));
                context.completeNow();
            });
        });
    }

    // TODO 3way reconcilation where kafka and kube agree
    // TODO 3way reconcilation where all three agree
    // TODO 3way reconcilation with conflict
    // TODO reconciliation where only private state exists => delete the private state

    // TODO tests for the other reconciliation cases
    // + non-matching predicate
    // + error cases

    private Future<Void> resourceRemoved(VertxTestContext context, Exception deleteTopicException, Exception storeException) {
        Topic kubeTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar")).build();

        mockKafka.setCreateTopicResponse(topicName.toString(), null)
                .createTopic(Reconciliation.DUMMY_RECONCILIATION, kubeTopic);
        mockKafka.setTopicMetadataResponse(topicName, Utils.getTopicMetadata(kubeTopic), null);
        mockKafka.setDeleteTopicResponse(topicName, deleteTopicException);

        mockTopicStore.setCreateTopicResponse(topicName, null)
                .create(kubeTopic);
        mockTopicStore.setDeleteTopicResponse(topicName, storeException);

        KafkaTopic resource = TopicSerialization.toTopicResource(kubeTopic, labels);
        LogContext logContext = LogContext.kubeWatch(Watcher.Action.DELETED, resource);

        return topicOperator.onResourceEvent(logContext, resource, DELETED).onComplete(ar -> {
            if (deleteTopicException != null || storeException != null) {

                if (deleteTopicException instanceof TopicDeletionDisabledException) {
                    // For the specific topic deletion disabled exception the exception will be caught and the resource
                    // event will be processed successfully
                    assertSucceeded(context, ar);
                } else {
                    // For all other exceptions the resource event will fail.
                    assertFailed(context, ar);
                }

                if (deleteTopicException != null) {
                    // If there was a broker deletion exception the broker topic should still exist
                    mockKafka.assertExists(context, kubeTopic.getTopicName());
                } else {
                    mockKafka.assertNotExists(context, kubeTopic.getTopicName());
                }

                if (deleteTopicException instanceof TopicDeletionDisabledException) {
                    //If there was a topic deletion disabled exception then the Store topic would still be deleted.
                    mockTopicStore.assertNotExists(context, kubeTopic.getTopicName());
                } else {
                    mockTopicStore.assertExists(context, kubeTopic.getTopicName());
                }

            } else {
                assertSucceeded(context, ar);
                mockKafka.assertNotExists(context, kubeTopic.getTopicName());
                mockTopicStore.assertNotExists(context, kubeTopic.getTopicName());
            }
        });
    }

    @Test
    public void testOnKafkaTopicChanged(VertxTestContext context) {
        Topic kubeTopic = new Topic.Builder(topicName, resourceName, 10, (short) 2, map("cleanup.policy", "baz"), null).build();
        Topic kafkaTopic = new Topic.Builder(topicName, resourceName, 10, (short) 2, map("cleanup.policy", "bar"), null).build();
        KafkaTopic resource = TopicSerialization.toTopicResource(kubeTopic, labels);
        LogContext logContext = LogContext.zkWatch("///", topicName.toString(), topicOperator.getNamespace(), topicName.toString());

        mockKafka.setCreateTopicResponse(topicName.toString(), null)
                .createTopic(Reconciliation.DUMMY_RECONCILIATION, kafkaTopic);
        mockKafka.setTopicMetadataResponse(topicName, Utils.getTopicMetadata(kafkaTopic), null);
        mockKafka.setUpdateTopicResponse(topicName -> Future.succeededFuture());

        mockTopicStore.setCreateTopicResponse(topicName, null)
                .create(kafkaTopic);
        mockTopicStore.setUpdateTopicResponse(topicName, null);

        mockK8s.setCreateResponse(resourceName, null);
        mockK8s.createResource(resource).onComplete(ar -> assertSucceeded(context, ar));
        mockK8s.setModifyResponse(resourceName, null);

        CountDownLatch async = new CountDownLatch(3);
        topicOperator.onResourceEvent(logContext, resource, MODIFIED).onComplete(ar -> {
            assertSucceeded(context, ar);
            context.verify(() -> assertThat(mockKafka.getTopicState(topicName).getConfig().get("cleanup.policy"), is("baz")));
            mockTopicStore.read(topicName).onComplete(ar2 -> {
                assertSucceeded(context, ar2);
                context.verify(() -> assertThat(ar2.result().getConfig().get("cleanup.policy"), is("baz")));
                async.countDown();
            });
            mockK8s.getFromName(resourceName).onComplete(ar2 -> {
                assertSucceeded(context, ar2);
                context.verify(() -> assertThat(ar2.result(), is(notNullValue())));
                context.verify(() -> assertThat(TopicSerialization.fromTopicResource(ar2.result()).getConfig().get("cleanup.policy"), is("baz")));
                async.countDown();
            });

            context.verify(() -> {
                MeterRegistry registry = metrics.meterRegistry();

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "resource.state")
                        .tag("kind", "KafkaTopic")
                        .tag("name", topicName.toString())
                        .tag("resource-namespace", "default-namespace")
                        .gauge().value(), is(1.0));
                context.completeNow();
            });


        });
    }

    @Test
    public void testOnKafkaTopicRemoved(VertxTestContext context) {
        Future<Void> removalFuture = resourceRemoved(context, null, null);
        removalFuture.onSuccess(x -> context.completeNow());
        removalFuture.onFailure(context::failNow);
    }

    @Test
    public void testOnKafkaTopicRemoved_UnknownTopicOrPartitionException(VertxTestContext context) {
        Exception deleteTopicException = new UnknownTopicOrPartitionException();
        resourceRemoved(context, deleteTopicException, null).onComplete(x -> context.completeNow());
    }

    @Test
    public void testOnKafkaTopicRemoved_NoSuchEntityExistsException(VertxTestContext context) {
        Exception storeException = new TopicStore.NoSuchEntityExistsException();
        resourceRemoved(context, null, storeException).onComplete(x -> context.completeNow());
    }

    @Test
    public void testOnKafkaTopicRemoved_TopicDeletionDisabledException(VertxTestContext context) {
        // Deals with the situation where the delete.topic.enable=false config is set in the broker
        Exception deleteTopicException = new TopicDeletionDisabledException("Topic deletion disable");
        resourceRemoved(context, deleteTopicException, null).onComplete(x -> context.completeNow());
    }


    private void topicDeleted(VertxTestContext context, Exception storeException, Exception k8sException) {
        Topic kubeTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar")).withMapName(resourceName).build();

        mockK8s.setCreateResponse(resourceName, null)
                .createResource(TopicSerialization.toTopicResource(kubeTopic, labels));
        mockK8s.setDeleteResponse(resourceName, k8sException);

        mockTopicStore.setCreateTopicResponse(topicName, null)
                .create(kubeTopic);
        mockTopicStore.setDeleteTopicResponse(topicName, storeException);

        mockKafka.setTopicExistsResult(t -> Future.succeededFuture(false));

        LogContext logContext = LogContext.zkWatch("///", topicName.toString(), topicOperator.getNamespace(), topicName.toString());
        topicOperator.onTopicDeleted(logContext, topicName).onComplete(ar -> {
            if (k8sException != null
                    || storeException != null) {
                assertFailed(context, ar);
                 if (k8sException == null) {
                    mockK8s.assertNotExists(context, resourceName);
                } else {
                    mockK8s.assertExists(context, resourceName);
                }
                mockTopicStore.assertExists(context, topicName);
            } else {
                assertSucceeded(context, ar);
                mockK8s.assertNotExists(context, resourceName);
                mockTopicStore.assertNotExists(context, topicName);
            }
            context.completeNow();
        });
    }

    @Test
    public void testOnTopicDeleted(VertxTestContext context) {
        topicDeleted(context, null, null);
    }

    @Test
    public void testOnTopicDeletedSpurious(VertxTestContext context) {
        topicDeleted(context, null, null);
    }

    @Test
    public void testOnTopicDeleted_NoSuchEntityExistsException(VertxTestContext context) {
        Exception storeException = new TopicStore.NoSuchEntityExistsException();
        topicDeleted(context, storeException, null);
    }

    @Test
    public void testOnTopicDeleted_KubernetesClientException(VertxTestContext context) {
        Exception k8sException = new KubernetesClientException("Test exception");
        topicDeleted(context, null, k8sException);
    }

    @Test
    public void testReconcileAllTopics_listTopicsFails(VertxTestContext context) {
        RuntimeException error = new RuntimeException("some failure");
        mockKafka.setTopicsListResponse(Future.failedFuture(error));

        Future<?> reconcileFuture = topicOperator.reconcileAllTopics("periodic");

        reconcileFuture.onComplete(context.failing(e -> {
            context.verify(() -> assertThat(e.getMessage(), is("Error listing existing topics during periodic reconciliation")));
            context.verify(() -> assertThat(e.getCause(), is(error)));
            context.completeNow();
        }));
    }

    @Test
    public void testReconcileAllTopics_getResourceFails(VertxTestContext context) {
        RuntimeException error = new RuntimeException("some failure");
        mockKafka.setTopicsListResponse(Future.succeededFuture(singleton(topicName.toString())));
        mockKafka.setDeleteTopicResponse(topicName, null);
        mockTopicStore.setGetTopicResponse(topicName, Future.failedFuture(error));

        Future<?> reconcileFuture = topicOperator.reconcileAllTopics("periodic");

        reconcileFuture.onComplete(context.failing(e -> {
            context.verify(() -> assertThat(e.getMessage(), is("Error getting topic my-topic from topic store during periodic reconciliation")));
            context.verify(() -> assertThat(e.getCause(), is(error)));
            context.verify(() -> {
                MeterRegistry registry = metrics.meterRegistry();

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations").tag("kind", "KafkaTopic").counter().count(), is(1.0));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "KafkaTopic").counter().count(), is(0.0));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "KafkaTopic").counter().count(), is(1.0));

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().count(), is(1L));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));
            });
            context.completeNow();
        }));
    }

    @Test
    public void testReconcileAllTopics_listMapsFails(VertxTestContext context) {
        RuntimeException error = new RuntimeException("some failure");
        mockKafka.setTopicsListResponse(Future.succeededFuture(emptySet()));
        mockK8s.setListMapsResult(() -> Future.failedFuture(error));

        Future<?> reconcileFuture = topicOperator.reconcileAllTopics("periodic");

        reconcileFuture.onComplete(context.failing(e -> {
            context.verify(() -> assertThat(e.getMessage(), is("Error listing existing KafkaTopics during periodic reconciliation")));
            context.verify(() -> assertThat(e.getCause(), is(error)));
            context.completeNow();
        }));
    }

    @Test
    public void testReconcileMetrics(VertxTestContext context) {
        mockKafka.setTopicsListResponse(Future.succeededFuture(emptySet()));
        mockKafka.setUpdateTopicResponse(topicName -> Future.succeededFuture());
        resourceAdded(context, null)
                .compose(x -> topicOperator.reconcileAllTopics("periodic"))
                .onComplete(context.succeeding(e -> context.verify(() -> {
                        System.out.println("Here");
                        MeterRegistry registry = metrics.meterRegistry();

                        assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations").tag("kind", "KafkaTopic").counter().count(), is(1.0));
                        assertThat(registry.get(TopicOperator.METRICS_PREFIX + "resources.paused").tag("kind", "KafkaTopic").gauge().value(), is(0.0));
                        assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "KafkaTopic").counter().count(), is(1.0));
                        assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "KafkaTopic").counter().count(), is(0.0));

                        assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().count(), is(1L));
                        assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

                        assertThat(registry.get(TopicOperator.METRICS_PREFIX + "resource.state")
                                .tag("kind", "KafkaTopic")
                                .tag("name", topicName.toString())
                                .tag("resource-namespace", "default-namespace")
                                .gauge().value(), is(1.0));

                        context.completeNow();
                    })));
    }

    @Test
    public void testReconcileMetricsWithPausedTopic(VertxTestContext context) {
        mockKafka.setTopicsListResponse(Future.succeededFuture(emptySet()));
        mockKafka.setUpdateTopicResponse(topicName -> Future.succeededFuture());
        metadata.getAnnotations().put("strimzi.io/pause-reconciliation", "false");

        resourceAdded(context, null)
                .compose(x ->  topicOperator.reconcileAllTopics("periodic"))
                .onComplete(context.succeeding(e -> context.verify(() -> {
                        MeterRegistry registry = metrics.meterRegistry();

                        assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations").tag("kind", "KafkaTopic").counter().count(), is(1.0));
                        assertThat(registry.get(TopicOperator.METRICS_PREFIX + "resources.paused").tag("kind", "KafkaTopic").gauge().value(), is(0.0));
                        assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "KafkaTopic").counter().count(), is(1.0));
                        assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "KafkaTopic").counter().count(), is(0.0));

                        assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().count(), is(1L));
                        assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

                        assertThat(registry.get(TopicOperator.METRICS_PREFIX + "resource.state")
                                .tag("kind", "KafkaTopic")
                                .tag("name", topicName.toString())
                                .tag("resource-namespace", "default-namespace")
                                .gauge().value(), is(1.0));
        })))
        .onSuccess(x -> metadata.getAnnotations().put("strimzi.io/pause-reconciliation", "true"))
        .compose(x  -> resourceAdded(context, null))
        .onComplete(x -> topicOperator.reconcileAllTopics("periodic2"))
        .onComplete(context.succeeding(f -> context.verify(() -> {
            MeterRegistry registry = metrics.meterRegistry();

            assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations").tag("kind", "KafkaTopic").counter().count(), is(2.0));
            assertThat(registry.get(TopicOperator.METRICS_PREFIX + "resources.paused").tag("kind", "KafkaTopic").gauge().value(), is(1.0));
            assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "KafkaTopic").counter().count(), is(1.0));
            assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "KafkaTopic").counter().count(), is(0.0));

            assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().count(), is(1L));
            assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

            assertThat(registry.get(TopicOperator.METRICS_PREFIX + "resource.state")
                    .tag("kind", "KafkaTopic")
                    .tag("name", topicName.toString())
                    .tag("resource-namespace", "default-namespace")
                    .gauge().value(), is(1.0));
        })))
        .onSuccess(x -> context.completeNow());

    }

    @Test
    public void testReconcileMetricsDeletedTopic(VertxTestContext context) {
        mockKafka.setTopicsListResponse(Future.succeededFuture(emptySet()));
        mockKafka.setUpdateTopicResponse(topicName -> Future.succeededFuture());
        topicOperator.reconcileAllTopics("periodic")
                .onSuccess(x -> resourceAdded(context, null))
                .onSuccess(x -> resourceRemoved(context, null, null))
                .onComplete(context.succeeding(e -> {
                    MeterRegistry registry = metrics.meterRegistry();

                    assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations").tag("kind", "KafkaTopic").counter().count(), is(0.0));
                    assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "KafkaTopic").counter().count(), is(0.0));
                    assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "KafkaTopic").counter().count(), is(0.0));

                    assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().count(), is(0L));
                    assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().totalTime(TimeUnit.MILLISECONDS), is(0.0));
                    context.completeNow();
        }));
    }

    /**
     * Created new MetricsProvider and makes sure it doesn't contain any metrics from previous tests.
     *
     * @return  Clean MetricsProvider
     */
    public MetricsProvider createCleanMetricsProvider() {
        MetricsProvider metrics = new MicrometerMetricsProvider();
        MeterRegistry registry = metrics.meterRegistry();

        registry.forEachMeter(registry::remove);

        return metrics;
    }

    // TODO tests for nasty races (e.g. create on both ends, update on one end and delete on the other)
    // I think in these cases we should seek to detect the concurrent modification
    // and perform a full reconciliation, possibly after a backoff time
    // (to cover the case where topic config and other aspects get changed via multiple calls)
    // TODO test for zookeeper session timeout
    // TODO test for Kubernetes connection death
}
