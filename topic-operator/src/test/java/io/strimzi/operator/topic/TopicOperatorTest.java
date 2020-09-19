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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.fabric8.kubernetes.client.Watcher.Action.ADDED;
import static io.fabric8.kubernetes.client.Watcher.Action.DELETED;
import static io.fabric8.kubernetes.client.Watcher.Action.MODIFIED;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
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
    private Config config;
    private MetricsProvider metrics;
    private ObjectMeta metadata = new ObjectMeta();

    private static final Map<String, String> MANDATORY_CONFIG = new HashMap<>();

    static {
        MANDATORY_CONFIG.put(Config.ZOOKEEPER_CONNECT.key, "localhost:2181");
        MANDATORY_CONFIG.put(Config.KAFKA_BOOTSTRAP_SERVERS.key, "localhost:9092");
        MANDATORY_CONFIG.put(Config.NAMESPACE.key, "default");
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
    public static void after() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        vertx.close(done -> latch.countDown());
        latch.await(30, TimeUnit.SECONDS);
    }

    @BeforeEach
    public void setup() {
        mockKafka = new MockKafka();
        mockTopicStore = new MockTopicStore();
        mockK8s = new MockK8s();
        config = new Config(new HashMap<>(MANDATORY_CONFIG));
        metrics = createCleanMetricsProvider();
        topicOperator = new TopicOperator(vertx, mockKafka, mockK8s, mockTopicStore, labels, "default-namespace", config, metrics);
        metadata.setName(topicName.toString());
        Map<String, String> lbls = new HashMap<>();
        lbls.put("app", "strimzi");
        metadata.setLabels(lbls);
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

        Checkpoint async = context.checkpoint();
        K8sTopicWatcher w = new K8sTopicWatcher(topicOperator, Future.succeededFuture());
        w.eventReceived(ADDED, kafkaTopic);
        mockKafka.assertEmpty(context);
        mockTopicStore.assertEmpty(context);
        async.flag();
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
        mockK8s.setGetFromNameResponse(new ResourceName(kafkaTopic), Future.succeededFuture(kafkaTopic));
        LogContext logContext = LogContext.kubeWatch(Watcher.Action.ADDED, kafkaTopic);
        Checkpoint async = context.checkpoint();
        topicOperator.onResourceEvent(logContext, kafkaTopic, ADDED).onComplete(ar -> {
            assertFailed(context, ar);
            context.verify(() -> assertThat(ar.cause(), instanceOf(InvalidTopicException.class)));
            context.verify(() -> assertThat(ar.cause().getMessage(), is("KafkaTopic's spec.config has invalid entry: The key 'null' of the topic config is invalid: The value corresponding to the key must have a string, number or boolean value but the value was null")));
            mockKafka.assertEmpty(context);
            mockTopicStore.assertEmpty(context);
            assertNotReadyStatus(context, new InvalidTopicException(null, ar.cause().getMessage()));
            context.verify(() -> {
                MeterRegistry registry = metrics.meterRegistry();

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations").tag("kind", "KafkaTopic").counter().count(), is(1.0));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "KafkaTopic").counter().count(), is(0.0));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "KafkaTopic").counter().count(), is(0.0));

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().count(), is(0L));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().totalTime(TimeUnit.MILLISECONDS), is(0.0));
            });
            async.flag();

        });
    }

    /**
     * Trigger {@link TopicOperator#onResourceEvent(LogContext, KafkaTopic, io.fabric8.kubernetes.client.Watcher.Action)}
     * and have the Kafka and TopicStore respond with the given exceptions.
     */
    private TopicOperator resourceAdded(VertxTestContext context, Exception createException, Exception storeException) throws InterruptedException {
        mockKafka.setCreateTopicResponse(topicName.toString(), createException);
        mockTopicStore.setCreateTopicResponse(topicName, storeException);

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
        Checkpoint async = context.checkpoint();
        mockK8s.setGetFromNameResponse(new ResourceName(kafkaTopic), Future.succeededFuture(kafkaTopic));

        topicOperator.onResourceEvent(logContext, kafkaTopic, ADDED).onComplete(ar -> {
            if (createException != null
                    || storeException != null) {
                assertFailed(context, ar);
                Class<? extends Exception> expectedExceptionType;
                if (createException != null) {
                    expectedExceptionType = createException.getClass();
                } else {
                    expectedExceptionType = storeException.getClass();
                }
                if (!expectedExceptionType.equals(ar.cause().getClass())) {
                    ar.cause().printStackTrace();
                }
                context.verify(() -> assertThat(ar.cause().getMessage(),  ar.cause().getClass().getName(), is(expectedExceptionType.getName())));
                TopicName topicName = TopicSerialization.fromTopicResource(kafkaTopic).getTopicName();
                if (createException != null) {
                    mockKafka.assertNotExists(context, topicName);
                } else {
                    mockKafka.assertExists(context, topicName);
                }
                mockTopicStore.assertNotExists(context, topicName);
                //TODO mockK8s.assertContainsEvent(context, e -> "Error".equals(e.getKind()));
            } else {
                assertSucceeded(context, ar);
                Topic expectedTopic = TopicSerialization.fromTopicResource(kafkaTopic);
                mockKafka.assertContains(context, expectedTopic);
                mockTopicStore.assertContains(context, expectedTopic);
                mockK8s.assertNoEvents(context);
            }
            async.flag();
        });
        if (!context.awaitCompletion(60, TimeUnit.SECONDS)) {
            context.failNow(new Throwable("Test timeout"));
        }

        return topicOperator;
    }

    /**
     * 1. operator is notified that a KafkaTopic is created
     * 2. operator successfully creates topic in kafka
     * 3. operator successfully creates in topic store
     */
    @Test
    public void testOnKafkaTopicAdded(VertxTestContext context) throws InterruptedException {
        resourceAdded(context, null, null);
    }

    /**
     * 1. operator is notified that a KafkaTopic is created
     * 2. error when creating topic in kafka
     */
    @Test
    public void testOnKafkaTopicAdded_TopicExistsException(VertxTestContext context) throws InterruptedException {
        Exception createException = new TopicExistsException("");
        resourceAdded(context, createException, null);
        // TODO check a k8s event got created
        // TODO what happens when we subsequently reconcile?
        assertNotReadyStatus(context, createException);
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
    public void testOnKafkaTopicAdded_ClusterAuthorizationException(VertxTestContext context) throws InterruptedException {
        Exception createException = new ClusterAuthorizationException("Test exception");
        TopicOperator op = resourceAdded(context, createException, null);
        assertNotReadyStatus(context, createException);
        // TODO check a k8s event got created
        // TODO what happens when we subsequently reconcile?
    }

    /**
     * 1. operator is notified that a KafkaTopic is created
     * 2. operator successfully creates topic in kafka
     * 3. error when creating in topic store
     */
    @Test
    public void testOnKafkaTopicAdded_EntityExistsException(VertxTestContext context) throws InterruptedException {
        TopicStore.EntityExistsException storeException = new TopicStore.EntityExistsException();
        resourceAdded(context,
                null,
                storeException);
        // TODO what happens when we subsequently reconcile?
        assertNotReadyStatus(context, storeException);
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
        mockKafka.setTopicMetadataResponse(topicName, topicMetadata, null);
        mockK8s.setCreateResponse(resourceName, null);
        LogContext logContext = LogContext.zkWatch("///", topicName.toString());
        Checkpoint async = context.checkpoint();
        topicOperator.onTopicCreated(logContext, topicName).onComplete(ar -> {
            assertSucceeded(context, ar);
            mockK8s.assertExists(context, resourceName);
            Topic t = TopicSerialization.fromTopicMetadata(topicMetadata);
            mockTopicStore.assertContains(context, t);
            async.flag();
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
        mockK8s.setCreateResponse(resourceName, null);
        LogContext logContext = LogContext.zkWatch("///", topicName.toString());
        Checkpoint async = context.checkpoint();
        topicOperator.onTopicCreated(logContext, topicName).onComplete(ar -> {
            assertSucceeded(context, ar);
            context.verify(() -> assertThat(counter.get(), is(4)));
            mockK8s.assertExists(context, resourceName);
            mockTopicStore.assertContains(context, TopicSerialization.fromTopicMetadata(topicMetadata));
            context.verify(() -> {
                MeterRegistry registry = metrics.meterRegistry();

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations").tag("kind", "KafkaTopic").counter().count(), is(1.0));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "KafkaTopic").counter().count(), is(0.0));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "KafkaTopic").counter().count(), is(0.0));

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().count(), is(0L));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().totalTime(TimeUnit.MILLISECONDS), is(0.0));
            });
            async.flag();
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

        mockKafka.setTopicMetadataResponse(topicName, null, null);
        LogContext logContext = LogContext.zkWatch("///", topicName.toString());
        Checkpoint async = context.checkpoint();
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
            async.flag();
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
        Topic privateTopic = kubeTopic;
        KafkaTopic resource = TopicSerialization.toTopicResource(kubeTopic, labels);

        mockKafka.setCreateTopicResponse(topicName.toString(), null)
                .createTopic(kafkaTopic);
        mockKafka.setTopicMetadataResponse(topicName, Utils.getTopicMetadata(kafkaTopic), null);
        //mockKafka.setUpdateTopicResponse(topicName -> Future.succeededFuture());

        mockTopicStore.setCreateTopicResponse(topicName, null)
                .create(privateTopic);
        mockTopicStore.setUpdateTopicResponse(topicName, null);

        mockK8s.setCreateResponse(resourceName, null)
                .createResource(resource);
        mockK8s.setModifyResponse(resourceName, null);
        LogContext logContext = LogContext.zkWatch("///", topicName.toString());
        Checkpoint async = context.checkpoint(3);
        topicOperator.onTopicConfigChanged(logContext, topicName).onComplete(ar -> {
            assertSucceeded(context, ar);
            context.verify(() -> assertThat(mockKafka.getTopicState(topicName).getConfig().get("cleanup.policy"), is("baz")));
            mockTopicStore.read(topicName).onComplete(ar2 -> {
                assertSucceeded(context, ar2);
                context.verify(() -> assertThat(ar2.result().getConfig().get("cleanup.policy"), is("baz")));
                async.flag();
            });
            mockK8s.getFromName(resourceName).onComplete(ar2 -> {
                assertSucceeded(context, ar2);
                context.verify(() -> assertThat(TopicSerialization.fromTopicResource(ar2.result()).getConfig().get("cleanup.policy"), is("baz")));
                async.flag();
            });
            async.flag();
        });
    }

    // TODO error getting full topic metadata, and then reconciliation
    // TODO error creating KafkaTopic (exists), and then reconciliation

    /**
     * Test reconciliation when a resource has been created while the operator wasn't running
     */
    @Test
    public void testReconcile_withResource_noKafka_noPrivate(VertxTestContext context) throws InterruptedException {

        Topic kubeTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar")).build();
        Topic kafkaTopic = null;
        Topic privateTopic = null;

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

        CountDownLatch latch = new CountDownLatch(1);
        Checkpoint async = context.checkpoint(1);
        topicOperator.reconcile(null, logContext, null, kubeTopic, kafkaTopic, privateTopic).onComplete(reconcileResult -> {
            assertSucceeded(context, reconcileResult);
            mockKafka.assertExists(context, kubeTopic.getTopicName());
            mockTopicStore.assertExists(context, kubeTopic.getTopicName());
            mockK8s.assertNoEvents(context);
            mockTopicStore.read(topicName).onComplete(readResult -> {
                assertSucceeded(context, readResult);
                context.verify(() -> assertThat(readResult.result(), is(kubeTopic)));
                async.flag();
                latch.countDown();
            });
        });
        latch.await(30, TimeUnit.SECONDS);
        context.completeNow();
    }

    /**
     * Test reconciliation when a topic has been deleted while the operator
     * wasn't running
     */
    @Test
    public void testReconcile_withResource_noKafka_withPrivate(VertxTestContext context) {

        Topic kubeTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar")).build();
        Topic kafkaTopic = null;
        Topic privateTopic = kubeTopic;

        Checkpoint async0 = context.checkpoint(2);
        KafkaTopic topicResource = TopicSerialization.toTopicResource(kubeTopic, labels);
        LogContext logContext = LogContext.kubeWatch(Watcher.Action.DELETED, topicResource);
        mockK8s.setCreateResponse(resourceName, null)
                .createResource(topicResource).onComplete(ar -> async0.flag());
        mockK8s.setDeleteResponse(resourceName, null);
        mockTopicStore.setCreateTopicResponse(topicName, null)
                .create(privateTopic).onComplete(ar -> async0.flag());
        mockTopicStore.setDeleteTopicResponse(topicName, null);

        Checkpoint async = context.checkpoint();

        topicOperator.reconcile(reconciliation(), logContext, null, kubeTopic, kafkaTopic, privateTopic).onComplete(reconcileResult -> {
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
    public void testReconcile_noResource_withKafka_noPrivate(VertxTestContext context) throws InterruptedException {

        Topic kubeTopic = null;
        Topic kafkaTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar"), metadata).build();
        Topic privateTopic = null;

        CountDownLatch async0 = new CountDownLatch(1);
        mockTopicStore.setCreateTopicResponse(topicName, null);
        mockK8s.setCreateResponse(topicName.asKubeName(), null);
        mockKafka.setCreateTopicResponse(topicName -> Future.succeededFuture());
        mockKafka.createTopic(kafkaTopic).onComplete(ar -> async0.countDown());
        async0.await();
        LogContext logContext = LogContext.periodic(topicName.toString());
        CountDownLatch async = new CountDownLatch(2);
        topicOperator.reconcile(reconciliation(), logContext, null, kubeTopic, kafkaTopic, privateTopic).onComplete(reconcileResult -> {
            assertSucceeded(context, reconcileResult);
            mockTopicStore.assertExists(context, topicName);
            mockK8s.assertExists(context, topicName.asKubeName());
            mockKafka.assertExists(context, topicName);
            mockK8s.assertNoEvents(context);
            mockTopicStore.read(topicName).onComplete(readResult -> {
                assertSucceeded(context, readResult);
                context.verify(() -> assertThat(readResult.result(), is(kafkaTopic)));
                async.countDown();
            });
            mockK8s.getFromName(topicName.asKubeName()).onComplete(readResult -> {
                assertSucceeded(context, readResult);
                context.verify(() -> assertThat(TopicSerialization.fromTopicResource(readResult.result()), is(kafkaTopic)));
                async.countDown();
            });
            try {
                async.await(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            context.verify(() -> assertThat(mockKafka.getTopicState(topicName), is(kafkaTopic)));
            context.verify(() -> {
                MeterRegistry registry = metrics.meterRegistry();

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations").tag("kind", "KafkaTopic").counter().count(), is(1.0));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "KafkaTopic").counter().count(), is(0.0));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "KafkaTopic").counter().count(), is(0.0));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.locked").tag("kind", "KafkaTopic").counter().count(), is(0.0));

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().count(), is(0L));
                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().totalTime(TimeUnit.MILLISECONDS), is(0.0));
            });
            context.completeNow();
        });
    }

    TopicOperator.Reconciliation reconciliation() {
        return topicOperator.new Reconciliation("test") {
            @Override
            public Future<Void> execute() {
                return null;
            }
        };
    }

    /**
     * Test reconciliation when a resource has been deleted while the operator
     * wasn't running
     */
    @Test
    public void testReconcile_noResource_withKafka_withPrivate(VertxTestContext context) throws InterruptedException {
        Topic kubeTopic = null;
        Topic kafkaTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar")).build();
        Topic privateTopic = kafkaTopic;

        CountDownLatch async0 = new CountDownLatch(2);
        mockKafka.createTopic(kafkaTopic).onComplete(ar -> async0.countDown());
        mockKafka.setDeleteTopicResponse(topicName, null);
        mockTopicStore.setCreateTopicResponse(topicName, null);
        mockTopicStore.create(kafkaTopic).onComplete(ar -> async0.countDown());
        mockTopicStore.setDeleteTopicResponse(topicName, null);
        async0.await();
        LogContext logContext = LogContext.periodic(topicName.toString());
        Checkpoint async = context.checkpoint();
        topicOperator.reconcile(reconciliation(), logContext, null, kubeTopic, kafkaTopic, privateTopic).onComplete(reconcileResult -> {
            assertSucceeded(context, reconcileResult);
            mockTopicStore.assertNotExists(context, topicName);
            mockK8s.assertNotExists(context, topicName.asKubeName());
            mockKafka.assertNotExists(context, topicName);
            mockK8s.assertNoEvents(context);
            async.flag();
        });
    }

    /**
     * Test reconciliation when a resource has been added both in kafka and in k8s while the operator was down, and both
     * topics are identical.
     */
    @Test
    public void testReconcile_withResource_withKafka_noPrivate_matching(VertxTestContext context) throws InterruptedException {
        Topic kubeTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar"), metadata).build();
        Topic kafkaTopic = kubeTopic;
        Topic privateTopic = null;

        CountDownLatch async0 = new CountDownLatch(2);
        mockKafka.setCreateTopicResponse(topicName -> Future.succeededFuture());
        mockKafka.createTopic(kafkaTopic).onComplete(ar -> async0.countDown());
        mockK8s.setCreateResponse(topicName.asKubeName(), null);
        KafkaTopic topicResource = TopicSerialization.toTopicResource(kubeTopic, labels);
        LogContext logContext = LogContext.periodic(topicName.toString());
        mockK8s.createResource(topicResource).onComplete(ar -> async0.countDown());
        mockTopicStore.setCreateTopicResponse(topicName, null);
        async0.await();

        Checkpoint async = context.checkpoint();
        topicOperator.reconcile(reconciliation(), logContext, null, kubeTopic, kafkaTopic, privateTopic).onComplete(reconcileResult -> {
            assertSucceeded(context, reconcileResult);
            mockTopicStore.assertExists(context, topicName);
            mockK8s.assertExists(context, topicName.asKubeName());
            mockK8s.assertNoEvents(context);
            mockKafka.assertExists(context, topicName);
            mockTopicStore.read(topicName).onComplete(readResult -> {
                assertSucceeded(context, readResult);
                context.verify(() -> assertThat(readResult.result(), is(kubeTopic)));
                async.flag();
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
    public void testReconcile_withResource_withKafka_noPrivate_configsReconcilable(VertxTestContext context) throws InterruptedException {
        Topic kubeTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar"), metadata).build();
        Topic kafkaTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("unclean.leader.election.enable", "true"), metadata).build();
        Topic privateTopic = null;
        Topic mergedTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("unclean.leader.election.enable", "true", "cleanup.policy", "bar"), metadata).build();

        CountDownLatch async0 = new CountDownLatch(2);
        mockKafka.setCreateTopicResponse(topicName -> Future.succeededFuture());
        mockKafka.createTopic(kafkaTopic).onComplete(ar -> async0.countDown());
        mockKafka.setUpdateTopicResponse(topicName -> Future.succeededFuture());

        KafkaTopic topic = TopicSerialization.toTopicResource(kubeTopic, labels);
        LogContext logContext = LogContext.periodic(topicName.toString());
        mockK8s.setCreateResponse(topicName.asKubeName(), null);
        mockK8s.createResource(topic).onComplete(ar -> async0.countDown());
        mockK8s.setModifyResponse(topicName.asKubeName(), null);
        mockTopicStore.setCreateTopicResponse(topicName, null);
        async0.await();

        CountDownLatch async = new CountDownLatch(2);
        topicOperator.reconcile(reconciliation(), logContext, topic, kubeTopic, kafkaTopic, privateTopic).onComplete(reconcileResult -> {
            assertSucceeded(context, reconcileResult);
            mockTopicStore.assertExists(context, topicName);
            mockK8s.assertExists(context, topicName.asKubeName());
            mockKafka.assertExists(context, topicName);
            mockTopicStore.read(topicName).onComplete(readResult -> {
                assertSucceeded(context, readResult);
                context.verify(() -> assertThat(readResult.result(), is(mergedTopic)));
                async.countDown();
            });
            mockK8s.getFromName(topicName.asKubeName()).onComplete(readResult -> {
                assertSucceeded(context, readResult);
                context.verify(() -> assertThat(TopicSerialization.fromTopicResource(readResult.result()), is(mergedTopic)));
                async.countDown();
            });
            try {
                async.await(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            context.verify(() -> assertThat(mockKafka.getTopicState(topicName), is(mergedTopic)));
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
     * Test reconciliation when a resource has been added both in kafka and in k8s while the operator was down, and
     * the topics are irreconcilably different: Kafka wins
     */
    @Test
    public void testReconcile_withResource_withKafka_noPrivate_irreconcilable(VertxTestContext context) throws InterruptedException {
        Topic kubeTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar"), metadata).build();
        Topic kafkaTopic = new Topic.Builder(topicName.toString(), 12, (short) 2, map("cleanup.policy", "baz"), metadata).build();
        Topic privateTopic = null;

        CountDownLatch async0 = new CountDownLatch(2);
        mockKafka.setCreateTopicResponse(topicName -> Future.succeededFuture());
        mockKafka.createTopic(kafkaTopic).onComplete(ar -> async0.countDown());

        KafkaTopic topic = TopicSerialization.toTopicResource(kubeTopic, labels);
        LogContext logContext = LogContext.periodic(topicName.toString());
        mockK8s.setCreateResponse(topicName.asKubeName(), null);
        mockK8s.createResource(topic).onComplete(ar -> async0.countDown());
        mockK8s.setModifyResponse(topicName.asKubeName(), null);
        mockTopicStore.setCreateTopicResponse(topicName, null);
        async0.await();

        CountDownLatch async = new CountDownLatch(2);
        topicOperator.reconcile(reconciliation(), logContext, topic, kubeTopic, kafkaTopic, privateTopic).onComplete(reconcileResult -> {
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
                async.countDown();
            });
            mockK8s.getFromName(topicName.asKubeName()).onComplete(readResult -> {
                assertSucceeded(context, readResult);
                context.verify(() -> assertThat(TopicSerialization.fromTopicResource(readResult.result()), is(kafkaTopic)));
                async.countDown();
            });
            try {
                async.await(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            context.verify(() -> assertThat(mockKafka.getTopicState(topicName), is(kafkaTopic)));
            context.completeNow();
        });
    }

    /**
     * Test reconciliation when a resource has been changed both in kafka and in k8s while the operator was down, and
     * a 3 way merge is needed.
     */
    @Test
    public void testReconcile_withResource_withKafka_withPrivate_3WayMerge(VertxTestContext context) throws InterruptedException {
        Topic kubeTopic = new Topic.Builder(topicName, resourceName, 10, (short) 2, map("cleanup.policy", "bar"), metadata).build();
        Topic kafkaTopic = new Topic.Builder(topicName, resourceName, 12, (short) 2, map("cleanup.policy", "baz"), metadata).build();
        Topic privateTopic = new Topic.Builder(topicName, resourceName, 10, (short) 2, map("cleanup.policy", "baz"), metadata).build();
        Topic resultTopic = new Topic.Builder(topicName, resourceName, 12, (short) 2, map("cleanup.policy", "bar"), metadata).build();

        CountDownLatch async0 = new CountDownLatch(3);
        mockKafka.setCreateTopicResponse(topicName -> Future.succeededFuture());
        mockKafka.createTopic(kafkaTopic).onComplete(ar -> async0.countDown());
        mockKafka.setUpdateTopicResponse(topicName -> Future.succeededFuture());

        KafkaTopic resource = TopicSerialization.toTopicResource(kubeTopic, labels);
        LogContext logContext = LogContext.periodic(topicName.toString());
        mockK8s.setCreateResponse(topicName.asKubeName(), null);
        mockK8s.createResource(resource).onComplete(ar -> async0.countDown());
        mockK8s.setModifyResponse(topicName.asKubeName(), null);
        mockTopicStore.setCreateTopicResponse(topicName, null);
        mockTopicStore.create(privateTopic).onComplete(ar -> async0.countDown());
        async0.await();

        CountDownLatch async = new CountDownLatch(3);
        topicOperator.reconcile(reconciliation(), logContext, resource, kubeTopic, kafkaTopic, privateTopic).onComplete(reconcileResult -> {
            assertSucceeded(context, reconcileResult);
            mockK8s.assertNoEvents(context);
            mockTopicStore.read(topicName).onComplete(readResult -> {
                assertSucceeded(context, readResult);
                context.verify(() -> assertThat(readResult.result(), is(resultTopic)));
                async.countDown();
            });
            mockK8s.getFromName(topicName.asKubeName()).onComplete(readResult -> {
                assertSucceeded(context, readResult);
                context.verify(() -> assertThat(TopicSerialization.fromTopicResource(readResult.result()), is(resultTopic)));
                async.countDown();
            });
            context.verify(() -> assertThat(mockKafka.getTopicState(topicName), is(resultTopic)));
            async.countDown();
            try {
                async.await(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            context.completeNow();
        });
    }

    // TODO 3way reconcilation where kafka and kube agree
    // TODO 3way reconcilation where all three agree
    // TODO 3way reconcilation with conflict
    // TODO reconciliation where only private state exists => delete the private state

    // TODO tests for the other reconciliation cases
    // + non-matching predicate
    // + error cases

    private void resourceRemoved(VertxTestContext context, Exception deleteTopicException, Exception storeException) {
        Topic kubeTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar")).build();
        Topic kafkaTopic = kubeTopic;
        Topic privateTopic = kubeTopic;

        mockKafka.setCreateTopicResponse(topicName.toString(), null)
                .createTopic(kafkaTopic);
        mockKafka.setTopicMetadataResponse(topicName, Utils.getTopicMetadata(kubeTopic), null);
        mockKafka.setDeleteTopicResponse(topicName, deleteTopicException);

        mockTopicStore.setCreateTopicResponse(topicName, null)
                .create(privateTopic);
        mockTopicStore.setDeleteTopicResponse(topicName, storeException);

        KafkaTopic resource = TopicSerialization.toTopicResource(kubeTopic, labels);
        LogContext logContext = LogContext.kubeWatch(Watcher.Action.DELETED, resource);

        Checkpoint async = context.checkpoint();
        topicOperator.onResourceEvent(logContext, resource, DELETED).onComplete(ar -> {
            if (deleteTopicException != null || storeException != null) {

                if (deleteTopicException != null && deleteTopicException instanceof TopicDeletionDisabledException) {
                    // For the specific topic deletion disabled exception the exception will be caught and the resource
                    // event will be processed successfully
                    assertSucceeded(context, ar);
                } else {
                    // For all other exceptions the resource event will fail.
                    assertFailed(context, ar);
                }

                if (deleteTopicException != null) {
                    // If there was a broker deletion exception the broker topic should still exist
                    mockKafka.assertExists(context, kafkaTopic.getTopicName());
                } else {
                    mockKafka.assertNotExists(context, kafkaTopic.getTopicName());
                }

                if (deleteTopicException != null && deleteTopicException instanceof TopicDeletionDisabledException) {
                    //If there was a topic deletion disabled exception then the Store topic would still be deleted.
                    mockTopicStore.assertNotExists(context, kafkaTopic.getTopicName());
                } else {
                    mockTopicStore.assertExists(context, kafkaTopic.getTopicName());
                }

            } else {
                assertSucceeded(context, ar);
                mockKafka.assertNotExists(context, kafkaTopic.getTopicName());
                mockTopicStore.assertNotExists(context, kafkaTopic.getTopicName());
            }
            async.flag();
        });
    }

    @Test
    public void testOnKafkaTopicChanged(VertxTestContext context) {
        Topic kubeTopic = new Topic.Builder(topicName, resourceName, 10, (short) 2, map("cleanup.policy", "baz"), null).build();
        Topic kafkaTopic = new Topic.Builder(topicName, resourceName, 10, (short) 2, map("cleanup.policy", "bar"), null).build();
        Topic privateTopic = kafkaTopic;
        KafkaTopic resource = TopicSerialization.toTopicResource(kubeTopic, labels);
        LogContext logContext = LogContext.zkWatch("///", topicName.toString());

        mockKafka.setCreateTopicResponse(topicName.toString(), null)
                .createTopic(kafkaTopic);
        mockKafka.setTopicMetadataResponse(topicName, Utils.getTopicMetadata(kafkaTopic), null);
        mockKafka.setUpdateTopicResponse(topicName -> Future.succeededFuture());

        mockTopicStore.setCreateTopicResponse(topicName, null)
                .create(privateTopic);
        mockTopicStore.setUpdateTopicResponse(topicName, null);

        mockK8s.setCreateResponse(resourceName, null);
        mockK8s.createResource(resource).onComplete(ar -> {
            assertSucceeded(context, ar);
        });
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
            async.countDown();
            try {
                async.await(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            context.completeNow();
        });
    }

    @Test
    public void testOnKafkaTopicRemoved(VertxTestContext context) {
        Exception deleteTopicException = null;
        Exception storeException = null;
        resourceRemoved(context, deleteTopicException, storeException);
    }

    @Test
    public void testOnKafkaTopicRemoved_UnknownTopicOrPartitionException(VertxTestContext context) {
        Exception deleteTopicException = new UnknownTopicOrPartitionException();
        Exception storeException = null;
        resourceRemoved(context, deleteTopicException, storeException);
    }

    @Test
    public void testOnKafkaTopicRemoved_NoSuchEntityExistsException(VertxTestContext context) {
        Exception deleteTopicException = null;
        Exception storeException = new TopicStore.NoSuchEntityExistsException();
        resourceRemoved(context, deleteTopicException, storeException);
    }

    @Test
    public void testOnKafkaTopicRemoved_TopicDeletionDisabledException(VertxTestContext context) {
        // Deals with the situation where the delete.topic.enable=false config is set in the broker
        Exception deleteTopicException = new TopicDeletionDisabledException("Topic deletion disable");
        Exception storeException = null;
        resourceRemoved(context, deleteTopicException, storeException);
    }

    private void topicDeleted(VertxTestContext context, Exception storeException, Exception k8sException) {
        Topic kubeTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar")).withMapName(resourceName).build();
        Topic kafkaTopic = kubeTopic;
        Topic privateTopic = kubeTopic;

        mockK8s.setCreateResponse(resourceName, null)
                .createResource(TopicSerialization.toTopicResource(kubeTopic, labels));
        mockK8s.setDeleteResponse(resourceName, k8sException);

        mockTopicStore.setCreateTopicResponse(topicName, null)
                .create(privateTopic);
        mockTopicStore.setDeleteTopicResponse(topicName, storeException);
        LogContext logContext = LogContext.zkWatch("///", topicName.toString());
        Checkpoint async = context.checkpoint();
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
            async.flag();
        });
    }

    @Test
    public void testOnTopicDeleted(VertxTestContext context) {
        Exception storeException = null;
        Exception k8sException = null;
        topicDeleted(context, storeException, k8sException);
    }

    @Test
    public void testOnTopicDeleted_NoSuchEntityExistsException(VertxTestContext context) {
        Exception k8sException = null;
        Exception storeException = new TopicStore.NoSuchEntityExistsException();
        topicDeleted(context, storeException, k8sException);
    }

    @Test
    public void testOnTopicDeleted_KubernetesClientException(VertxTestContext context) {
        Exception k8sException = new KubernetesClientException("Test exception");
        Exception storeException = null;
        topicDeleted(context, storeException, k8sException);
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

                assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations").tag("kind", "KafkaTopic").counter().count(), is(2.0));
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
    public void testReconcileMetrics(VertxTestContext context) throws InterruptedException {
        mockKafka.setTopicsListResponse(Future.succeededFuture(emptySet()));
        mockKafka.setUpdateTopicResponse(topicName -> Future.succeededFuture());
        Future<?> reconcileFuture = topicOperator.reconcileAllTopics("periodic");
        resourceAdded(context, null, null);

        Checkpoint async = context.checkpoint();
        reconcileFuture.onComplete(context.succeeding(e -> context.verify(() -> {
            MeterRegistry registry = metrics.meterRegistry();

            assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations").tag("kind", "KafkaTopic").counter().count(), is(1.0));
            assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "KafkaTopic").counter().count(), is(0.0));
            assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "KafkaTopic").counter().count(), is(0.0));

            assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().count(), is(1L));
            assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

            async.flag();
        })));
    }

    @Test
    public void testReconcileMetricsDeletedTopic(VertxTestContext context) throws InterruptedException {
        mockKafka.setTopicsListResponse(Future.succeededFuture(emptySet()));
        mockKafka.setUpdateTopicResponse(topicName -> Future.succeededFuture());
        Future<?> reconcileFuture = topicOperator.reconcileAllTopics("periodic");
        resourceAdded(context, null, null);
        resourceRemoved(context, null, null);

        Checkpoint async = context.checkpoint();
        reconcileFuture.onComplete(context.succeeding(e -> context.verify(() -> {
            MeterRegistry registry = metrics.meterRegistry();

            assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations").tag("kind", "KafkaTopic").counter().count(), is(0.0));
            assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "KafkaTopic").counter().count(), is(0.0));
            assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.failed").tag("kind", "KafkaTopic").counter().count(), is(0.0));

            assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().count(), is(1L));
            assertThat(registry.get(TopicOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "KafkaTopic").timer().totalTime(TimeUnit.MILLISECONDS), greaterThan(0.0));

            async.flag();
        })));
    }

    /**
     * Created new MetricsProvider and makes sure it doesn't contain any metrics from previous tests.
     *
     * @return  Clean MetricsProvider
     */
    public MetricsProvider createCleanMetricsProvider() {
        MetricsProvider metrics = new MicrometerMetricsProvider();
        MeterRegistry registry = metrics.meterRegistry();

        registry.forEachMeter(meter -> {
            registry.remove(meter);
        });

        return metrics;
    }

    // TODO tests for nasty races (e.g. create on both ends, update on one end and delete on the other)
    // I think in these cases we should seek to detect the concurrent modification
    // and perform a full reconciliation, possibly after a backoff time
    // (to cover the case where topic config and other aspects get changed via multiple calls)
    // TODO test for zookeeper session timeout
    // TODO test for Kubernetes connection death
}
