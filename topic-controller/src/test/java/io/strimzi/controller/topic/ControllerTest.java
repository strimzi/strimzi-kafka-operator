/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.topic;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;

@RunWith(VertxUnitRunner.class)
public class ControllerTest {

    private final LabelPredicate cmPredicate = LabelPredicate.fromString("kind=topic,app=strimzi");

    private final TopicName topicName = new TopicName("my-topic");
    private final MapName mapName = topicName.asMapName();
    private Vertx vertx = Vertx.vertx();
    private MockKafka mockKafka = new MockKafka();
    private MockTopicStore mockTopicStore = new MockTopicStore();
    private MockK8s mockK8s = new MockK8s();
    private Controller controller;

    @Before
    public void setup() {
        mockKafka = new MockKafka();
        mockTopicStore = new MockTopicStore();
        mockK8s = new MockK8s();
        controller = new Controller(vertx, mockKafka, mockK8s, mockTopicStore, cmPredicate, "default-namespace");
    }

    @After
    public void teardown() {
        vertx.close();
        mockKafka = null;
        mockTopicStore = null;
        mockK8s = null;
        controller = null;
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

    private TopicMetadata getTopicMetadata() {
        Node node0 = new Node(0, "host0", 1234);
        Node node1 = new Node(1, "host1", 1234);
        Node node2 = new Node(2, "host2", 1234);
        List<Node> nodes02 = asList(node0, node1, node2);
        TopicDescription desc = new TopicDescription(topicName.toString(), false, asList(
                new TopicPartitionInfo(0, node0, nodes02, nodes02),
                new TopicPartitionInfo(1, node0, nodes02, nodes02)
        ));
        Config config = new Config(Collections.emptyList());
        return new TopicMetadata(desc, config);
    }

    private TopicMetadata getTopicMetadata(Topic kubeTopic) {
        List<Node> nodes = new ArrayList<>();
        for (int nodeId = 0; nodeId < kubeTopic.getNumReplicas(); nodeId++) {
            nodes.add(new Node(nodeId, "localhost", 9092 + nodeId));
        }
        List<TopicPartitionInfo> partitions = new ArrayList<>();
        for (int partitionId = 0; partitionId < kubeTopic.getNumPartitions(); partitionId++) {
            partitions.add(new TopicPartitionInfo(partitionId, nodes.get(0), nodes, nodes));
        }
        List<ConfigEntry> configs = new ArrayList<>();
        for (Map.Entry<String, String> entry: kubeTopic.getConfig().entrySet()) {
            configs.add(new ConfigEntry(entry.getKey(), entry.getValue()));
        }

        return new TopicMetadata(new TopicDescription(kubeTopic.getTopicName().toString(), false,
                partitions), new Config(configs));
    }

    /** Test what happens when a non-topic config map gets created in kubernetes */
    @Test
    public void testOnConfigMapAdded_ignorable(TestContext context) {
        ConfigMap cm = new ConfigMapBuilder().withNewMetadata().withName("non-topic").endMetadata().build();

        Async async = context.async();
        controller.onConfigMapAdded(cm, ar -> {
            assertSucceeded(context, ar);
            mockKafka.assertEmpty(context);
            mockTopicStore.assertEmpty(context);
            async.complete();

        });
    }

    /** Test what happens when a non-topic config map gets created in kubernetes */
    @Test
    public void testOnConfigMapAdded_invalidCm(TestContext context) {
        Map<String, String> data = map(TopicSerialization.CM_KEY_REPLICAS, "1",
                TopicSerialization.CM_KEY_PARTITIONS, "1",
                TopicSerialization.CM_KEY_CONFIG, "{null:null}");
        ConfigMap cm = new ConfigMapBuilder().withNewMetadata().withName("invalid").withLabels(cmPredicate.labels()).endMetadata().
                withData(data).build();

        Async async = context.async();
        controller.onConfigMapAdded(cm, ar -> {
            assertFailed(context, ar);
            context.assertTrue(ar.cause() instanceof InvalidConfigMapException);
            context.assertEquals("ConfigMap's 'data' section has invalid key 'config': Unexpected character ('n' (code 110)): was expecting double-quote to start field name\n" +
                    " at [Source: 'config' key of 'data' section of ConfigMap 'invalid' in namespace 'null'; line: 1, column: 3]", ar.cause().getMessage());
            mockKafka.assertEmpty(context);
            mockTopicStore.assertEmpty(context);
            async.complete();

        });
    }

    /**
     * Trigger {@link Controller#onConfigMapAdded(ConfigMap, Handler)}
     * and have the Kafka and TopicStore respond with the given exceptions.
     */
    private Controller configMapAdded(TestContext context, Exception createException, Exception storeException) {
        mockKafka.setCreateTopicResponse(topicName.toString(), createException);
        mockKafka.setTopicMetadataResponse(topicName, null, null);
        mockTopicStore.setCreateTopicResponse(topicName, storeException);

        ConfigMap cm = new ConfigMapBuilder().withNewMetadata()
                .withName(topicName.toString())
                .withLabels(cmPredicate.labels()).endMetadata()
                .withData(map(TopicSerialization.CM_KEY_PARTITIONS, "10",
                        TopicSerialization.CM_KEY_REPLICAS, "2")).build();

        Async async = context.async();

        controller.onConfigMapAdded(cm, ar -> {
            if (createException != null
                    || storeException != null) {
                assertFailed(context, ar);
                Class<? extends Exception> expectedExceptionType;
                if (createException != null) {
                    expectedExceptionType = createException.getClass();
                } else {
                    expectedExceptionType = storeException.getClass();
                }
                context.assertEquals(expectedExceptionType, ar.cause().getClass(), ar.cause().getMessage());
                TopicName topicName = TopicSerialization.fromConfigMap(cm).getTopicName();
                if (createException != null) {
                    mockKafka.assertNotExists(context, topicName);
                } else {
                    mockKafka.assertExists(context, topicName);
                }
                mockTopicStore.assertNotExists(context, topicName);
                //TODO mockK8s.assertContainsEvent(context, e -> "Error".equals(e.getKind()));
            } else {
                assertSucceeded(context, ar);
                Topic expectedTopic = TopicSerialization.fromConfigMap(cm);
                mockKafka.assertContains(context, expectedTopic);
                mockTopicStore.assertContains(context, expectedTopic);
                mockK8s.assertNoEvents(context);
            }
            async.complete();
        });

        return controller;
    }

    /**
     * 1. controller is notified that a ConfigMap is created
     * 2. controller successfully creates topic in kafka
     * 3. controller successfully creates in topic store
     */
    @Test
    public void testOnConfigMapAdded(TestContext context) {
        configMapAdded(context, null, null);
    }

    /**
     * 1. controller is notified that a ConfigMap is created
     * 2. error when creating topic in kafka
     */
    @Test
    public void testOnConfigMapAdded_TopicExistsException(TestContext context) {
        Exception createException = new TopicExistsException("");
        configMapAdded(context, createException, null);
        // TODO check a k8s event got created
        // TODO what happens when we subsequently reconcile?
    }

    /**
     * 1. controller is notified that a ConfigMap is created
     * 2. error when creating topic in kafka
     */
    @Test
    public void testOnConfigMapAdded_ClusterAuthorizationException(TestContext context) {
        Exception createException = new ClusterAuthorizationException("");
        Controller op = configMapAdded(context, createException, null);
        // TODO check a k8s event got created
        // TODO what happens when we subsequently reconcile?
    }

    /**
     * 1. controller is notified that a ConfigMap is created
     * 2. controller successfully creates topic in kafka
     * 3. error when creating in topic store
     */
    @Test
    public void testOnConfigMapAdded_EntityExistsException(TestContext context) {
        configMapAdded(context,
                null,
                new TopicStore.EntityExistsException());
        // TODO what happens when we subsequently reconcile?
    }

    // TODO ^^ but a disconnected/loss of session error

    /**
     * 1. controller is notified that a topic is created
     * 2. controller successfully queries kafka to get topic metadata
     * 3. controller successfully creates config map
     * 4. controller successfully creates in topic store
     */
    @Test
    public void testOnTopicCreated(TestContext context) {
        TopicMetadata topicMetadata = getTopicMetadata();

        mockTopicStore.setCreateTopicResponse(topicName, null);
        mockKafka.setTopicMetadataResponse(topicName, topicMetadata, null);
        mockK8s.setCreateResponse(mapName, null);

        Async async = context.async();
        controller.onTopicCreated(topicName, ar -> {
            assertSucceeded(context, ar);
            mockK8s.assertExists(context, mapName);
            mockTopicStore.assertContains(context, TopicSerialization.fromTopicMetadata(topicMetadata));
            async.complete();
        });
    }

    /**
     * 1. controller is notified that a topic is created
     * 2. controller initially failed querying kafka to get topic metadata
     * 3. controller is subsequently successful in querying kafka to get topic metadata
     * 4. controller successfully creates config map
     * 5. controller successfully creates in topic store
     */
    @Test
    public void testOnTopicCreated_retry(TestContext context) {
        TopicMetadata topicMetadata = getTopicMetadata();

        mockTopicStore.setCreateTopicResponse(topicName, null);
        AtomicInteger counter = new AtomicInteger();
        mockKafka.setTopicMetadataResponse(t -> {
            int count = counter.getAndIncrement();
            if (count == 3) {
                return Future.succeededFuture(topicMetadata);
            } else if (count < 3) {
                return Future.succeededFuture(null);
            }
            context.fail("This should never happen");
            return Future.failedFuture("This should never happen");
        });
        mockK8s.setCreateResponse(mapName, null);

        Async async = context.async();
        controller.onTopicCreated(topicName, ar -> {
            assertSucceeded(context, ar);
            context.assertEquals(4, counter.get());
            mockK8s.assertExists(context, mapName);
            mockTopicStore.assertContains(context, TopicSerialization.fromTopicMetadata(topicMetadata));
            async.complete();
        });
    }

    private <T> void assertSucceeded(TestContext context, AsyncResult<T> ar) {
        context.assertTrue(ar.succeeded(), ar.cause() != null ? ar.cause().toString() : "");
    }

    private <T> void assertFailed(TestContext context, AsyncResult<T> ar) {
        context.assertFalse(ar.succeeded(), ar.failed() ? "" : ar.result().toString());
    }


    /**
     * 1. controller is notified that a topic is created
     * 2. controller times out getting metadata
     */
    @Test
    public void testOnTopicCreated_retryTimeout(TestContext context) {
        TopicMetadata topicMetadata = getTopicMetadata();

        mockKafka.setTopicMetadataResponse(topicName, null, null);

        Async async = context.async();
        controller.onTopicCreated(topicName, ar -> {
            assertFailed(context, ar);
            context.assertEquals(ar.cause().getClass(), MaxAttemptsExceededException.class);
            mockK8s.assertNotExists(context, mapName);
            mockTopicStore.assertNotExists(context, topicName);
            async.complete();
        });
    }

    /**
     * 0. ZK notifies of a change in topic config
     * 1. controller gets updated topic metadata
     * 2. controller updates k8s and topic store.
     */
    @Test
    public void testOnTopicChanged(TestContext context) {
        Topic kubeTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar")).build();
        Topic kafkaTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "baz")).build();
        Topic privateTopic = kubeTopic;
        ConfigMap cm = TopicSerialization.toConfigMap(kubeTopic, cmPredicate);

        mockKafka.setCreateTopicResponse(topicName.toString(), null)
                .createTopic(kafkaTopic, ar -> { });
        mockKafka.setTopicMetadataResponse(topicName, getTopicMetadata(kafkaTopic), null);
        //mockKafka.setUpdateTopicResponse(topicName -> Future.succeededFuture());

        mockTopicStore.setCreateTopicResponse(topicName, null)
                .create(privateTopic, ar -> { });
        mockTopicStore.setUpdateTopicResponse(topicName, null);

        mockK8s.setCreateResponse(mapName, null)
                .createConfigMap(cm, ar -> { });
        mockK8s.setModifyResponse(mapName, null);

        Async async = context.async(3);
        controller.onTopicConfigChanged(topicName, ar -> {
            assertSucceeded(context, ar);
            context.assertEquals("baz", mockKafka.getTopicState(topicName).getConfig().get("cleanup.policy"));
            mockTopicStore.read(topicName, ar2 -> {
                assertSucceeded(context, ar2);
                context.assertEquals("baz", ar2.result().getConfig().get("cleanup.policy"));
                async.countDown();
            });
            mockK8s.getFromName(mapName, ar2 -> {
                assertSucceeded(context, ar2);
                context.assertEquals("baz", TopicSerialization.fromConfigMap(ar2.result()).getConfig().get("cleanup.policy"));
                async.countDown();
            });
            async.countDown();
        });
    }

    // TODO error getting full topic metadata, and then reconciliation
    // TODO error creating config map (exists), and then reconciliation

    /**
     * Test reconciliation when a configmap has been created while the controller wasn't running
     */
    @Test
    public void testReconcile_withCm_noKafka_noPrivate(TestContext context) {

        Topic kubeTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar")).build();
        Topic kafkaTopic = null;
        Topic privateTopic = null;

        Async async0 = context.async();
        mockKafka.setCreateTopicResponse(topicName.toString(), null);

        mockTopicStore.setCreateTopicResponse(topicName, null);
        mockK8s.setCreateResponse(topicName.asMapName(), null);
        mockK8s.createConfigMap(TopicSerialization.toConfigMap(kubeTopic, cmPredicate), ar -> async0.countDown());

        Async async = context.async(1);
        controller.reconcile(null, kubeTopic, kafkaTopic, privateTopic, reconcileResult -> {
            assertSucceeded(context, reconcileResult);
            mockKafka.assertExists(context, kubeTopic.getTopicName());
            mockTopicStore.assertExists(context, kubeTopic.getTopicName());
            mockK8s.assertNoEvents(context);
            mockTopicStore.read(topicName, readResult -> {
                assertSucceeded(context, readResult);
                context.assertEquals(kubeTopic, readResult.result());
                async.countDown();
            });
        });
    }

    /**
     * Test reconciliation when a topic has been deleted while the controller
     * wasn't running
     */
    @Test
    public void testReconcile_withCm_noKafka_withPrivate(TestContext context) {

        Topic kubeTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar")).build();
        Topic kafkaTopic = null;
        Topic privateTopic = kubeTopic;

        Async async0 = context.async(2);
        mockK8s.setCreateResponse(mapName, null)
                .createConfigMap(TopicSerialization.toConfigMap(kubeTopic, cmPredicate), ar -> async0.countDown());
        mockK8s.setDeleteResponse(mapName, null);
        mockTopicStore.setCreateTopicResponse(topicName, null)
                .create(privateTopic, ar -> async0.countDown());
        mockTopicStore.setDeleteTopicResponse(topicName, null);

        Async async = context.async();

        controller.reconcile(null, kubeTopic, kafkaTopic, privateTopic, reconcileResult -> {
            assertSucceeded(context, reconcileResult);
            mockKafka.assertNotExists(context, kubeTopic.getTopicName());
            mockTopicStore.assertNotExists(context, kubeTopic.getTopicName());
            mockK8s.assertNotExists(context, kubeTopic.getMapName());
            mockK8s.assertNoEvents(context);
            async.complete();
        });
    }

    /**
     * Test reconciliation when a topic has been created while the controller wasn't running
     */
    @Test
    public void testReconcile_noCm_withKafka_noPrivate(TestContext context) {

        Topic kubeTopic = null;
        Topic kafkaTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar")).build();
        Topic privateTopic = null;

        Async async0 = context.async();
        mockTopicStore.setCreateTopicResponse(topicName, null);
        mockK8s.setCreateResponse(topicName.asMapName(), null);
        mockKafka.setCreateTopicResponse(topicName -> Future.succeededFuture());
        mockKafka.createTopic(kafkaTopic, ar -> async0.complete());
        async0.await();

        Async async = context.async(2);
        controller.reconcile(null, kubeTopic, kafkaTopic, privateTopic, reconcileResult -> {
            assertSucceeded(context, reconcileResult);
            mockTopicStore.assertExists(context, topicName);
            mockK8s.assertExists(context, topicName.asMapName());
            mockKafka.assertExists(context, topicName);
            mockK8s.assertNoEvents(context);
            mockTopicStore.read(topicName, readResult -> {
                assertSucceeded(context, readResult);
                context.assertEquals(kafkaTopic, readResult.result());
                async.countDown();
            });
            mockK8s.getFromName(topicName.asMapName(), readResult -> {
                assertSucceeded(context, readResult);
                context.assertEquals(kafkaTopic, TopicSerialization.fromConfigMap(readResult.result()));
                async.countDown();
            });
            context.assertEquals(kafkaTopic, mockKafka.getTopicState(topicName));
        });
    }

    /**
     * Test reconciliation when a cm has been deleted while the controller
     * wasn't running
     */
    @Test
    public void testReconcile_noCm_withKafka_withPrivate(TestContext context) {
        Topic kubeTopic = null;
        Topic kafkaTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar")).build();
        Topic privateTopic = kafkaTopic;

        Async async0 = context.async(2);
        mockKafka.createTopic(kafkaTopic, ar -> async0.countDown());
        mockKafka.setDeleteTopicResponse(topicName, null);
        mockTopicStore.setCreateTopicResponse(topicName, null);
        mockTopicStore.create(kafkaTopic, ar -> async0.countDown());
        mockTopicStore.setDeleteTopicResponse(topicName, null);
        async0.await();

        Async async = context.async();
        controller.reconcile(null, kubeTopic, kafkaTopic, privateTopic, reconcileResult -> {
            assertSucceeded(context, reconcileResult);
            mockTopicStore.assertNotExists(context, topicName);
            mockK8s.assertNotExists(context, topicName.asMapName());
            mockKafka.assertNotExists(context, topicName);
            mockK8s.assertNoEvents(context);
            async.complete();
        });
    }

    /**
     * Test reconciliation when a cm has been added both in kafka and in k8s while the controller was down, and both
     * topics are identical.
     */
    @Test
    public void testReconcile_withCm_withKafka_noPrivate_matching(TestContext context) {
        Topic kubeTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar")).build();
        Topic kafkaTopic = kubeTopic;
        Topic privateTopic = null;

        Async async0 = context.async(2);
        mockKafka.setCreateTopicResponse(topicName -> Future.succeededFuture());
        mockKafka.createTopic(kafkaTopic, ar -> async0.countDown());
        mockK8s.setCreateResponse(topicName.asMapName(), null);
        mockK8s.createConfigMap(TopicSerialization.toConfigMap(kubeTopic, cmPredicate), ar -> async0.countDown());
        mockTopicStore.setCreateTopicResponse(topicName, null);
        async0.await();

        Async async = context.async();
        controller.reconcile(null, kubeTopic, kafkaTopic, privateTopic, reconcileResult -> {
            assertSucceeded(context, reconcileResult);
            mockTopicStore.assertExists(context, topicName);
            mockK8s.assertExists(context, topicName.asMapName());
            mockK8s.assertNoEvents(context);
            mockKafka.assertExists(context, topicName);
            mockTopicStore.read(topicName, readResult -> {
                assertSucceeded(context, readResult);
                context.assertEquals(kubeTopic, readResult.result());
                async.complete();
            });
            context.assertEquals(kafkaTopic, mockKafka.getTopicState(topicName));

        });
    }

    /**
     * Test reconciliation when a cm has been added both in kafka and in k8s while the controller was down, and
     * the topics are irreconcilably different: Kafka wins
     */
    @Test
    public void testReconcile_withCm_withKafka_noPrivate_configsReconcilable(TestContext context) {
        Topic kubeTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar")).build();
        Topic kafkaTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("unclean.leader.election.enable", "true")).build();
        Topic privateTopic = null;
        Topic mergedTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("unclean.leader.election.enable", "true", "cleanup.policy", "bar")).build();

        Async async0 = context.async(2);
        mockKafka.setCreateTopicResponse(topicName -> Future.succeededFuture());
        mockKafka.createTopic(kafkaTopic, ar -> async0.countDown());
        mockKafka.setUpdateTopicResponse(topicName -> Future.succeededFuture());

        ConfigMap cm = TopicSerialization.toConfigMap(kubeTopic, cmPredicate);
        mockK8s.setCreateResponse(topicName.asMapName(), null);
        mockK8s.createConfigMap(cm, ar -> async0.countDown());
        mockK8s.setModifyResponse(topicName.asMapName(), null);
        mockTopicStore.setCreateTopicResponse(topicName, null);
        async0.await();

        Async async = context.async(2);
        controller.reconcile(cm, kubeTopic, kafkaTopic, privateTopic, reconcileResult -> {
            assertSucceeded(context, reconcileResult);
            mockTopicStore.assertExists(context, topicName);
            mockK8s.assertExists(context, topicName.asMapName());
            mockKafka.assertExists(context, topicName);
            mockTopicStore.read(topicName, readResult -> {
                assertSucceeded(context, readResult);
                context.assertEquals(mergedTopic, readResult.result());
                async.countDown();
            });
            mockK8s.getFromName(topicName.asMapName(), readResult -> {
                assertSucceeded(context, readResult);
                context.assertEquals(mergedTopic, TopicSerialization.fromConfigMap(readResult.result()));
                async.countDown();
            });
            context.assertEquals(mergedTopic, mockKafka.getTopicState(topicName));
        });
    }

    /**
     * Test reconciliation when a cm has been added both in kafka and in k8s while the controller was down, and
     * the topics are irreconcilably different: Kafka wins
     */
    @Test
    public void testReconcile_withCm_withKafka_noPrivate_irreconcilable(TestContext context) {
        Topic kubeTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar")).build();
        Topic kafkaTopic = new Topic.Builder(topicName.toString(), 12, (short) 2, map("cleanup.policy", "baz")).build();
        Topic privateTopic = null;

        Async async0 = context.async(2);
        mockKafka.setCreateTopicResponse(topicName -> Future.succeededFuture());
        mockKafka.createTopic(kafkaTopic, ar -> async0.countDown());

        ConfigMap cm = TopicSerialization.toConfigMap(kubeTopic, cmPredicate);
        mockK8s.setCreateResponse(topicName.asMapName(), null);
        mockK8s.createConfigMap(cm, ar -> async0.countDown());
        mockK8s.setModifyResponse(topicName.asMapName(), null);
        mockTopicStore.setCreateTopicResponse(topicName, null);
        async0.await();

        Async async = context.async(2);
        controller.reconcile(cm, kubeTopic, kafkaTopic, privateTopic, reconcileResult -> {
            assertSucceeded(context, reconcileResult);
            mockK8s.assertContainsEvent(context, e ->
                    e.getMessage().contains("ConfigMap is incompatible with the topic metadata. " +
                            "The topic metadata will be treated as canonical."));
            mockTopicStore.assertExists(context, topicName);
            mockK8s.assertExists(context, topicName.asMapName());
            mockKafka.assertExists(context, topicName);
            mockTopicStore.read(topicName, readResult -> {
                assertSucceeded(context, readResult);
                context.assertEquals(kafkaTopic, readResult.result());
                async.countDown();
            });
            mockK8s.getFromName(topicName.asMapName(), readResult -> {
                assertSucceeded(context, readResult);
                context.assertEquals(kafkaTopic, TopicSerialization.fromConfigMap(readResult.result()));
                async.countDown();
            });
            context.assertEquals(kafkaTopic, mockKafka.getTopicState(topicName));
        });
    }

    /**
     * Test reconciliation when a cm has been changed both in kafka and in k8s while the controller was down, and
     * a 3 way merge is needed.
     */
    @Test
    public void testReconcile_withCm_withKafka_withPrivate_3WayMerge(TestContext context) {
        Topic kubeTopic = new Topic.Builder(topicName, mapName, 10, (short) 2, map("cleanup.policy", "bar")).build();
        Topic kafkaTopic = new Topic.Builder(topicName, mapName, 12, (short) 2, map("cleanup.policy", "baz")).build();
        Topic privateTopic = new Topic.Builder(topicName, mapName, 10, (short) 2, map("cleanup.policy", "baz")).build();
        Topic resultTopic = new Topic.Builder(topicName, mapName, 12, (short) 2, map("cleanup.policy", "bar")).build();

        Async async0 = context.async(3);
        mockKafka.setCreateTopicResponse(topicName -> Future.succeededFuture());
        mockKafka.createTopic(kafkaTopic, ar -> async0.countDown());
        mockKafka.setUpdateTopicResponse(topicName -> Future.succeededFuture());

        ConfigMap cm = TopicSerialization.toConfigMap(kubeTopic, cmPredicate);
        mockK8s.setCreateResponse(topicName.asMapName(), null);
        mockK8s.createConfigMap(cm, ar -> async0.countDown());
        mockK8s.setModifyResponse(topicName.asMapName(), null);
        mockTopicStore.setCreateTopicResponse(topicName, null);
        mockTopicStore.create(privateTopic, ar -> async0.countDown());
        async0.await();

        Async async = context.async(3);
        controller.reconcile(cm, kubeTopic, kafkaTopic, privateTopic, reconcileResult -> {
            assertSucceeded(context, reconcileResult);
            mockK8s.assertNoEvents(context);
            mockTopicStore.read(topicName, readResult -> {
                assertSucceeded(context, readResult);
                context.assertEquals(resultTopic, readResult.result());
                async.countDown();
            });
            mockK8s.getFromName(topicName.asMapName(), readResult -> {
                assertSucceeded(context, readResult);
                context.assertEquals(resultTopic, TopicSerialization.fromConfigMap(readResult.result()));
                async.countDown();
            });
            context.assertEquals(resultTopic, mockKafka.getTopicState(topicName));
            async.countDown();
        });
    }

    // TODO 3way reconcilation where kafka and kube agree
    // TODO 3way reconcilation where all three agree
    // TODO 3way reconcilation with conflict
    // TODO reconciliation where only private state exists => delete the private state

    // TODO tests for the other reconciliation cases
    // + non-matching predicate
    // + error cases

    private void configMapRemoved(TestContext context, Exception deleteTopicException, Exception storeException) {
        Topic kubeTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar")).build();
        Topic kafkaTopic = kubeTopic;
        Topic privateTopic = kubeTopic;

        mockKafka.setCreateTopicResponse(topicName.toString(), null)
                .createTopic(kafkaTopic, ar -> { });
        mockKafka.setTopicMetadataResponse(topicName, getTopicMetadata(kubeTopic), null);
        mockKafka.setDeleteTopicResponse(topicName, deleteTopicException);

        mockTopicStore.setCreateTopicResponse(topicName, null)
                .create(privateTopic, ar -> { });
        mockTopicStore.setDeleteTopicResponse(topicName, storeException);

        ConfigMap cm = TopicSerialization.toConfigMap(kubeTopic, cmPredicate);

        Async async = context.async();
        controller.onConfigMapDeleted(cm, ar -> {
            if (deleteTopicException != null
                    || storeException != null) {
                assertFailed(context, ar);
                if (deleteTopicException != null) {
                    // should still exist
                    mockKafka.assertExists(context, kafkaTopic.getTopicName());
                } else {
                    mockKafka.assertNotExists(context, kafkaTopic.getTopicName());
                }
                mockTopicStore.assertExists(context, kafkaTopic.getTopicName());
            } else {
                assertSucceeded(context, ar);
                mockKafka.assertNotExists(context, kafkaTopic.getTopicName());
                mockTopicStore.assertNotExists(context, kafkaTopic.getTopicName());
            }
            async.complete();
        });
    }

    @Test
    public void testOnConfigMapChanged(TestContext context) {
        Topic kubeTopic = new Topic.Builder(topicName, mapName, 10, (short) 2, map("cleanup.policy", "baz")).build();
        Topic kafkaTopic = new Topic.Builder(topicName, mapName, 10, (short) 2, map("cleanup.policy", "bar")).build();
        Topic privateTopic = kafkaTopic;
        ConfigMap cm = TopicSerialization.toConfigMap(kubeTopic, cmPredicate);

        mockKafka.setCreateTopicResponse(topicName.toString(), null)
                .createTopic(kafkaTopic, ar -> { });
        mockKafka.setTopicMetadataResponse(topicName, getTopicMetadata(kafkaTopic), null);
        mockKafka.setUpdateTopicResponse(topicName -> Future.succeededFuture());

        mockTopicStore.setCreateTopicResponse(topicName, null)
                .create(privateTopic, ar -> { });
        mockTopicStore.setUpdateTopicResponse(topicName, null);

        mockK8s.setModifyResponse(mapName, null);

        Async async = context.async(3);
        controller.onConfigMapModified(cm, ar -> {
            assertSucceeded(context, ar);
            context.assertEquals("baz", mockKafka.getTopicState(topicName).getConfig().get("cleanup.policy"));
            mockTopicStore.read(topicName, ar2 -> {
                assertSucceeded(context, ar2);
                context.assertEquals("baz", ar2.result().getConfig().get("cleanup.policy"));
                async.countDown();
            });
            mockK8s.getFromName(mapName, ar2 -> {
                assertSucceeded(context, ar2);
                context.assertEquals("baz", TopicSerialization.fromConfigMap(ar2.result()).getConfig().get("cleanup.policy"));
                async.countDown();
            });
            async.countDown();
        });
    }

    @Test
    public void testOnConfigMapRemoved(TestContext context) {
        Exception deleteTopicException = null;
        Exception storeException = null;
        configMapRemoved(context, deleteTopicException, storeException);
    }

    @Test
    public void testOnConfigMapRemoved_UnknownTopicOrPartitionException(TestContext context) {
        Exception deleteTopicException = new UnknownTopicOrPartitionException();
        Exception storeException = null;
        configMapRemoved(context, deleteTopicException, storeException);
    }

    @Test
    public void testOnConfigMapRemoved_NoSuchEntityExistsException(TestContext context) {
        Exception deleteTopicException = null;
        Exception storeException = new TopicStore.NoSuchEntityExistsException();
        configMapRemoved(context, deleteTopicException, storeException);
    }

    private void topicDeleted(TestContext context, Exception storeException, Exception k8sException) {
        Topic kubeTopic = new Topic.Builder(topicName.toString(), 10, (short) 2, map("cleanup.policy", "bar")).withMapName(mapName).build();
        Topic kafkaTopic = kubeTopic;
        Topic privateTopic = kubeTopic;

        mockK8s.setCreateResponse(mapName, null)
                .createConfigMap(TopicSerialization.toConfigMap(kubeTopic, cmPredicate), ar -> { });
        mockK8s.setDeleteResponse(mapName, k8sException);

        mockTopicStore.setCreateTopicResponse(topicName, null)
                .create(privateTopic, ar -> { });
        mockTopicStore.setDeleteTopicResponse(topicName, storeException);

        Async async = context.async();
        controller.onTopicDeleted(topicName, ar -> {
            if (k8sException != null
                    || storeException != null) {
                assertFailed(context, ar);
                if (k8sException == null) {
                    mockK8s.assertNotExists(context, mapName);
                } else {
                    mockK8s.assertExists(context, mapName);
                }
                mockTopicStore.assertExists(context, topicName);
            } else {
                assertSucceeded(context, ar);
                mockK8s.assertNotExists(context, mapName);
                mockTopicStore.assertNotExists(context, topicName);
            }
            async.complete();
        });
    }

    @Test
    public void testOnTopicDeleted(TestContext context) {
        Exception storeException = null;
        Exception k8sException = null;
        topicDeleted(context, storeException, k8sException);
    }

    @Test
    public void testOnTopicDeleted_NoSuchEntityExistsException(TestContext context) {
        Exception k8sException = null;
        Exception storeException = new TopicStore.NoSuchEntityExistsException();
        topicDeleted(context, storeException, k8sException);
    }

    @Test
    public void testOnTopicDeleted_KubernetesClientException(TestContext context) {
        Exception k8sException = new KubernetesClientException("");
        Exception storeException = null;
        topicDeleted(context, storeException, k8sException);
    }

    // TODO tests for nasty races (e.g. create on both ends, update on one end and delete on the other)
    // I think in these cases we should seek to detect the concurrent modification
    // and perform a full reconciliation, possibly after a backoff time
    // (to cover the case where topic config and other aspects get changed via multiple calls)
    // TODO test for zookeeper session timeout
    // TODO test for Kubernetes connection death
}
