/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.enmasse.barnabas.operator.topic;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;

@RunWith(VertxUnitRunner.class)
public class OperatorTest {

    private final CmPredicate cmPredicate = new CmPredicate("type", "runtime",
            "kind", "topic",
            "app", "barnabas");

    private final TopicName topicName = new TopicName("my-topic");
    private final MapName mapName = topicName.asMapName();
    private Vertx vertx = Vertx.vertx();
    private MockKafka mockKafka = new MockKafka();
    private MockTopicStore mockTopicStore = new MockTopicStore();
    private MockK8s mockK8s = new MockK8s();
    private Operator op;

    @Before
    public void setup() {
        mockKafka = new MockKafka();
        mockTopicStore = new MockTopicStore();
        mockK8s = new MockK8s();
        op = new Operator(vertx, mockKafka, mockK8s, cmPredicate);
        op.setTopicStore(mockTopicStore);
    }

    @After
    public void teardown() {
        vertx.close();
        mockKafka = null;
        mockTopicStore = null;
        mockK8s = null;
        op = null;
    }

    private Map<String, String> map(String... pairs) {
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException();
        }
        Map<String, String> result = new HashMap<>(pairs.length/2);
        for (int i = 0; i < pairs.length; i+=2) {
            result.put(pairs[i], pairs[i+1]);
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

    /** Test what happens when a non-topic config map gets created in kubernetes */
    @Test
    public void testOnConfigMapAdded_ignorable(TestContext context) {
        ConfigMap cm = new ConfigMapBuilder().withNewMetadata().withName("non-topic").endMetadata().build();

        Async async = context.async();
        op.onConfigMapAdded(cm, ar -> {
            assertSucceeded(context, ar);
            mockKafka.assertEmpty(context);
            mockTopicStore.assertEmpty(context);
            async.complete();

        });
    }

    /**
     * Trigger {@link Operator#onConfigMapAdded(ConfigMap, Handler)}
     * and have the Kafka and TopicStore respond with the given exceptions.
     */
    private Operator configMapAdded(TestContext context, Exception createException, Exception storeException) {
        mockKafka.setCreateTopicResponse(topicName.toString(), createException);
        mockTopicStore.setCreateTopicResponse(topicName, storeException);

        ConfigMap cm = new ConfigMapBuilder().withNewMetadata()
                .withName(topicName.toString())
                .withLabels(cmPredicate.labels()).endMetadata()
                .withData(map(TopicSerialization.CM_KEY_PARTITIONS, "10",
                        TopicSerialization.CM_KEY_REPLICAS, "2")).build();

        Async async = context.async();

        op.onConfigMapAdded(cm, ar -> {
            if (createException != null
                    || storeException != null) {
                assertFailed(context, ar);
                Class<? extends Exception> expectedExceptionType;
                if (createException != null) {
                    expectedExceptionType = createException.getClass();
                } else {
                    expectedExceptionType = storeException.getClass();
                }
                context.assertEquals(expectedExceptionType, ar.cause().getClass());
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

        return op;
    }

    /**
     * 1. operator is notified that a ConfigMap is created
     * 2. operator successfully creates topic in kafka
     * 3. operator successfully creates in topic store
     */
    @Test
    public void testOnConfigMapAdded(TestContext context) {
        configMapAdded(context, null, null);
    }

    /**
     * 1. operator is notified that a ConfigMap is created
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
     * 1. operator is notified that a ConfigMap is created
     * 2. error when creating topic in kafka
     */
    @Test
    public void testOnConfigMapAdded_ClusterAuthorizationException(TestContext context) {
        Exception createException = new ClusterAuthorizationException("");
        Operator op = configMapAdded(context, createException, null);
        // TODO check a k8s event got created
        // TODO what happens when we subsequently reconcile?
    }

    /**
     * 1. operator is notified that a ConfigMap is created
     * 2. operator successfully creates topic in kafka
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
     * 1. operator is notified that a topic is created
     * 2. operator successfully queries kafka to get topic metadata
     * 3. operator successfully creates config map
     * 4. operator successfully creates in topic store
     */
    @Test
    public void testProcessableTopicCreated(TestContext context) {
        TopicMetadata topicMetadata = getTopicMetadata();

        mockTopicStore.setCreateTopicResponse(topicName, null);
        mockKafka.setTopicMetadataResponse(topicName, topicMetadata, null);
        mockK8s.setCreateResponse(mapName, null);

        Async async = context.async();
        op.onTopicCreated(topicName, ar -> {
            assertSucceeded(context, ar);
            mockK8s.assertExists(context, mapName);
            mockTopicStore.assertContains(context, TopicSerialization.fromTopicMetadata(topicMetadata));
            async.complete();
        });
    }

    /**
     * 1. operator is notified that a topic is created
     * 2. operator initially failed querying kafka to get topic metadata
     * 3. operator is subsequently successful in querying kafka to get topic metadata
     * 4. operator successfully creates config map
     * 5. operator successfully creates in topic store
     */
    @Test
    public void testProcessableTopicCreated_retry(TestContext context) {
        TopicMetadata topicMetadata = getTopicMetadata();

        mockTopicStore.setCreateTopicResponse(topicName, null);
        AtomicInteger counter = new AtomicInteger();
        mockKafka.setTopicMetadataResponse(t -> {
            int count = counter.getAndIncrement();
            if (count == 3) {
                return Future.succeededFuture(topicMetadata);
            } else if (count < 3) {
                return Future.failedFuture(new UnknownTopicOrPartitionException());
            }
            context.fail("This should never happen");
            return Future.failedFuture("This should never happen");
        });
        mockK8s.setCreateResponse(mapName, null);

        Async async = context.async();
        op.onTopicCreated(topicName, ar -> {
            assertSucceeded(context, ar);
            context.assertEquals(4, counter.get());
            mockK8s.assertExists(context, mapName);
            mockTopicStore.assertContains(context, TopicSerialization.fromTopicMetadata(topicMetadata));
            async.complete();
        });
    }

    private void assertSucceeded(TestContext context, AsyncResult<Void> ar) {
        context.assertTrue(ar.succeeded(), ar.cause() != null ? ar.cause().toString(): "");
    }

    private void assertFailed(TestContext context, AsyncResult<Void> ar) {
        context.assertFalse(ar.succeeded(), ar.failed() ? "" : ar.result().toString());
    }


    /**
     * 1. operator is notified that a topic is created
     * 2. operator times out getting metadata
     */
    @Test
    public void testProcessableTopicCreated_retryTimeout(TestContext context) {
        TopicMetadata topicMetadata = getTopicMetadata();

        mockKafka.setTopicMetadataResponse(topicName, null, new UnknownTopicOrPartitionException());

        Async async = context.async();
        op.onTopicCreated(topicName, ar -> {
            assertFailed(context, ar);
            context.assertEquals(ar.cause().getClass(), MaxAttemptsExceededException.class);
            mockK8s.assertNotExists(context, mapName);
            mockTopicStore.assertNotExists(context, topicName);
            async.complete();
        });
    }

    // TODO error getting full topic metadata, and then reconciliation
    // TODO error creating config map (exists), and then reconciliation

    /**
     * Test reconciliation when a configmap has been created while the operator wasn't running
     */
    @Test
    public void testReconcile_withCm_noKafka_noPrivate(TestContext context) {
        mockKafka.setCreateTopicResponse(topicName.toString(), null);

        mockTopicStore.setCreateTopicResponse(topicName, null);

        Topic kubeTopic = new Topic.Builder(topicName.toString(), 10, (short)2, map("foo", "bar")).build();
        Topic kafkaTopic = null;
        Topic privateTopic = null;
        Async async = context.async();
        op.reconcile(null, kubeTopic, kafkaTopic, privateTopic, reconcileResult -> {
            assertSucceeded(context, reconcileResult);
            mockKafka.assertExists(context, kubeTopic.getTopicName());
            mockTopicStore.assertExists(context, kubeTopic.getTopicName());
            mockK8s.assertNoEvents(context);
            async.complete();
        });
    }

    /**
     * Test reconciliation when a topic has been deleted while the operator
     * wasn't running
     */
    @Test
    public void testReconcile_withCm_noKafka_withPrivate(TestContext context) {

        Topic kubeTopic = new Topic.Builder(topicName.toString(), 10, (short)2, map("foo", "bar")).build();
        Topic kafkaTopic = null;
        Topic privateTopic = kubeTopic;

        mockK8s.setCreateResponse(mapName, null)
                .createConfigMap(TopicSerialization.toConfigMap(kubeTopic, cmPredicate), ar -> {});
        mockK8s.setDeleteResponse(topicName, null);
        mockTopicStore.setCreateTopicResponse(topicName, null)
                .create(privateTopic, ar-> {});
        mockTopicStore.setDeleteTopicResponse(topicName, null);


        Async async = context.async();

        op.reconcile(null, kubeTopic, kafkaTopic, privateTopic, reconcileResult -> {
            assertSucceeded(context, reconcileResult);
            mockKafka.assertNotExists(context, kubeTopic.getTopicName());
            mockTopicStore.assertNotExists(context, kubeTopic.getTopicName());
            mockK8s.assertNotExists(context, kubeTopic.getMapName());
            async.complete();
        });
    }

    // TODO tests for the other reconciliation cases

    private void configMapRemoved(TestContext context, Exception deleteTopicException, Exception storeException) {
        Topic kubeTopic = new Topic.Builder(topicName.toString(), 10, (short)2, map("foo", "bar")).build();
        Topic kafkaTopic = kubeTopic;
        Topic privateTopic = kubeTopic;

        mockKafka.setCreateTopicResponse(topicName.toString(), null)
                .createTopic(TopicSerialization.toNewTopic(kafkaTopic), ar -> {});
        mockKafka.setDeleteTopicResponse(topicName, deleteTopicException);

        mockTopicStore.setCreateTopicResponse(topicName, null)
                .create(privateTopic, ar -> {});
        mockTopicStore.setDeleteTopicResponse(topicName, storeException);

        ConfigMap cm = TopicSerialization.toConfigMap(kubeTopic, cmPredicate);

        Async async = context.async();
        op.onConfigMapDeleted(cm, ar -> {
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

    @Test
    public void testOnTopicDeleted(TestContext context) {
        Exception storeException = null;
        Exception k8sException = null;
        Topic kubeTopic = new Topic.Builder(topicName.toString(), 10, (short)2, map("foo", "bar")).build();
        Topic kafkaTopic = kubeTopic;
        Topic privateTopic = kubeTopic;

        mockK8s.setCreateResponse(mapName, null)
                .createConfigMap(TopicSerialization.toConfigMap(kubeTopic, cmPredicate), ar -> {});
        mockK8s.setDeleteResponse(topicName, k8sException);

        mockTopicStore.setCreateTopicResponse(topicName, null)
                .create(privateTopic, ar -> {});
        mockTopicStore.setDeleteTopicResponse(topicName, storeException);

        Async async = context.async();
        op.onTopicDeleted(topicName, ar -> {
            context.assertTrue(ar.succeeded());
            async.complete();
        });
    }

    // TODO tests for nasty races (e.g. create on both ends, update on one end and delete on the other)
    // I think in these cases we should seek to detect the concurrent modification
    // and perform a full reconciliation, possibly after a backoff time
    // (to cover the case where topic config and other aspects get changed via multiple calls)
    // TODO test for zookeeper session timeout
    // TODO test for Kubernetes connection death
}
