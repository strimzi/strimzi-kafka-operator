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

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
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

    Vertx vertx = Vertx.vertx();

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

    /** Test what happens when a non-topic config map gets created in kubernetes */
    @Test
    public void testIgnorableConfigMapCreated(TestContext context) {
        ConfigMap cm = new ConfigMapBuilder().withNewMetadata().withName("non-topic").endMetadata().build();

        Operator op = new Operator(null, null, null, vertx, cmPredicate);

        Async async = context.async();
        op.onConfigMapAdded(cm, ar -> {
            context.assertTrue(ar.succeeded());
            async.complete();
        });
    }

    /**
     * Trigger {@link Operator#onConfigMapAdded(ConfigMap, Handler)}
     * and have the Kafka and TopicStore respond with the given exceptions.
     */
    private void processableConfigMapCreatedWithError(TestContext context, Exception createException, Exception storeException) {
        MockKafka mockKafka = new MockKafka().setCreateTopicResponse("my-topic", createException);
        MockTopicStore mockTopicStore = new MockTopicStore().setCreateTopicResponse(new TopicName("my-topic"), storeException);

        ConfigMap cm = new ConfigMapBuilder().withNewMetadata()
                .withName("my-topic")
                .withLabels(cmPredicate.labels()).endMetadata()
                .withData(map(TopicSerialization.CM_KEY_PARTITIONS, "10",
                        TopicSerialization.CM_KEY_REPLICAS, "2")).build();

        Operator op = new Operator(null, mockKafka, null, vertx, cmPredicate);
        op.setTopicStore(mockTopicStore);
        Async async = context.async();

        op.onConfigMapAdded(cm, ar -> {
            if (createException != null
                    || storeException != null) {
                context.assertFalse(ar.succeeded());
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
            } else {
                context.assertTrue(ar.succeeded());
                Topic expectedTopic = TopicSerialization.fromConfigMap(cm);
                mockKafka.assertContains(context, expectedTopic);
                mockTopicStore.assertContains(context, expectedTopic);
            }
            async.complete();
        });
    }

    /**
     * 1. operator is notified that a ConfigMap is created
     * 2. operator successfully creates topic in kafka
     * 3. operator successfully creates in topic store
     */
    @Test
    public void testProcessableConfigMapCreated(TestContext context) {
        processableConfigMapCreatedWithError(context, null, null);
    }

    /**
     * 1. operator is notified that a ConfigMap is created
     * 2. error when creating topic in kafka
     */
    @Test
    public void testProcessableConfigMapCreated_TopicExistsException(TestContext context) {
        Exception createException = new TopicExistsException("");
        processableConfigMapCreatedWithError(context, createException, null);
        // TODO check a k8s event got created
        // TODO what happens when we subsequently reconcile?
    }

    /**
     * 1. operator is notified that a ConfigMap is created
     * 2. error when creating topic in kafka
     */
    @Test
    public void testProcessableConfigMapCreated_ClusterAuthorizationException(TestContext context) {
        Exception createException = new ClusterAuthorizationException("");
        processableConfigMapCreatedWithError(context, createException, null);
        // TODO check a k8s event got created
        // TODO what happens when we subsequently reconcile?
    }

    /**
     * 1. operator is notified that a ConfigMap is created
     * 2. operator successfully creates topic in kafka
     * 3. error when creating in topic store
     */
    @Test
    public void testProcessableConfigMapCreated_EntityExistsException(TestContext context) {
        processableConfigMapCreatedWithError(context,
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
        Node node0 = new Node(0, "host0", 1234);
        Node node1 = new Node(1, "host1", 1234);
        Node node2 = new Node(2, "host2", 1234);
        List<Node> nodes02 = asList(node0, node1, node2);
        TopicDescription desc = new TopicDescription("my-topic", false, asList(
                new TopicPartitionInfo(0, node0, nodes02, nodes02),
                new TopicPartitionInfo(1, node0, nodes02, nodes02)
        ));
        Config config = new Config(Collections.emptyList());
        TopicMetadata topicMetadata = new TopicMetadata(desc, config);

        MockKafka mockKafka = new MockKafka();
        MockK8s mockK8s = new MockK8s();
        MockTopicStore mockTopicStore = new MockTopicStore();
        TopicName topicName = new TopicName("my-topic");
        mockKafka.setTopicMetadataResponse(topicName, topicMetadata, null);

        Operator op = new Operator(null, mockKafka, mockK8s, vertx, cmPredicate);
        op.setTopicStore(mockTopicStore);

        Async async = context.async();
        op.onTopicCreated(topicName, ar -> {
            context.assertTrue(ar.succeeded());
            mockK8s.assertExists(context, topicName.asMapName());
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
        Node node0 = new Node(0, "host0", 1234);
        Node node1 = new Node(1, "host1", 1234);
        Node node2 = new Node(2, "host2", 1234);
        List<Node> nodes02 = asList(node0, node1, node2);
        TopicDescription desc = new TopicDescription("my-topic", false, asList(
                new TopicPartitionInfo(0, node0, nodes02, nodes02),
                new TopicPartitionInfo(1, node0, nodes02, nodes02)
        ));
        Config config = new Config(Collections.emptyList());
        TopicMetadata topicMetadata = new TopicMetadata(desc, config);

        MockKafka mockKafka = new MockKafka();
        MockK8s mockK8s = new MockK8s();
        MockTopicStore mockTopicStore = new MockTopicStore();
        TopicName topicName = new TopicName("my-topic");
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

        Operator op = new Operator(null, mockKafka, mockK8s, vertx, cmPredicate);
        op.setTopicStore(mockTopicStore);

        Async async = context.async();
        op.onTopicCreated(topicName, ar -> {
            context.assertTrue(ar.succeeded());
            context.assertEquals(4, counter.get());
            mockK8s.assertExists(context, topicName.asMapName());
            mockTopicStore.assertContains(context, TopicSerialization.fromTopicMetadata(topicMetadata));
            async.complete();
        });
    }

    /**
     * 1. operator is notified that a topic is created
     * 2. operator times out getting metadata
     */
    @Test
    public void testProcessableTopicCreated_timeout(TestContext context) {
        Node node0 = new Node(0, "host0", 1234);
        Node node1 = new Node(1, "host1", 1234);
        Node node2 = new Node(2, "host2", 1234);
        List<Node> nodes02 = asList(node0, node1, node2);
        TopicDescription desc = new TopicDescription("my-topic", false, asList(
                new TopicPartitionInfo(0, node0, nodes02, nodes02),
                new TopicPartitionInfo(1, node0, nodes02, nodes02)
        ));
        Config config = new Config(Collections.emptyList());
        TopicMetadata topicMetadata = new TopicMetadata(desc, config);

        MockKafka mockKafka = new MockKafka();
        MockK8s mockK8s = new MockK8s();
        MockTopicStore mockTopicStore = new MockTopicStore();
        TopicName topicName = new TopicName("my-topic");
        mockKafka.setTopicMetadataResponse(topicName, null, new UnknownTopicOrPartitionException());

        Operator op = new Operator(null, mockKafka, mockK8s, vertx, cmPredicate);
        op.setTopicStore(mockTopicStore);

        Async async = context.async();
        op.onTopicCreated(topicName, ar -> {
            context.assertFalse(ar.succeeded());
            context.assertEquals(ar.cause().getClass(), MaxAttemptsExceededException.class);
            mockK8s.assertNotExists(context, topicName.asMapName());
            mockTopicStore.assertNotExists(context, topicName);
            async.complete();
        });
    }

    // TODO error getting full topic data
    // TODO error creating config map (exists)



    /**
     * Test reconciliation when a configmap has been created while the operator wasn't running
     */
    @Test
    public void testReconciliation_withCm_noKafka_noPrivate(TestContext context) {
        MockKafka mockKafka = new MockKafka().setCreateTopicResponse("my-topic", null);
        MockTopicStore mockStore = new MockTopicStore();

        Operator op = new Operator(null, mockKafka, null, vertx, cmPredicate);
        op.setTopicStore(mockStore);

        Topic kubeTopic = new Topic.Builder("my-topic", 10, (short)2, map("foo", "bar")).build();
        Topic kafkaTopic = null;
        Topic privateTopic = null;
        Async async = context.async();
        op.reconcile(null, kubeTopic, kafkaTopic, privateTopic, reconcileResult -> {
            context.assertTrue(reconcileResult.succeeded());
            mockKafka.assertExists(context, kubeTopic.getTopicName());
            mockStore.assertExists(context, kubeTopic.getTopicName());
            async.complete();
        });
        async.await();
    }

    /**
     * Test reconciliation when a topic has been deleted while the operator
     * wasn't running
     */
    @Test
    public void testReconciliation_withCm_noKafka_withPrivate(TestContext context) {

        Topic kubeTopic = new Topic.Builder("my-topic", 10, (short)2, map("foo", "bar")).build();
        Topic kafkaTopic = null;
        Topic privateTopic = new Topic.Builder(kubeTopic).withNumPartitions(5).build();

        MockKafka mockKafka = new MockKafka();
        MockTopicStore mockStore = new MockTopicStore();
        MockK8s mockK8s = new MockK8s();
        mockK8s.createConfigMap(TopicSerialization.toConfigMap(kubeTopic, cmPredicate), ar -> {});
        mockStore.create(privateTopic, ar-> {});

        Operator op = new Operator(null, mockKafka, mockK8s, vertx, cmPredicate);
        op.setTopicStore(mockStore);

        Async async = context.async();

        op.reconcile(null, kubeTopic, kafkaTopic, privateTopic, reconcileResult -> {
            context.assertTrue(reconcileResult.succeeded());
            mockKafka.assertNotExists(context, kubeTopic.getTopicName());
            mockStore.assertNotExists(context, kubeTopic.getTopicName());
            mockK8s.assertNotExists(context, kubeTopic.getMapName());
            async.complete();
        });



    }

    // TODO tests for the other reconciliation cases
    // TODO tests for nasty races (e.g. create on both ends, update on one end and delete on the other)
    // I think in these cases we should seek to detect the concurrent modification
    // and perform a full reconciliation, possibly after a backoff time
    // (to cover the case where topic config and other aspects get changed via multiple calls)
    // TODO test for zookeeper session timeout
    // TODO test for Kubernetes connection death
}
