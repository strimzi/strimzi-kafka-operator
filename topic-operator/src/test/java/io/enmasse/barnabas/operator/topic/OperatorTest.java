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
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.TopicExistsException;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OperatorTest {

    private final CmPredicate cmPredicate = new CmPredicate("type", "runtime",
            "kind", "topic",
            "app", "barnabas");

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
    public void testIgnorableConfigMapCreated() {
        ConfigMap cm = new ConfigMapBuilder().withNewMetadata().withName("non-topic").endMetadata().build();

        ScheduledExecutorService mockedExecutor = mock(ScheduledExecutorService.class);
        verify(mockedExecutor, never()).execute(any());

        Operator op = new Operator(null, null, null, mockedExecutor, cmPredicate);
        op.onConfigMapAdded(cm);
    }

    /**
     * Test what happens when a processable config map gets created in
     * kubernetes, in the successful case. */
    @Test
    public void testProcessableConfigMapCreated() {
        ArgumentCaptor<Handler<AsyncResult<Void>>> handler = processConfigMapCreated();

        // Simulate successful topic creation
        handler.getValue().handle(Future.succeededFuture());

        // TODO assert storeTopic created
    }

    /**
     * Test what happens when a processable config map gets created in
     * kubernetes, in the case where AdminClient returns error */
    @Test
    public void testProcessableConfigMapCreated_TopicExistsException() {
        ArgumentCaptor<Handler<AsyncResult<Void>>> handler = processConfigMapCreated();

        // Simulate error
        handler.getValue().handle(Future.failedFuture(new TopicExistsException("")));
        fail("TODO should either either throw or cause some reconciliation");
    }

    /**
     * Test what happens when a processable config map gets created in
     * kubernetes, in the case where AdminClient returns error */
    @Test
    public void testProcessableConfigMapCreated_ClusterAuthorizationException() {
        ArgumentCaptor<Handler<AsyncResult<Void>>> handler = processConfigMapCreated();

        // Simulate error
        try {
            handler.getValue().handle(Future.failedFuture(new ClusterAuthorizationException("")));
            fail("Should throw");
        } catch (OperatorException e) {
            // This would normally propagate via the executor's uncaught exception handler
        }
    }

    private ArgumentCaptor<Handler<AsyncResult<Void>>> processConfigMapCreated() {
        ScheduledExecutorService mockedExecutor = mock(ScheduledExecutorService.class);
        Kafka mockKafka = mock(Kafka.class);

        ConfigMap cm = new ConfigMapBuilder().withNewMetadata()
                .withName("my-topic")
                .withLabels(cmPredicate.labels()).endMetadata()
                .withData(map(TopicSerialization.CM_KEY_PARTITIONS, "10",
                        TopicSerialization.CM_KEY_REPLICAS, "2")).build();

        Operator op = new Operator(null, mockKafka, null, mockedExecutor, cmPredicate);
        op.onConfigMapAdded(cm);

        // assert work added to executor
        ArgumentCaptor<Operator.CreateKafkaTopic> argument = ArgumentCaptor.forClass(Operator.CreateKafkaTopic.class);
        verify(mockedExecutor).execute(argument.capture());
        argument.getValue().process();

        // Simulate processing the work
        ArgumentCaptor<NewTopic> newTopic = ArgumentCaptor.forClass(NewTopic.class);
        ArgumentCaptor<Handler<AsyncResult<Void>>> handler = ArgumentCaptor.forClass(Handler.class);
        verify(mockKafka).createTopic(newTopic.capture(), handler.capture());

        // Check the NewTopic is correct
        assertEquals("my-topic", newTopic.getValue().name());
        assertEquals(10, newTopic.getValue().numPartitions());
        assertEquals(2, newTopic.getValue().replicationFactor());
        return handler;
    }

    @Test
    public void testProcessableTopicCreated() {
        Node node0 = new Node(0, "host0", 1234);
        Node node1 = new Node(1, "host1", 1234);
        Node node2 = new Node(2, "host2", 1234);

        Kafka mockKafka = mock(Kafka.class);
        ScheduledExecutorService mockedExecutor = mock(ScheduledExecutorService.class);
        K8s mockK8s = mock(K8s.class);
        List<Node> nodes02 = asList(node0, node1, node2);
        TopicDescription desc = new TopicDescription("my-topic", false, asList(
                new TopicPartitionInfo(0, node0, nodes02, nodes02),
                new TopicPartitionInfo(1, node0, nodes02, nodes02)
        ));
        Config config = new Config(Collections.emptyList());
        when(mockKafka.topicMetadata(any(), anyLong(), any())).thenReturn(CompletableFuture.completedFuture(new TopicMetadata(desc, config)));

        Operator op = new Operator(null, mockKafka, mockK8s, mockedExecutor, cmPredicate);
        op.onTopicCreated(new TopicName("my-topic"));

        // assert task added to executor
        ArgumentCaptor<Operator.CreateConfigMap> argument = ArgumentCaptor.forClass(Operator.CreateConfigMap.class);
        verify(mockedExecutor).execute(argument.capture());

        // simulate the executor running the task
        argument.getValue().process();

        ArgumentCaptor<ConfigMap> configMap = ArgumentCaptor.forClass(ConfigMap.class);
        verify(mockK8s).createConfigMap(configMap.capture(), ar -> {});
        ConfigMap cm = configMap.getValue();
        assertEquals("my-topic", cm.getMetadata().getName());
        assertEquals(cmPredicate.labels(), cm.getMetadata().getLabels());
        assertEquals("2", cm.getData().get(TopicSerialization.CM_KEY_PARTITIONS));
        assertEquals("3", cm.getData().get(TopicSerialization.CM_KEY_REPLICAS));

        // TODO assert storeTopic created
    }

    // TODO timeout getting the full topic data
    // TODO error getting full topic data
    // TODO error creating config map (exists)

    /**
     * Test reconciliation when a configmap has been created while the operator wasn't running
     */
    @Test
    public void testReconciliation_withCm_noKafka_noPrivate() {

        ScheduledExecutorService mockedExecutor = mock(ScheduledExecutorService.class);
        Kafka mockKafka = mock(Kafka.class);
        TopicStore mockStore = mock(TopicStore.class);

        ArgumentCaptor<Topic> storeTopic = ArgumentCaptor.forClass(Topic.class);
        when(mockStore.create(storeTopic.capture())).thenReturn(CompletableFuture.completedFuture(null));

        Operator op = new Operator(null, mockKafka, null, mockedExecutor, cmPredicate);
        op.setTopicStore(mockStore);
        Topic kubeTopic = new Topic.Builder("my-topic", 10, (short)2, map("foo", "bar")).build();
        Topic kafkaTopic = null;
        Topic privateTopic = null;
        op.reconcile(null, kubeTopic, kafkaTopic, privateTopic);

        ArgumentCaptor<Operator.OperatorEvent> kafkaCreate = ArgumentCaptor.forClass(Operator.OperatorEvent.class);
        verify(mockedExecutor, times(2)).execute(kafkaCreate.capture());
        Operator.CreateKafkaTopic c = (Operator.CreateKafkaTopic)kafkaCreate.getAllValues().get(0);

        // simulate execution of the topic creation task
        c.process();
        ArgumentCaptor<NewTopic> newTopic = ArgumentCaptor.forClass(NewTopic.class);
        ArgumentCaptor<Handler> handler = ArgumentCaptor.forClass(Handler.class);
        verify(mockKafka).createTopic(newTopic.capture(), handler.capture());

        // TODO assert on the New Topic and call the handler

        Operator.CreateInTopicStore c2 = (Operator.CreateInTopicStore)kafkaCreate.getAllValues().get(1);

        // simulate execution of the topic store creation task
        c2.process();

        assertEquals(new TopicName("my-topic"), storeTopic.getValue().getTopicName());
        assertEquals(new MapName("my-topic"), storeTopic.getValue().getMapName());

    }

    /**
     * Test reconciliation when a topic has been deleted while the operator
     * wasn't running
     */
    @Test
    public void testReconciliation_withCm_noKafka_withPrivate() {

        ScheduledExecutorService mockedExecutor = mock(ScheduledExecutorService.class);
        Kafka mockKafka = mock(Kafka.class);
        TopicStore mockStore = mock(TopicStore.class);

        ArgumentCaptor<Topic> storeTopic = ArgumentCaptor.forClass(Topic.class);
        when(mockStore.create(storeTopic.capture())).thenReturn(CompletableFuture.completedFuture(null));

        Operator op = new Operator(null, mockKafka, null, mockedExecutor, cmPredicate);
        op.setTopicStore(mockStore);
        Topic kubeTopic = new Topic.Builder("my-topic", 10, (short)2, map("foo", "bar")).build();
        Topic kafkaTopic = null;
        Topic privateTopic = kubeTopic;
        op.reconcile(null, kubeTopic, kafkaTopic, privateTopic);

        ArgumentCaptor<Operator.OperatorEvent> kafkaCreate = ArgumentCaptor.forClass(Operator.OperatorEvent.class);
        verify(mockedExecutor, times(2)).execute(kafkaCreate.capture());
        Operator.CreateKafkaTopic c = (Operator.CreateKafkaTopic)kafkaCreate.getAllValues().get(0);

        // simulate execution of the topic creation task
        c.process();
        ArgumentCaptor<NewTopic> newTopic = ArgumentCaptor.forClass(NewTopic.class);
        ArgumentCaptor<Handler> handler = ArgumentCaptor.forClass(Handler.class);
        verify(mockKafka).createTopic(newTopic.capture(), handler.capture());

        // TODO assert on the New Topic and call the handler

        Operator.CreateInTopicStore c2 = (Operator.CreateInTopicStore)kafkaCreate.getAllValues().get(1);

        // simulate execution of the topic store creation task
        c2.process();

        assertEquals(new TopicName("my-topic"), storeTopic.getValue().getTopicName());
        assertEquals(new MapName("my-topic"), storeTopic.getValue().getMapName());

    }

    // TODO tests for the other reconciliation cases
    // TODO tests for nasty races (e.g. create on both ends, update on one end and delete on the other)
    // I think in these cases we should seek to detect the concurrent modification
    // and perform a full reconciliation, possibly after a backoff time
    // (to cover the case where topic config and other aspects get changed via multiple calls)
    // TODO test for zookeeper session timeout
    // TODO test for Kubernetes connection death
}
