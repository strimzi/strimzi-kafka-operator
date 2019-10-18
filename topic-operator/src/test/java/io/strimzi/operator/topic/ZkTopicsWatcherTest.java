/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.operator.topic.MockTopicOperator.MockOperatorEvent.Type;
import io.vertx.core.Future;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(VertxUnitRunner.class)
public class ZkTopicsWatcherTest {

    private MockTopicOperator operator;
    private MockZk mockZk;
    @Before
    public void setup() {
        operator = new MockTopicOperator();
        mockZk = new MockZk();
    }

    @Test
    public void testTopicAdd() {
        addTopic();
    }

    private void addTopic() {
        operator = new MockTopicOperator();
        operator.topicCreatedResult = Future.succeededFuture();
        mockZk = new MockZk();
        mockZk.childrenResult = Future.succeededFuture(asList("foo", "bar"));
        mockZk.dataResult = Future.succeededFuture(new byte[0]);
        TopicConfigsWatcher topicConfigsWatcher = new TopicConfigsWatcher(operator);
        ZkTopicWatcher topicWatcher = new ZkTopicWatcher(operator);
        ZkTopicsWatcher topicsWatcher = new ZkTopicsWatcher(operator, topicConfigsWatcher, topicWatcher);
        topicsWatcher.start(mockZk);
        mockZk.triggerChildren(Future.succeededFuture(asList("foo", "bar", "baz")));
        assertEquals(asList(new MockTopicOperator.MockOperatorEvent(
                Type.CREATE, new TopicName("baz"))), operator.getMockOperatorEvents());
        assertTrue(topicConfigsWatcher.watching("baz"));
        assertTrue(topicWatcher.watching("baz"));
    }

    @Test
    public void testTopicConfigChange() {
        // First add a topic
        addTopic();
        // Now change the config
        operator.clearEvents();
        mockZk.triggerData("/config/topics/baz", Future.succeededFuture(new byte[0]));
        assertEquals(singletonList(
                new MockTopicOperator.MockOperatorEvent(Type.MODIFY_CONFIG, new TopicName("baz"))),
                operator.getMockOperatorEvents());

        operator.clearEvents();
        mockZk.triggerData("/brokers/topics/baz", Future.succeededFuture(new byte[0]));
        assertEquals(singletonList(
                new MockTopicOperator.MockOperatorEvent(Type.MODIFY_PARTITIONS, new TopicName("baz"))),
                operator.getMockOperatorEvents());
    }

    @Test
    public void testTopicDelete() {
        operator = new MockTopicOperator();
        operator.topicDeletedResult = Future.succeededFuture();
        mockZk = new MockZk();
        mockZk.childrenResult = Future.succeededFuture(asList("foo", "bar"));
        TopicConfigsWatcher topicConfigsWatcher = new TopicConfigsWatcher(operator);
        ZkTopicWatcher topicWatcher = new ZkTopicWatcher(operator);
        ZkTopicsWatcher topicsWatcher = new ZkTopicsWatcher(operator, topicConfigsWatcher, topicWatcher);
        topicsWatcher.start(mockZk);
        mockZk.triggerChildren(Future.succeededFuture(asList("foo")));
        assertEquals(asList(new MockTopicOperator.MockOperatorEvent(
                Type.DELETE, new TopicName("bar"))), operator.getMockOperatorEvents());
        assertFalse(topicConfigsWatcher.watching("baz"));
    }
}
