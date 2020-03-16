/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.operator.topic.MockTopicOperator.MockOperatorEvent.Type;
import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(VertxExtension.class)
public class ZkTopicsWatcherTest {

    private MockTopicOperator operator;
    private MockZk mockZk;
    @BeforeEach
    public void setup() {
        operator = new MockTopicOperator();
        mockZk = new MockZk();
    }

    @Test
    public void testTopicAdd() {
        addTopic();
    }

    private void addTopic() {
        operator.topicCreatedResult = Future.succeededFuture();
        mockZk.childrenResult = Future.succeededFuture(asList("foo", "bar"));
        mockZk.dataResult = Future.succeededFuture(new byte[0]);

        TopicConfigsWatcher topicConfigsWatcher = new TopicConfigsWatcher(operator);
        ZkTopicWatcher topicWatcher = new ZkTopicWatcher(operator);
        ZkTopicsWatcher topicsWatcher = new ZkTopicsWatcher(operator, topicConfigsWatcher, topicWatcher);
        topicsWatcher.start(mockZk);
        mockZk.triggerChildren(Future.succeededFuture(asList("foo", "bar", "baz")));

        assertThat(operator.getMockOperatorEvents(),
                is(asList(new MockTopicOperator.MockOperatorEvent(Type.CREATE, new TopicName("baz")))));
        assertThat(topicConfigsWatcher.watching("baz"), is(true));
        assertThat(topicWatcher.watching("baz"), is(true));
    }

    @Test
    public void testTopicConfigChange() {
        // First add a topic
        addTopic();

        // Now change the config
        operator.clearEvents();
        mockZk.triggerData("/config/topics/baz", Future.succeededFuture(new byte[0]));
        assertThat(operator.getMockOperatorEvents(),
                is(singletonList(new MockTopicOperator.MockOperatorEvent(Type.MODIFY_CONFIG, new TopicName("baz")))));

        operator.clearEvents();
        mockZk.triggerData("/brokers/topics/baz", Future.succeededFuture(new byte[0]));
        assertThat(operator.getMockOperatorEvents(),
                is(singletonList(new MockTopicOperator.MockOperatorEvent(Type.MODIFY_PARTITIONS, new TopicName("baz")))));
    }

    @Test
    public void testTopicDelete() {
        operator.topicDeletedResult = Future.succeededFuture();
        mockZk.childrenResult = Future.succeededFuture(asList("foo", "bar"));

        TopicConfigsWatcher topicConfigsWatcher = new TopicConfigsWatcher(operator);
        ZkTopicWatcher topicWatcher = new ZkTopicWatcher(operator);
        ZkTopicsWatcher topicsWatcher = new ZkTopicsWatcher(operator, topicConfigsWatcher, topicWatcher);
        topicsWatcher.start(mockZk);
        mockZk.triggerChildren(Future.succeededFuture(asList("foo")));

        assertThat(operator.getMockOperatorEvents(), is(asList(new MockTopicOperator.MockOperatorEvent(
                Type.DELETE, new TopicName("bar")))));
        assertThat(topicConfigsWatcher.watching("baz"), is(false));
    }
}
