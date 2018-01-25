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

package io.strimzi.controller.topic;

import io.vertx.core.Future;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(VertxUnitRunner.class)
public class TopicsWatcherTest {

    private MockController controller;
    private MockZk mockZk;
    @Before
    public void setup() {
        controller = new MockController();
        mockZk = new MockZk();
    }

    @Test
    public void testTopicAdd() {
        addTopic();
    }

    private void addTopic() {
        controller = new MockController();
        controller.topicCreatedResult = Future.succeededFuture();
        mockZk = new MockZk();
        mockZk.childrenResult = Future.succeededFuture(asList("foo", "bar"));
        mockZk.dataResult = Future.succeededFuture(new byte[0]);
        TopicConfigsWatcher topicConfigsWatcher = new TopicConfigsWatcher(controller);
        TopicWatcher topicWatcher = new TopicWatcher(controller);
        TopicsWatcher topicsWatcher = new TopicsWatcher(controller, topicConfigsWatcher, topicWatcher);
        topicsWatcher.start(mockZk);
        mockZk.triggerChildren(Future.succeededFuture(asList("foo", "bar", "baz")));
        assertEquals(asList(new MockController.MockControllerEvent(
                MockController.MockControllerEvent.Type.CREATE, new TopicName("baz"))), controller.getMockControllerEvents());
        assertTrue(topicConfigsWatcher.watching("baz"));
        assertTrue(topicWatcher.watching("baz"));
    }

    @Test
    public void testTopicConfigChange() {
        // First add a topic
        addTopic();
        // Now change the config
        controller.clearEvents();
        mockZk.triggerData(Future.succeededFuture(new byte[0]));
        assertEquals(asList(
                new MockController.MockControllerEvent(MockController.MockControllerEvent.Type.MODIFY_PARTITIONS, new TopicName("baz")),
                new MockController.MockControllerEvent(MockController.MockControllerEvent.Type.MODIFY_CONFIG, new TopicName("baz"))),
                controller.getMockControllerEvents());
    }

    @Test
    public void testTopicDelete() {
        controller = new MockController();
        controller.topicDeletedResult = Future.succeededFuture();
        mockZk = new MockZk();
        mockZk.childrenResult = Future.succeededFuture(asList("foo", "bar"));
        TopicConfigsWatcher topicConfigsWatcher = new TopicConfigsWatcher(controller);
        TopicWatcher topicWatcher = new TopicWatcher(controller);
        TopicsWatcher topicsWatcher = new TopicsWatcher(controller, topicConfigsWatcher, topicWatcher);
        topicsWatcher.start(mockZk);
        mockZk.triggerChildren(Future.succeededFuture(asList("foo")));
        assertEquals(asList(new MockController.MockControllerEvent(
                MockController.MockControllerEvent.Type.DELETE, new TopicName("bar"))), controller.getMockControllerEvents());
        assertFalse(topicConfigsWatcher.watching("baz"));
    }
}
