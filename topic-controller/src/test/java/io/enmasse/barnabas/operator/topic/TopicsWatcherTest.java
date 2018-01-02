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

import io.vertx.core.Future;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import static java.util.Arrays.asList;

@RunWith(VertxUnitRunner.class)
public class TopicsWatcherTest {

    @Test
    public void testTopicAdd() {
        MockOperator op = new MockOperator();
        op.topicCreatedResult = Future.succeededFuture();
        MockZk mockZk = new MockZk();
        mockZk.childrenResult = Future.succeededFuture(asList("foo", "bar"));
        TopicsWatcher tw = new TopicsWatcher(op);
        tw.start(mockZk);
        mockZk.triggerChildren(Future.succeededFuture(asList("foo", "bar", "baz")));
        Assert.assertEquals(asList(new MockOperator.Event(
                MockOperator.Event.Type.CREATE, new TopicName("baz"))), op.getEvents());
    }

    @Test
    public void testTopicDelete() {
        MockOperator op = new MockOperator();
        op.topicDeletedResult = Future.succeededFuture();
        MockZk mockZk = new MockZk();
        mockZk.childrenResult = Future.succeededFuture(asList("foo", "bar"));
        TopicsWatcher tw = new TopicsWatcher(op);
        tw.start(mockZk);
        mockZk.triggerChildren(Future.succeededFuture(asList("foo")));
        Assert.assertEquals(asList(new MockOperator.Event(
                MockOperator.Event.Type.DELETE, new TopicName("bar"))), op.getEvents());
    }
}
