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

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.unit.TestContext;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class MockTopicStore implements TopicStore {

    private Map<TopicName, Topic> topics = new HashMap<>();
    private Function<TopicName, AsyncResult<Void>> createTopicResponse = t -> Future.failedFuture("Unexpected. Your test's MockTopicStore probably nees a createTopicResponse configured.");
    private Function<TopicName, AsyncResult<Void>> deleteTopicResponse = t -> Future.failedFuture("Unexpected. Your test's MockTopicStore probably nees a deleteTopicResponse configured.");
    private Function<TopicName, AsyncResult<Void>> updateTopicResponse = t -> Future.failedFuture("Unexpected. Your test's MockTopicStore probably nees a updateTopicResponse configured.");

    @Override
    public void read(TopicName name, Handler<AsyncResult<Topic>> handler) {
        Topic result = topics.get(name);
        handler.handle(Future.succeededFuture(result));
    }

    @Override
    public void create(Topic topic, Handler<AsyncResult<Void>> handler) {
        AsyncResult<Void> response = createTopicResponse.apply(topic.getTopicName());
        if (response.succeeded()) {
            Topic old = topics.put(topic.getTopicName(), topic);
            if (old != null) {
                handler.handle(Future.failedFuture(new TopicStore.EntityExistsException()));
            }
        }
        handler.handle(response);
    }

    @Override
    public void update(Topic topic, Handler<AsyncResult<Void>> handler) {
        Topic old = topics.put(topic.getTopicName(), topic);
        if (old != null) {
            handler.handle(Future.succeededFuture());
        } else {
            handler.handle(Future.failedFuture(new TopicStore.NoSuchEntityExistsException()));
        }
    }

    @Override
    public void delete(TopicName topicName, Handler<AsyncResult<Void>> handler) {
        AsyncResult<Void> response = deleteTopicResponse.apply(topicName);
        if (response.succeeded()) {
            Topic topic = topics.remove(topicName);
            if (topic == null) {
                handler.handle(Future.failedFuture(new TopicStore.NoSuchEntityExistsException()));
            }
        }
        handler.handle(response);
    }

    public void assertExists(TestContext context, TopicName topicName) {
        context.assertTrue(topics.containsKey(topicName));
    }

    public void assertNotExists(TestContext context, TopicName topicName) {
        context.assertFalse(topics.containsKey(topicName));
    }

    public void assertContains(TestContext context, Topic topic) {
        context.assertEquals(topic, topics.get(topic.getTopicName()));
    }

    public MockTopicStore setCreateTopicResponse(TopicName createTopic, Exception exception) {
        Function<TopicName, AsyncResult<Void>> old = this.createTopicResponse;
        this.createTopicResponse = t -> {
            if (t.equals(createTopic)) {
                if (exception != null) {
                    return Future.failedFuture(exception);
                } else {
                    return Future.succeededFuture();
                }
            } else {
                return old.apply(t);
            }
        };
        return this;
    }

    public MockTopicStore setDeleteTopicResponse(TopicName createTopic, Exception exception) {
        Function<TopicName, AsyncResult<Void>> old = this.deleteTopicResponse;
        this.deleteTopicResponse = t -> {
            if (t.equals(createTopic)) {
                if (exception != null) {
                    return Future.failedFuture(exception);
                } else {
                    return Future.succeededFuture();
                }
            } else {
                return old.apply(t);
            }
        };
        return this;
    }

    public MockTopicStore setUpdateTopicResponse(TopicName updateTopic, Exception exception) {
        Function<TopicName, AsyncResult<Void>> old = this.updateTopicResponse;
        this.updateTopicResponse = t -> {
            if (t.equals(updateTopic)) {
                if (exception != null) {
                    return Future.failedFuture(exception);
                } else {
                    return Future.succeededFuture();
                }
            } else {
                return old.apply(t);
            }
        };
        return this;
    }

    public void assertEmpty(TestContext context) {
        context.assertTrue(this.topics.isEmpty());
    }
}
