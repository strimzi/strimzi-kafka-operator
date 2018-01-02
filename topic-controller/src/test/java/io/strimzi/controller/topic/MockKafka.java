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
import org.apache.kafka.clients.admin.NewTopic;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static io.vertx.core.Future.*;

public class MockKafka implements Kafka {

    private Map<TopicName, Topic> topics = new HashMap<>();

    private AsyncResult<Set<String>> topicsListResponse = Future.succeededFuture(Collections.emptySet());
    private Function<TopicName, AsyncResult<TopicMetadata>> topicMetadataRespose =
            t -> failedFuture("Unexpected. Your test probably need to configure the MockKafka with a topicMetadataResponse.");
    private Function<String, AsyncResult<Void>> createTopicResponse =
            t -> failedFuture("Unexpected. Your test probably need to configure the MockKafka with a createTopicResponse.");
    private Function<TopicName, AsyncResult<Void>> deleteTopicResponse =
            t -> failedFuture("Unexpected. Your test probably need to configure the MockKafka with a deleteTopicResponse.");
    private Function<TopicName, AsyncResult<Void>> updateTopicResponse =
            t -> failedFuture("Unexpected. Your test probably need to configure the MockKafka with a updateTopicResponse.");

    public MockKafka setTopicsListResponse(AsyncResult<Set<String>> topicsListResponse) {
        this.topicsListResponse = topicsListResponse;
        return this;
    }

    public MockKafka setTopicsList(Set<String> topicsList) {
        this.topicsListResponse = Future.succeededFuture(topicsList);
        return this;
    }

    public MockKafka setTopicMetadataResponse(Function<TopicName, AsyncResult<TopicMetadata>> topicMetadataRespose) {
        this.topicMetadataRespose = topicMetadataRespose;
        return this;
    }

    public MockKafka setTopicMetadataResponse(TopicName topic, TopicMetadata topicMetadata, Exception exception) {
        Function<TopicName, AsyncResult<TopicMetadata>> old = this.topicMetadataRespose;
        this.topicMetadataRespose = t -> {
            if (t.equals(topic)) {
                if (exception != null) {
                    return failedFuture(exception);
                } else {
                    return succeededFuture(topicMetadata);
                }
            } else {
                return old.apply(t);
            }
        };
        return this;
    }

    public MockKafka setCreateTopicResponse(Function<String, AsyncResult<Void>> createTopicResponse) {
        this.createTopicResponse = createTopicResponse;
        return this;
    }

    public MockKafka setCreateTopicResponse(String createTopic, Exception exception) {
        Function<String, AsyncResult<Void>> old = this.createTopicResponse;
        this.createTopicResponse = t -> {
            if (t.equals(createTopic)) {
                if (exception != null) {
                    return failedFuture(exception);
                } else {
                    return succeededFuture();
                }
            } else {
                return old.apply(t);
            }
        };
        return this;
    }

    public MockKafka setDeleteTopicResponse(Function<TopicName, AsyncResult<Void>> deleteTopicResponse) {
        this.deleteTopicResponse = deleteTopicResponse;
        return this;
    }

    public MockKafka setDeleteTopicResponse(TopicName topic, Exception exception) {
        Function<TopicName, AsyncResult<Void>> old = this.deleteTopicResponse;
        this.deleteTopicResponse = t -> {
            if (t.equals(topic)) {
                if (exception != null) {
                    return failedFuture(exception);
                } else {
                    return succeededFuture();
                }
            } else {
                return old.apply(t);
            }
        };
        return this;
    }

    @Override
    public void createTopic(Topic t, Handler<AsyncResult<Void>> handler) {
        NewTopic newTopic = TopicSerialization.toNewTopic(t, null);
        AsyncResult<Void> event = createTopicResponse.apply(newTopic.name());
        if (event.succeeded()) {
            Topic.Builder topicBuilder = new Topic.Builder()
                    .withTopicName(newTopic.name())
                    .withNumPartitions(newTopic.numPartitions())
                    .withNumReplicas(newTopic.replicationFactor());
            try {
                Field field = NewTopic.class.getDeclaredField("configs");
                field.setAccessible(true);
                topicBuilder.withConfig((Map) field.get(newTopic));
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
            Topic topic = topicBuilder.build();
            topics.put(topic.getTopicName(), topic);
        }
        handler.handle(event);
    }

    @Override
    public void deleteTopic(TopicName topicName, Handler<AsyncResult<Void>> handler) {
        AsyncResult<Void> event = deleteTopicResponse.apply(topicName);
        if (event.succeeded()) {
            topics.remove(topicName);
        }
        handler.handle(event);
    }

    public MockKafka setUpdateTopicResponse(Function<TopicName, AsyncResult<Void>> updateTopicResponse) {
        this.updateTopicResponse = updateTopicResponse;
        return this;
    }

    @Override
    public void updateTopicConfig(Topic topic, Handler<AsyncResult<Void>> handler) {
        AsyncResult<Void> event = updateTopicResponse.apply(topic.getTopicName());
        if (event.succeeded()) {
            Topic t = topics.get(topic.getTopicName());
            if (t == null) {
                event = Future.failedFuture("No such topic " + topic.getTopicName());
            }
            t = new Topic.Builder(t).withConfig(topic.getConfig()).build();
            topics.put(topic.getTopicName(), t);
        }
        handler.handle(event);
    }

    @Override
    public void increasePartitions(Topic topic, Handler<AsyncResult<Void>> handler) {
        AsyncResult<Void> event = updateTopicResponse.apply(topic.getTopicName());
        if (event.succeeded()) {
            Topic t = topics.get(topic.getTopicName());
            if (t == null) {
                event = Future.failedFuture("No such topic " + topic.getTopicName());
            }
            t = new Topic.Builder(t).withNumPartitions(topic.getNumPartitions()).build();
            topics.put(topic.getTopicName(), t);
        }
        handler.handle(event);
    }

    @Override
    public void changeReplicationFactor(Topic topic, Handler<AsyncResult<Void>> handler) {
        AsyncResult<Void> event = updateTopicResponse.apply(topic.getTopicName());
        if (event.succeeded()) {
            Topic t = topics.get(topic.getTopicName());
            if (t == null) {
                event = Future.failedFuture("No such topic " + topic.getTopicName());
            }
            t = new Topic.Builder(t).withNumReplicas(topic.getNumReplicas()).build();
            topics.put(topic.getTopicName(), t);
        }
        handler.handle(event);
    }

    @Override
    public void topicMetadata(TopicName topicName, Handler<AsyncResult<TopicMetadata>> handler) {
        handler.handle(topicMetadataRespose.apply(topicName));
    }

    @Override
    public void listTopics(Handler<AsyncResult<Set<String>>> handler) {
        handler.handle(topicsListResponse);
    }

    public void assertExists(TestContext context, TopicName topicName) {
        context.assertTrue(topics.containsKey(topicName));
    }

    public void assertNotExists(TestContext context, TopicName topicName) {
        context.assertFalse(topics.containsKey(topicName));
    }

    public void assertEmpty(TestContext context) {
        context.assertTrue(topics.isEmpty());
    }

    public void assertContains(TestContext context, Topic topic) {
        context.assertEquals(topic, topics.get(topic.getTopicName()));
    }

    public Topic getTopicState(TopicName topicName) {
        return topics.get(topicName);
    }
}
