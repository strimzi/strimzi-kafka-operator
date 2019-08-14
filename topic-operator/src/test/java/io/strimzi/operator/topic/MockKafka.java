/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.vertx.core.Future;
import io.vertx.ext.unit.TestContext;
import org.apache.kafka.clients.admin.NewTopic;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static java.lang.Integer.min;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class MockKafka implements Kafka {

    private Map<TopicName, Topic> topics = new HashMap<>();

    private Future<Set<String>> topicsListResponse = Future.succeededFuture(Collections.emptySet());
    private int topicMetadataResposeCall = 0;
    private List<Function<TopicName, Future<TopicMetadata>>> topicMetadataRespose = singletonList(
        t -> failedFuture("Unexpected. Your test probably need to configure the MockKafka with a topicMetadataResponse."));
    private Function<String, Future<Void>> createTopicResponse =
        t -> failedFuture("Unexpected. Your test probably need to configure the MockKafka with a createTopicResponse.");
    private Function<TopicName, Future<Void>> deleteTopicResponse =
        t -> failedFuture("Unexpected. Your test probably need to configure the MockKafka with a deleteTopicResponse.");
    private Function<TopicName, Future<Void>> updateTopicResponse =
        t -> failedFuture("Unexpected. Your test probably need to configure the MockKafka with a updateTopicResponse.");

    public MockKafka setTopicsListResponse(Future<Set<String>> topicsListResponse) {
        this.topicsListResponse = topicsListResponse;
        return this;
    }

    public MockKafka setTopicsList(Set<String> topicsList) {
        this.topicsListResponse = Future.succeededFuture(topicsList);
        return this;
    }

    public MockKafka setTopicMetadataResponse(Function<TopicName, Future<TopicMetadata>> topicMetadataRespose) {
        this.topicMetadataRespose = singletonList(topicMetadataRespose);
        return this;
    }

    public MockKafka setTopicMetadataResponses(Function<TopicName, Future<TopicMetadata>>... topicMetadataRespose) {
        this.topicMetadataRespose = asList(topicMetadataRespose);
        this.topicMetadataResposeCall = 0;
        return this;
    }

    public MockKafka setTopicMetadataResponse(TopicName topic, TopicMetadata topicMetadata, Exception exception) {
        Function<TopicName, Future<TopicMetadata>> old = getTopicNameFutureFunction();
        this.topicMetadataRespose = singletonList(t -> {
            if (t.equals(topic)) {
                if (exception != null) {
                    return failedFuture(exception);
                } else {
                    return succeededFuture(topicMetadata);
                }
            } else {
                return old.apply(t);
            }
        });
        return this;
    }

    public MockKafka setCreateTopicResponse(Function<String, Future<Void>> createTopicResponse) {
        this.createTopicResponse = createTopicResponse;
        return this;
    }

    public MockKafka setCreateTopicResponse(String createTopic, Exception exception) {
        Function<String, Future<Void>> old = this.createTopicResponse;
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

    public MockKafka setDeleteTopicResponse(Function<TopicName, Future<Void>> deleteTopicResponse) {
        this.deleteTopicResponse = deleteTopicResponse;
        return this;
    }

    public MockKafka setDeleteTopicResponse(TopicName topic, Exception exception) {
        Function<TopicName, Future<Void>> old = this.deleteTopicResponse;
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
    public Future<Void> createTopic(Topic t) {
        NewTopic newTopic = TopicSerialization.toNewTopic(t, null);
        Future<Void> event = createTopicResponse.apply(newTopic.name());
        if (event.succeeded()) {
            Topic.Builder topicBuilder = new Topic.Builder()
                    .withTopicName(newTopic.name())
                    .withNumPartitions(newTopic.numPartitions())
                    .withNumReplicas(newTopic.replicationFactor())
                    .withMetadata(t.getMetadata());
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
        return event;
    }

    @Override
    public Future<Void> deleteTopic(TopicName topicName) {
        Future<Void> event = deleteTopicResponse.apply(topicName);
        if (event.succeeded()) {
            topics.remove(topicName);
        }
        return event;
    }

    public MockKafka setUpdateTopicResponse(Function<TopicName, Future<Void>> updateTopicResponse) {
        this.updateTopicResponse = updateTopicResponse;
        return this;
    }

    @Override
    public Future<Void> updateTopicConfig(Topic topic) {
        Future<Void> event = updateTopicResponse.apply(topic.getTopicName());
        if (event.succeeded()) {
            Topic t = topics.get(topic.getTopicName());
            if (t == null) {
                event = failedFuture("No such topic " + topic.getTopicName());
            }
            t = new Topic.Builder(t).withConfig(topic.getConfig()).build();
            topics.put(topic.getTopicName(), t);
        }
        return event;
    }

    @Override
    public Future<Void> increasePartitions(Topic topic) {
        Future<Void> event = updateTopicResponse.apply(topic.getTopicName());
        if (event.succeeded()) {
            Topic t = topics.get(topic.getTopicName());
            if (t == null) {
                event = failedFuture("No such topic " + topic.getTopicName());
            }
            t = new Topic.Builder(t).withNumPartitions(topic.getNumPartitions()).build();
            topics.put(topic.getTopicName(), t);
        }
        return event;
    }

    @Override
    public Future<Void> changeReplicationFactor(Topic topic) {
        Future<Void> event = updateTopicResponse.apply(topic.getTopicName());
        if (event.succeeded()) {
            Topic t = topics.get(topic.getTopicName());
            if (t == null) {
                event = failedFuture("No such topic " + topic.getTopicName());
            }
            t = new Topic.Builder(t).withNumReplicas(topic.getNumReplicas()).build();
            topics.put(topic.getTopicName(), t);
        }
        return event;
    }

    @Override
    public Future<TopicMetadata> topicMetadata(TopicName topicName) {
        return getTopicNameFutureFunction().apply(topicName);
    }

    Function<TopicName, Future<TopicMetadata>> getTopicNameFutureFunction() {
        return topicMetadataRespose.get(min(topicMetadataResposeCall++, topicMetadataRespose.size() - 1));
    }

    @Override
    public Future<Set<String>> listTopics() {
        return topicsListResponse;
    }

    public void assertExists(TestContext context, TopicName topicName) {
        context.assertTrue(topics.containsKey(topicName), "The topic "  + topicName + " should exist in " + this);
    }

    public void assertNotExists(TestContext context, TopicName topicName) {
        context.assertFalse(topics.containsKey(topicName), "The topic "  + topicName + " should not exist in " + this);
    }

    public void assertEmpty(TestContext context) {
        context.assertTrue(topics.isEmpty(), "No topics should exist in " + this);
    }

    public void assertContains(TestContext context, Topic topic) {
        context.assertEquals(topic, topics.get(topic.getTopicName()), "The topic " + topic.getTopicName() + " either didn't exist, or had unexpected state");
    }

    public Topic getTopicState(TopicName topicName) {
        return topics.get(topicName);
    }
}
