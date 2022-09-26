/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.vertx.core.Future;
import io.vertx.junit5.VertxTestContext;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class MockTopicStore implements TopicStore {

    private Map<TopicName, Topic> topics = new HashMap<>();
    private Function<TopicName, Future<Void>> createTopicResponse = t -> Future.failedFuture("Unexpected. Your test's MockTopicStore probably needs a createTopicResponse configured.");
    private Function<TopicName, Future<Void>> deleteTopicResponse = t -> Future.failedFuture("Unexpected. Your test's MockTopicStore probably needs a deleteTopicResponse configured.");
    private Function<TopicName, Future<Void>> updateTopicResponse = t -> Future.failedFuture("Unexpected. Your test's MockTopicStore probably needs a updateTopicResponse configured.");
    private Function<TopicName, Future<Topic>> getTopicResponse = t -> null;

    @Override
    public Future<Topic> read(TopicName name) {
        Future<Topic> result1 = getTopicResponse.apply(name);
        if (result1 == null) {
            Topic result = topics.get(name);
            return Future.succeededFuture(result);
        } else {
            return result1;
        }
    }

    @Override
    public Future<Void> create(Topic topic) {
        Future<Void> response = createTopicResponse.apply(topic.getTopicName());
        if (response.succeeded()) {
            Topic old = topics.put(topic.getTopicName(), topic);
            if (old != null) {
                return Future.failedFuture(new TopicStore.EntityExistsException());
            }
        }
        return response;
    }

    @Override
    public Future<Void> update(Topic topic) {
        Topic old = topics.put(topic.getTopicName(), topic);
        if (old != null) {
            return Future.succeededFuture();
        } else {
            return Future.failedFuture(new TopicStore.NoSuchEntityExistsException());
        }
    }

    @Override
    public Future<Void> delete(TopicName topicName) {
        Future<Void> response = deleteTopicResponse.apply(topicName);
        if (response.succeeded()) {
            Topic topic = topics.remove(topicName);
            if (topic == null) {
                return Future.failedFuture(new TopicStore.NoSuchEntityExistsException());
            }
        }
        return response;
    }

    public void assertExists(VertxTestContext context, TopicName topicName) {
        context.verify(() -> assertThat(topics.containsKey(topicName), is(true)));
    }

    public void assertNotExists(VertxTestContext context, TopicName topicName) {
        context.verify(() -> assertThat(topics.containsKey(topicName), is(false)));
    }

    public void assertContains(VertxTestContext context, Topic topic) {
        // Don't compare metadata since the topic store doesn't retain metadata, only k8s does.
        context.verify(() -> assertThat("The topic " + topic.getTopicName() + " has an unexpected state",
                new Topic.Builder(topics.get(topic.getTopicName())).withMetadata(null).build(),
                is(new Topic.Builder(topic).withMetadata(null).build())));
    }

    public MockTopicStore setCreateTopicResponse(TopicName createTopic, Exception exception) {
        Function<TopicName, Future<Void>> old = this.createTopicResponse;
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
        Function<TopicName, Future<Void>> old = this.deleteTopicResponse;
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
        Function<TopicName, Future<Void>> old = this.updateTopicResponse;
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

    public MockTopicStore setGetTopicResponse(TopicName topic, Future<Topic> f) {
        Function<TopicName, Future<Topic>> old = this.getTopicResponse;
        this.getTopicResponse = t -> {
            if (t.equals(topic)) {
                return f;
            } else {
                return old.apply(t);
            }
        };
        return this;
    }

    public void assertEmpty(VertxTestContext context) {
        context.verify(() -> assertThat(this.topics.isEmpty(), is(true)));
    }

}
