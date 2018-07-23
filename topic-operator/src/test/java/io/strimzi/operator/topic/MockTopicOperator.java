/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.api.kafka.model.KafkaTopic;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.util.ArrayList;
import java.util.List;

class MockTopicOperator extends TopicOperator {

    public MockTopicOperator() {
        super(null, null, null, null, null, null, null);
    }

    static class MockOperatorEvent {
        private final MockOperatorEvent.Type type;

        static enum Type {
            CREATE,
            DELETE,
            MODIFY,
            MODIFY_CONFIG,
            MODIFY_PARTITIONS
        }
        private final TopicName topicName;
        private final KafkaTopic kafkaTopicResource;

        public MockOperatorEvent(MockOperatorEvent.Type type, TopicName topicName) {
            this.type = type;
            this.topicName = topicName;
            this.kafkaTopicResource = null;
        }

        public MockOperatorEvent(MockOperatorEvent.Type type, KafkaTopic kafkaTopicResource) {
            this.type = type;
            this.topicName = null;
            this.kafkaTopicResource = kafkaTopicResource;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MockOperatorEvent mockOperatorEvent = (MockOperatorEvent) o;

            if (type != mockOperatorEvent.type) return false;
            if (topicName != null ? !topicName.equals(mockOperatorEvent.topicName) : mockOperatorEvent.topicName != null)
                return false;
            return kafkaTopicResource != null ? kafkaTopicResource.equals(mockOperatorEvent.kafkaTopicResource) : mockOperatorEvent.kafkaTopicResource == null;
        }

        @Override
        public int hashCode() {
            int result = type.hashCode();
            result = 31 * result + (topicName != null ? topicName.hashCode() : 0);
            result = 31 * result + (kafkaTopicResource != null ? kafkaTopicResource.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Event{" +
                    "type=" + type +
                    ", topicName=" + topicName +
                    ", kafkaTopicResource=" + kafkaTopicResource +
                    '}';
        }
    }

    public AsyncResult<Void> topicCreatedResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName() + ".topicCreatedResult");
    public AsyncResult<Void> topicDeletedResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName() + ".topicDeletedResult");
    public AsyncResult<Void> topicModifiedResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName() + ".topicModifiedResult");
    public AsyncResult<Void> resourceAddedResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName() + ".resourceAddedResult");
    public AsyncResult<Void> resourceDeletedResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName() + ".resourceDeletedResult");
    public AsyncResult<Void> resourceModifiedResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName() + ".resourceModifiedResult");
    private List<MockOperatorEvent> mockOperatorEvents = new ArrayList<>();

    public List<MockOperatorEvent> getMockOperatorEvents() {
        return mockOperatorEvents;
    }

    public void clearEvents() {
        mockOperatorEvents.clear();
    }

    @Override
    public void onTopicCreated(TopicName topicName, Handler<AsyncResult<Void>> handler) {
        mockOperatorEvents.add(new MockOperatorEvent(MockOperatorEvent.Type.CREATE, topicName));
        handler.handle(topicCreatedResult);
    }

    @Override
    public void onTopicDeleted(TopicName topicName, Handler<AsyncResult<Void>> resultHandler) {
        mockOperatorEvents.add(new MockOperatorEvent(MockOperatorEvent.Type.DELETE, topicName));
        resultHandler.handle(topicDeletedResult);
    }

    @Override
    public void onTopicConfigChanged(TopicName topicName, Handler<AsyncResult<Void>> handler) {
        mockOperatorEvents.add(new MockOperatorEvent(MockOperatorEvent.Type.MODIFY_CONFIG, topicName));
        handler.handle(topicModifiedResult);
    }

    @Override
    public void onTopicPartitionsChanged(TopicName topicName, Handler<AsyncResult<Void>> handler) {
        mockOperatorEvents.add(new MockOperatorEvent(MockOperatorEvent.Type.MODIFY_PARTITIONS, topicName));
        handler.handle(topicModifiedResult);
    }

    @Override
    public void onResourceAdded(KafkaTopic resource, Handler<AsyncResult<Void>> resultHandler) {
        mockOperatorEvents.add(new MockOperatorEvent(MockOperatorEvent.Type.CREATE, resource));
        resultHandler.handle(resourceAddedResult);
    }

    @Override
    public void onResourceModified(KafkaTopic resource, Handler<AsyncResult<Void>> resultHandler) {
        mockOperatorEvents.add(new MockOperatorEvent(MockOperatorEvent.Type.MODIFY, resource));
        resultHandler.handle(resourceModifiedResult);
    }

    @Override
    public void onResourceDeleted(KafkaTopic resource, Handler<AsyncResult<Void>> resultHandler) {
        mockOperatorEvents.add(new MockOperatorEvent(MockOperatorEvent.Type.DELETE, resource));
        resultHandler.handle(resourceDeletedResult);
    }
}
