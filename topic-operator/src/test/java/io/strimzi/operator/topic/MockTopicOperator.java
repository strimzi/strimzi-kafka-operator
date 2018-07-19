/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.api.kafka.model.Topic;
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
        private final Topic topicResource;

        public MockOperatorEvent(MockOperatorEvent.Type type, TopicName topicName) {
            this.type = type;
            this.topicName = topicName;
            this.topicResource = null;
        }

        public MockOperatorEvent(MockOperatorEvent.Type type, Topic topicResource) {
            this.type = type;
            this.topicName = null;
            this.topicResource = topicResource;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MockOperatorEvent mockOperatorEvent = (MockOperatorEvent) o;

            if (type != mockOperatorEvent.type) return false;
            if (topicName != null ? !topicName.equals(mockOperatorEvent.topicName) : mockOperatorEvent.topicName != null)
                return false;
            return topicResource != null ? topicResource.equals(mockOperatorEvent.topicResource) : mockOperatorEvent.topicResource == null;
        }

        @Override
        public int hashCode() {
            int result = type.hashCode();
            result = 31 * result + (topicName != null ? topicName.hashCode() : 0);
            result = 31 * result + (topicResource != null ? topicResource.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Event{" +
                    "type=" + type +
                    ", topicName=" + topicName +
                    ", configMap=" + topicResource +
                    '}';
        }
    }

    public AsyncResult<Void> topicCreatedResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName() + ".topicCreatedResult");
    public AsyncResult<Void> topicDeletedResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName() + ".topicDeletedResult");
    public AsyncResult<Void> topicModifiedResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName() + ".topicModifiedResult");
    public AsyncResult<Void> resourceAddedResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName() + ".cmAddedResult");
    public AsyncResult<Void> resourceDeletedResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName() + ".cmDeletedResult");
    public AsyncResult<Void> resourceModifiedResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName() + ".cmModifiedResult");
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
    public void onConfigMapAdded(Topic cm, Handler<AsyncResult<Void>> resultHandler) {
        mockOperatorEvents.add(new MockOperatorEvent(MockOperatorEvent.Type.CREATE, cm));
        resultHandler.handle(resourceAddedResult);
    }

    @Override
    public void onConfigMapModified(Topic cm, Handler<AsyncResult<Void>> resultHandler) {
        mockOperatorEvents.add(new MockOperatorEvent(MockOperatorEvent.Type.MODIFY, cm));
        resultHandler.handle(resourceModifiedResult);
    }

    @Override
    public void onConfigMapDeleted(Topic cm, Handler<AsyncResult<Void>> resultHandler) {
        mockOperatorEvents.add(new MockOperatorEvent(MockOperatorEvent.Type.DELETE, cm));
        resultHandler.handle(resourceDeletedResult);
    }
}
