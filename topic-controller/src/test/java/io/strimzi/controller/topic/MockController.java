/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.topic;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.util.ArrayList;
import java.util.List;

class MockController extends Controller {

    public MockController() {
        super(null, null, null, null, null, null);
    }

    static class MockControllerEvent {
        private final MockControllerEvent.Type type;

        static enum Type {
            CREATE,
            DELETE,
            MODIFY,
            MODIFY_CONFIG,
            MODIFY_PARTITIONS
        }
        private final TopicName topicName;
        private final ConfigMap configMap;

        public MockControllerEvent(MockControllerEvent.Type type, TopicName topicName) {
            this.type = type;
            this.topicName = topicName;
            this.configMap = null;
        }

        public MockControllerEvent(MockControllerEvent.Type type, ConfigMap configMap) {
            this.type = type;
            this.topicName = null;
            this.configMap = configMap;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MockControllerEvent mockControllerEvent = (MockControllerEvent) o;

            if (type != mockControllerEvent.type) return false;
            if (topicName != null ? !topicName.equals(mockControllerEvent.topicName) : mockControllerEvent.topicName != null)
                return false;
            return configMap != null ? configMap.equals(mockControllerEvent.configMap) : mockControllerEvent.configMap == null;
        }

        @Override
        public int hashCode() {
            int result = type.hashCode();
            result = 31 * result + (topicName != null ? topicName.hashCode() : 0);
            result = 31 * result + (configMap != null ? configMap.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Event{" +
                    "type=" + type +
                    ", topicName=" + topicName +
                    ", configMap=" + configMap +
                    '}';
        }
    }

    public AsyncResult<Void> topicCreatedResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName() + ".topicCreatedResult");
    public AsyncResult<Void> topicDeletedResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName() + ".topicDeletedResult");
    public AsyncResult<Void> topicModifiedResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName() + ".topicModifiedResult");
    public AsyncResult<Void> cmAddedResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName() + ".cmAddedResult");
    public AsyncResult<Void> cmDeletedResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName() + ".cmDeletedResult");
    public AsyncResult<Void> cmModifiedResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName() + ".cmModifiedResult");
    private List<MockControllerEvent> mockControllerEvents = new ArrayList<>();

    public List<MockControllerEvent> getMockControllerEvents() {
        return mockControllerEvents;
    }

    public void clearEvents() {
        mockControllerEvents.clear();
    }

    @Override
    public void onTopicCreated(TopicName topicName, Handler<AsyncResult<Void>> handler) {
        mockControllerEvents.add(new MockControllerEvent(MockControllerEvent.Type.CREATE, topicName));
        handler.handle(topicCreatedResult);
    }

    @Override
    public void onTopicDeleted(TopicName topicName, Handler<AsyncResult<Void>> handler) {
        mockControllerEvents.add(new MockControllerEvent(MockControllerEvent.Type.DELETE, topicName));
        handler.handle(topicDeletedResult);
    }

    @Override
    public void onTopicConfigChanged(TopicName topicName, Handler<AsyncResult<Void>> handler) {
        mockControllerEvents.add(new MockControllerEvent(MockControllerEvent.Type.MODIFY_CONFIG, topicName));
        handler.handle(topicModifiedResult);
    }

    @Override
    public void onTopicPartitionsChanged(TopicName topicName, Handler<AsyncResult<Void>> handler) {
        mockControllerEvents.add(new MockControllerEvent(MockControllerEvent.Type.MODIFY_PARTITIONS, topicName));
        handler.handle(topicModifiedResult);
    }

    @Override
    public void onConfigMapAdded(ConfigMap cm, Handler<AsyncResult<Void>> handler) {
        mockControllerEvents.add(new MockControllerEvent(MockControllerEvent.Type.CREATE, cm));
        handler.handle(cmAddedResult);
    }

    @Override
    public void onConfigMapModified(ConfigMap cm, Handler<AsyncResult<Void>> handler) {
        mockControllerEvents.add(new MockControllerEvent(MockControllerEvent.Type.MODIFY, cm));
        handler.handle(cmModifiedResult);
    }

    @Override
    public void onConfigMapDeleted(ConfigMap cm, Handler<AsyncResult<Void>> handler) {
        mockControllerEvents.add(new MockControllerEvent(MockControllerEvent.Type.DELETE, cm));
        handler.handle(cmDeletedResult);
    }
}
