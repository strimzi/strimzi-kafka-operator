/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.Watcher;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.vertx.core.Future;

import java.util.ArrayList;
import java.util.List;

class MockTopicOperator extends TopicOperator {

    public MockTopicOperator() {
        super(null, null, null, null, null, null, null, null);
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

    public Future<Void> topicCreatedResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName() + ".topicCreatedResult");
    public Future<Void> topicDeletedResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName() + ".topicDeletedResult");
    public Future<Void> topicModifiedResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName() + ".topicModifiedResult");
    public Future<Void> resourceAddedResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName() + ".resourceAddedResult");
    private List<MockOperatorEvent> mockOperatorEvents = new ArrayList<>();

    public List<MockOperatorEvent> getMockOperatorEvents() {
        return mockOperatorEvents;
    }

    public void clearEvents() {
        mockOperatorEvents.clear();
    }

    @Override
    protected void initMetrics() {
        return;
    }

    @Override
    public Future<Void> onTopicCreated(LogContext logContext, TopicName topicName) {
        mockOperatorEvents.add(new MockOperatorEvent(MockOperatorEvent.Type.CREATE, topicName));
        return topicCreatedResult;
    }

    @Override
    public Future<Void> onTopicDeleted(LogContext logContext, TopicName topicName) {
        mockOperatorEvents.add(new MockOperatorEvent(MockOperatorEvent.Type.DELETE, topicName));
        return topicDeletedResult;
    }

    @Override
    public Future<Void> onTopicConfigChanged(LogContext logContext, TopicName topicName) {
        mockOperatorEvents.add(new MockOperatorEvent(MockOperatorEvent.Type.MODIFY_CONFIG, topicName));
        return topicModifiedResult;
    }

    @Override
    public Future<Void> onTopicPartitionsChanged(LogContext logContext, TopicName topicName) {
        mockOperatorEvents.add(new MockOperatorEvent(MockOperatorEvent.Type.MODIFY_PARTITIONS, topicName));
        return topicModifiedResult;
    }

    @Override
    public Future<Void> onResourceEvent(LogContext logContext, KafkaTopic resource, Watcher.Action action) {
        mockOperatorEvents.add(new MockOperatorEvent(action == Watcher.Action.MODIFIED ? MockOperatorEvent.Type.MODIFY :
                action == Watcher.Action.ADDED ? MockOperatorEvent.Type.CREATE :
                MockOperatorEvent.Type.DELETE, resource));
        return resourceAddedResult;
    }
}
