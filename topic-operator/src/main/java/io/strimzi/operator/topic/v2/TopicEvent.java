/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.v2;

import io.strimzi.api.kafka.model.KafkaTopic;

import java.util.Objects;

/**
 * Represents a change in Kube relating to a KafkaTopic
 */
sealed interface TopicEvent permits TopicUpsert, TopicDelete {
    long ageNs();
    String namespace();
    String name();
    String resourceVersion();

    default KubeRef toRef() {
        return new KubeRef(namespace(), name(), 0);
    }
}
/**
 * The creation or update of a KafkaTopic.
 * Note that this may include the change which adds a metadata.deletionTimestamp.
 */
record TopicUpsert(long nanos, String namespace, String name, String resourceVersion) implements TopicEvent {
    @Override
    public long ageNs() {
        return System.nanoTime() - nanos;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicUpsert that = (TopicUpsert) o;
        return Objects.equals(namespace, that.namespace) && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, name);
    }

    @Override
    public String toString() {
        return "TopicUpsert{" +
                "namespace='" + namespace + '\'' +
                ", name='" + name + '\'' +
                ", resourceVersion='" + resourceVersion + '\'' +
                ", ageNs='" + ageNs() + '\'' +
                '}';
    }
}
/**
 * The deletion of a KafkaTopic
 */
record TopicDelete(long nanos, KafkaTopic topic) implements TopicEvent {
    @Override
    public String namespace() {
        return topic.getMetadata().getNamespace();
    }

    @Override
    public String name() {
        return topic.getMetadata().getName();
    }

    @Override
    public String resourceVersion() {
        return topic.getMetadata().getResourceVersion();
    }

    @Override
    public long ageNs() {
        return System.nanoTime() - nanos;
    }

    @Override
    public String toString() {
        return "TopicDelete{" +
                "namespace='" + namespace() + '\'' +
                ", name='" + name() + '\'' +
                ", resourceVersion='" + resourceVersion() + '\'' +
                ", ageNs='" + ageNs() + '\'' +
                '}';
    }

}
