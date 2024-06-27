/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.model;

import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.operator.common.Reconciliation;

import java.util.Objects;

/**
 * A topic to be reconciled.
 */
public class ReconcilableTopic {
    private Reconciliation reconciliation;
    private KafkaTopic kt;
    private String topicName;

    /**
     * @param reconciliation The reconciliation.
     * @param kt The topic.
     * @param topicName The name of the topic in Kafka (spec.topicName, or metadata.name).
     */
    public ReconcilableTopic(Reconciliation reconciliation, KafkaTopic kt, String topicName) {
        this.reconciliation = reconciliation;
        this.kt = kt;
        this.topicName = topicName;
    }

    /**
     * @return Reconciliation.
     */
    public Reconciliation reconciliation() {
        return reconciliation;
    }

    /**
     * @return Kafka topic.
     */
    public KafkaTopic kt() {
        return kt;
    }

    /**
     * @return Topic name.
     */
    public String topicName() {
        return topicName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReconcilableTopic that = (ReconcilableTopic) o;
        return Objects.equals(reconciliation, that.reconciliation) && Objects.equals(topicName, that.topicName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(reconciliation, topicName);
    }

    @Override
    public String toString() {
        return "ReconcilableTopic{" +
                "reconciliation=" + reconciliation +
                '}';
    }
}
