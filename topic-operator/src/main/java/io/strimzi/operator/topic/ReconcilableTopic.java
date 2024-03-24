/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.micrometer.core.instrument.Timer;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.operator.common.Reconciliation;

/**
 * A topic to be reconciled
 */
public class ReconcilableTopic {
    private Reconciliation reconciliation;
    private KafkaTopic kt;
    private String topicName;
    private Timer.Sample sample;

    /**
     * @param reconciliation The reconciliation
     * @param kt The topic
     * @param topicName The name of the topic in Kafka (spec.topicName, or metadata.name)
     */
    public ReconcilableTopic(Reconciliation reconciliation, KafkaTopic kt, String topicName) {
        this.reconciliation = reconciliation;
        this.kt = kt;
        this.topicName = topicName;
    }

    /**
     * Returns the reconciliation.
     * @return Reconciliation.
     */
    public Reconciliation reconciliation() {
        return reconciliation;
    }

    /**
     * Returns the Kafka topic.
     * @return Kafka topic.
     */
    public KafkaTopic kt() {
        return kt;
    }

    /**
     * Returns the topic name.
     * @return Topic name
     */
    public String topicName() {
        return topicName;
    }

    /**
     * @return The timer sample
     */
    public Timer.Sample reconciliationTimerSample() {
        return sample;
    }

    /**
     * Store the timer sample
     * @param sample The timer sample
     */
    public void reconciliationTimerSample(Timer.Sample sample) {
        this.sample = sample;
    }

    @Override
    public String toString() {
        return "ReconcilableTopic{" +
                "reconciliation=" + reconciliation +
                '}';
    }
}
