/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.v2;

import io.micrometer.core.instrument.Timer;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.metrics.MetricsHolder;

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
     * Start the reconciliation timer.
     * @param metrics Metrics holder
     */
    public void startTimer(MetricsHolder metrics) {
        if (sample == null) {
            sample = Timer.start(metrics.metricsProvider().meterRegistry());
        }
    }

    /**
     * Stop the reconciliation timer if present.
     * @param metrics Metrics holder
     */
    public void stopTimer(MetricsHolder metrics) {
        if (sample != null) {
            sample.stop(metrics.reconciliationsTimer(reconciliation.namespace()));
        }
    }

    @Override
    public String toString() {
        return "ReconcilableTopic{" +
                "reconciliation=" + reconciliation +
                '}';
    }
}
