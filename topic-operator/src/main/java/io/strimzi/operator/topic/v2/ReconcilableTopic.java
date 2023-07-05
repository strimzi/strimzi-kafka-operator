/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.v2;

import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.operator.common.Reconciliation;

/**
 * A topic to be reconciled
 * @param reconciliation The reconciliation
 * @param kt The topic
 * @param topicName The name of the topic in Kafka (spec.topicName, or metadata.name)
 */
record ReconcilableTopic(Reconciliation reconciliation, KafkaTopic kt, String topicName) {

    @Override
    public String toString() {
        return "ReconcilableTopic{" +
                "reconciliation=" + reconciliation +
                '}';
    }
}
