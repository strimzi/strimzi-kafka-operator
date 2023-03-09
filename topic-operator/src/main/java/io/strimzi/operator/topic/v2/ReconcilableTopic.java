/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.v2;

import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.operator.common.Reconciliation;

record ReconcilableTopic(Reconciliation reconciliation, KafkaTopic kt, String topicName) {

    @Override
    public String toString() {
        return "ReconcilableTopic{" +
                "reconciliation=" + reconciliation +
                '}';
    }
}
