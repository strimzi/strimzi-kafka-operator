/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.crd;

import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceBuilder;

import java.util.HashMap;
import java.util.Map;

public class KafkaRebalanceTemplates {

    private KafkaRebalanceTemplates() {}

    public static KafkaRebalanceBuilder kafkaRebalance(String namespaceName, String kafkaClusterName) {
        return defaultKafkaRebalance(namespaceName, kafkaClusterName);
    }

    private static KafkaRebalanceBuilder defaultKafkaRebalance(String namespaceName, String kafkaClusterName) {

        Map<String, String> kafkaRebalanceLabels = new HashMap<>();
        kafkaRebalanceLabels.put("strimzi.io/cluster", kafkaClusterName);

        return new KafkaRebalanceBuilder()
            .editMetadata()
                .withName(kafkaClusterName)
                .withNamespace(namespaceName)
                .withLabels(kafkaRebalanceLabels)
            .endMetadata()
            // spec cannot be null, that's why the `withNewSpec` is used here.
            .withNewSpec()
            .endSpec();
    }
}
