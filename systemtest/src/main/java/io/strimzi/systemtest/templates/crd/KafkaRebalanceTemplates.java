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

    public static KafkaRebalanceBuilder kafkaRebalance(String namespaceName, String clusterName) {
        return defaultKafkaRebalance(namespaceName, clusterName);
    }

    private static KafkaRebalanceBuilder defaultKafkaRebalance(String namespaceName, String clusterName) {

        Map<String, String> kafkaRebalanceLabels = new HashMap<>();
        kafkaRebalanceLabels.put("strimzi.io/cluster", clusterName);

        return new KafkaRebalanceBuilder()
            .editMetadata()
                .withName(clusterName)
                .withNamespace(namespaceName)
                .withLabels(kafkaRebalanceLabels)
            .endMetadata()
            // spec cannot be null, that's why the `withNewSpec` is used here.
            .withNewSpec()
            .endSpec();
    }
}
