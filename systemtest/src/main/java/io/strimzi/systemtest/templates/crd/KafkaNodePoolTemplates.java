/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.crd;

import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.operator.common.model.Labels;

import java.util.Map;

public class KafkaNodePoolTemplates {

    private KafkaNodePoolTemplates() {}

    public static KafkaNodePoolBuilder defaultKafkaNodePool(String nodePoolName, String kafkaClusterName, int kafkaReplicas) {
        return new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName(nodePoolName)
                .withLabels(Map.of(Labels.STRIMZI_CLUSTER_LABEL, kafkaClusterName))
            .endMetadata()
            .withNewSpec()
                .withReplicas(kafkaReplicas)
            .endSpec();
    }
}
