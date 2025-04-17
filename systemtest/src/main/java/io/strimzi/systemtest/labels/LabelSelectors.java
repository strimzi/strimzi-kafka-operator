/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.labels;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.common.model.Labels;

import java.util.HashMap;
import java.util.Map;

public class LabelSelectors {
    public static LabelSelector bridgeLabelSelector(String clusterName, String componentName) {
        return new LabelSelectorBuilder()
            .withMatchLabels(commonMatchLabels(clusterName, KafkaBridge.RESOURCE_KIND, componentName))
            .build();
    }

    public static LabelSelector connectLabelSelector(String clusterName, String componentName) {
        return new LabelSelectorBuilder()
            .withMatchLabels(commonMatchLabels(clusterName, KafkaConnect.RESOURCE_KIND, componentName))
            .build();
    }

    public static LabelSelector mirrorMaker2LabelSelector(String clusterName, String componentName) {
        return new LabelSelectorBuilder()
            .withMatchLabels(commonMatchLabels(clusterName, KafkaMirrorMaker2.RESOURCE_KIND, componentName))
            .build();
    }

    public static LabelSelector nodePoolLabelSelector(String clusterName, String poolName, ProcessRoles processRole) {
        Map<String, String> matchLabels = new HashMap<>();

        matchLabels.put(Labels.STRIMZI_CLUSTER_LABEL, clusterName);
        matchLabels.put(Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);
        matchLabels.put(Labels.STRIMZI_POOL_NAME_LABEL, poolName);

        switch (processRole) {
            case BROKER -> matchLabels.put(Labels.STRIMZI_BROKER_ROLE_LABEL, "true");
            case CONTROLLER -> matchLabels.put(Labels.STRIMZI_CONTROLLER_ROLE_LABEL, "true");
            default -> throw new RuntimeException("No role for KafkaNodePool specified");
        }

        return new LabelSelectorBuilder()
            .withMatchLabels(matchLabels)
            .build();
    }

    public static LabelSelector kafkaLabelSelector(String clusterName, String componentName) {
        Map<String, String> labels = new HashMap<>();

        labels.put(Labels.STRIMZI_CLUSTER_LABEL, clusterName);
        labels.put(Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);
        labels.put(Labels.STRIMZI_CONTROLLER_NAME_LABEL, componentName);
        
        return new LabelSelectorBuilder()
            .withMatchLabels(labels)
            .build();
    }

    public static LabelSelector entityOperatorLabelSelector(final String clusterName) {
        final Map<String, String> matchLabels = new HashMap<>();
        matchLabels.put(Labels.STRIMZI_CLUSTER_LABEL, clusterName);
        matchLabels.put(Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND);
        matchLabels.put(Labels.STRIMZI_COMPONENT_TYPE_LABEL, "entity-operator");

        return new LabelSelectorBuilder()
            .withMatchLabels(matchLabels)
            .build();
    }

    public static LabelSelector allKafkaPodsLabelSelector(String clusterName) {
        return kafkaLabelSelector(clusterName, KafkaResources.kafkaComponentName(clusterName));
    }

    private static Map<String, String> commonMatchLabels(String clusterName, String kind, String componentName) {
        Map<String, String> labels = new HashMap<>();

        labels.put(Labels.STRIMZI_CLUSTER_LABEL, clusterName);
        labels.put(Labels.STRIMZI_KIND_LABEL, kind);
        labels.put(Labels.STRIMZI_NAME_LABEL, componentName);

        return labels;
    }
}
