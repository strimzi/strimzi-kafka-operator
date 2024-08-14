/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.crd;

import io.strimzi.api.kafka.model.topic.KafkaTopicBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.storage.TestStorage;

public class KafkaTopicTemplates {

    private KafkaTopicTemplates() {}

    public static KafkaTopicBuilder topic(TestStorage testStorage) {
        return defaultTopic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName(), 1, 1, 1);
    }

    public static KafkaTopicBuilder continuousTopic(TestStorage testStorage) {
        return defaultTopic(testStorage.getNamespaceName(), testStorage.getClusterName(), testStorage.getContinuousTopicName(), 1, 1, 1);
    }

    public static KafkaTopicBuilder topic(String namespaceName, String topicName, String kafkaClusterName) {
        return defaultTopic(namespaceName, topicName, kafkaClusterName, 1, 1, 1);
    }

    public static KafkaTopicBuilder topic(String namespaceName, String topicName, String kafkaClusterName, int partitions) {
        return defaultTopic(namespaceName, topicName, kafkaClusterName, partitions, 1, 1);
    }

    public static KafkaTopicBuilder topic(
        String namespaceName,
        String topicName,
        String kafkaClusterName,
        int partitions,
        int replicas
    ) {
        return defaultTopic(namespaceName, topicName, kafkaClusterName, partitions, replicas, replicas);
    }

    public static KafkaTopicBuilder topic(
        String namespaceName,
        String topicName,
        String kafkaClusterName,
        int partitions,
        int replicas,
        int minIsr
    ) {
        return defaultTopic(namespaceName, topicName, kafkaClusterName, partitions, replicas, minIsr);
    }

    public static KafkaTopicBuilder defaultTopic(
        String namespaceName,
        String topicName,
        String kafkaClusterName,
        int partitions,
        int replicas,
        int minIsr
    ) {
        return new KafkaTopicBuilder()
            .withNewMetadata()
                .withName(topicName)
                .withNamespace(namespaceName)
                .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, kafkaClusterName)
            .endMetadata()
            .editSpec()
                .withPartitions(partitions)
                .withReplicas(replicas)
                .addToConfig("min.insync.replicas", minIsr)
                .addToConfig("retention.ms", 7200000)
                .addToConfig("segment.bytes", 1073741824)
            .endSpec();
    }
}
