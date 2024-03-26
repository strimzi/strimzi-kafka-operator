/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.crd;

import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.test.TestUtils;

public class KafkaTopicTemplates {

    private KafkaTopicTemplates() {}

    public static KafkaTopicBuilder topic(TestStorage testStorage) {
        return defaultTopic(testStorage.getClusterName(), testStorage.getTopicName(), 1, 1, 1, testStorage.getNamespaceName());
    }

    public static KafkaTopicBuilder continuousTopic(TestStorage testStorage) {
        return defaultTopic(testStorage.getClusterName(), testStorage.getContinuousTopicName(), 1, 1, 1, testStorage.getNamespaceName());
    }

    public static KafkaTopicBuilder topic(String clusterName, String topicName, String topicNamespace) {
        return defaultTopic(clusterName, topicName, 1, 1, 1, topicNamespace);
    }

    public static KafkaTopicBuilder topic(String clusterName, String topicName, int partitions, String topicNamespace) {
        return defaultTopic(clusterName, topicName, partitions, 1, 1, topicNamespace);
    }

    public static KafkaTopicBuilder topic(String clusterName, String topicName, int partitions, int replicas, String topicNamespace) {
        return defaultTopic(clusterName, topicName, partitions, replicas, replicas, topicNamespace);
    }

    public static KafkaTopicBuilder topic(String clusterName, String topicName, int partitions, int replicas, int minIsr, String topicNamespace) {
        return defaultTopic(clusterName, topicName, partitions, replicas, minIsr, topicNamespace);
    }

    public static KafkaTopicBuilder defaultTopic(String clusterName, String topicName, int partitions, int replicas, int minIsr, String topicNamespace) {
        KafkaTopic kafkaTopic = getKafkaTopicFromYaml(TestConstants.PATH_TO_KAFKA_TOPIC_CONFIG);
        return new KafkaTopicBuilder(kafkaTopic)
            .withNewMetadata()
                .withName(topicName)
                .withNamespace(topicNamespace)
                .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, clusterName)
            .endMetadata()
            .editSpec()
                .withPartitions(partitions)
                .withReplicas(replicas)
                .addToConfig("min.insync.replicas", minIsr)
            .endSpec();
    }

    private static KafkaTopic getKafkaTopicFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, KafkaTopic.class);
    }
}
