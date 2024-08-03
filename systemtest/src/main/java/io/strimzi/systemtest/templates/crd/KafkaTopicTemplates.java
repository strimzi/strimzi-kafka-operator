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
        return defaultTopic(testStorage.getNamespace(), testStorage.getClusterName(), testStorage.getTopicName(), 1, 1, 1);
    }

    public static KafkaTopicBuilder continuousTopic(TestStorage testStorage) {
        return defaultTopic(testStorage.getNamespace(), testStorage.getClusterName(), testStorage.getContinuousTopicName(), 1, 1, 1);
    }

    public static KafkaTopicBuilder topic(String topicNamespace, String clusterName, String topicName) {
        return defaultTopic(topicNamespace, clusterName, topicName, 1, 1, 1);
    }

    public static KafkaTopicBuilder topic(String topicNamespace, String clusterName, String topicName, int partitions) {
        return defaultTopic(topicNamespace, clusterName, topicName, partitions, 1, 1);
    }

    public static KafkaTopicBuilder topic(String topicNamespace, String clusterName, String topicName, int partitions, int replicas) {
        return defaultTopic(topicNamespace, clusterName, topicName, partitions, replicas, replicas);
    }

    public static KafkaTopicBuilder topic(String topicNamespace, String clusterName, String topicName, int partitions, int replicas, int minIsr) {
        return defaultTopic(topicNamespace, clusterName, topicName, partitions, replicas, minIsr);
    }

    public static KafkaTopicBuilder defaultTopic(String topicNamespace, String clusterName, String topicName, int partitions, int replicas, int minIsr) {
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
