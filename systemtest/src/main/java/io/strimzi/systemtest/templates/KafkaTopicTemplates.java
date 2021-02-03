/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates;

import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KafkaTopicTemplates {

    private static final Logger LOGGER = LogManager.getLogger(KafkaTopicTemplates.class);

    private KafkaTopicTemplates() {}

    public static final String PATH_TO_KAFKA_TOPIC_CONFIG = TestUtils.USER_PATH + "/../examples/topic/kafka-topic.yaml";

    public static KafkaTopicBuilder topic(String clusterName, String topicName) {
        return defaultTopic(clusterName, topicName, 1, 1, 1);
    }

    public static KafkaTopicBuilder topic(String clusterName, String topicName, int partitions) {
        return defaultTopic(clusterName, topicName, partitions, 1, 1);
    }

    public static KafkaTopicBuilder topic(String clusterName, String topicName, int partitions, int replicas) {
        return defaultTopic(clusterName, topicName, partitions, replicas, replicas);
    }

    public static KafkaTopicBuilder topic(String clusterName, String topicName, int partitions, int replicas, int minIsr) {
        return defaultTopic(clusterName, topicName, partitions, replicas, minIsr);
    }

    public static KafkaTopicBuilder defaultTopic(String clusterName, String topicName, int partitions, int replicas, int minIsr) {
        KafkaTopic kafkaTopic = getKafkaTopicFromYaml(PATH_TO_KAFKA_TOPIC_CONFIG);
        return new KafkaTopicBuilder(kafkaTopic)
            .withNewMetadata()
                .withName(topicName)
                .withNamespace(ResourceManager.kubeClient().getNamespace())
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
