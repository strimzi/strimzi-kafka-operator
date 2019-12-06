/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.model.DoneableKafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.strimzi.systemtest.resources.ResourceManager;

public class KafkaTopicResource {
    private static final Logger LOGGER = LogManager.getLogger(KafkaTopicResource.class);

    public static final String PATH_TO_KAFKA_TOPIC_CONFIG = "../examples/topic/kafka-topic.yaml";

    public static MixedOperation<KafkaTopic, KafkaTopicList, DoneableKafkaTopic, Resource<KafkaTopic, DoneableKafkaTopic>> kafkaTopicClient() {
        return Crds.topicOperation(ResourceManager.kubeClient().getClient());
    }

    public static DoneableKafkaTopic topic(String clusterName, String topicName) {
        return topic(defaultTopic(clusterName, topicName, 1, 1).build());
    }

    public static DoneableKafkaTopic topic(String clusterName, String topicName, int partitions) {
        return topic(defaultTopic(clusterName, topicName, partitions, 1).build());
    }

    public static DoneableKafkaTopic topic(String clusterName, String topicName, int partitions, int replicas) {
        return topic(defaultTopic(clusterName, topicName, partitions, replicas).build());
    }

    private static KafkaTopicBuilder defaultTopic(String clusterName, String topicName, int partitions, int replicas) {
        KafkaTopic kafkaTopic = getKafkaTopicFromYaml(PATH_TO_KAFKA_TOPIC_CONFIG);
        return new KafkaTopicBuilder(kafkaTopic)
            .withNewMetadata()
                .withName(topicName)
                .withNamespace(ResourceManager.kubeClient().getNamespace())
                .addToLabels("strimzi.io/cluster", clusterName)
            .endMetadata()
            .editSpec()
                .withPartitions(partitions)
                .withReplicas(replicas)
                .addToConfig("min.insync.replicas", replicas)
            .endSpec();
    }

    static DoneableKafkaTopic topic(KafkaTopic topic) {
        return new DoneableKafkaTopic(topic, kt -> {
            kafkaTopicClient().inNamespace(topic.getMetadata().getNamespace()).createOrReplace(kt);
            LOGGER.info("Created KafkaTopic {}", kt.getMetadata().getName());
            return waitFor(deleteLater(kt));
        });
    }

    private static KafkaTopic getKafkaTopicFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, KafkaTopic.class);
    }

    private static KafkaTopic waitFor(KafkaTopic kafkaTopic) {
        LOGGER.info("Waiting for Kafka Topic {}", kafkaTopic.getMetadata().getName());
        StUtils.waitForKafkaTopicCreation(kafkaTopic.getMetadata().getName());
        LOGGER.info("Kafka Topic {} is ready", kafkaTopic.getMetadata().getName());
        return kafkaTopic;
    }

    private static KafkaTopic deleteLater(KafkaTopic kafkaTopic) {
        return ResourceManager.deleteLater(kafkaTopicClient(), kafkaTopic);
    }
}
