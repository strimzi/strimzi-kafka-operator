/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators.topic;

import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicScalabilityUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.test.annotations.IsolatedTest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static io.strimzi.systemtest.Constants.INFRA_NAMESPACE;
import static io.strimzi.systemtest.Constants.SCALABILITY;

@Tag(SCALABILITY)
@IsolatedSuite
public class TopicScalabilityIsolatedST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(TopicScalabilityIsolatedST.class);
    private static final int NUMBER_OF_TOPICS = 1000;
    private static final int SAMPLE_OFFSET = 50;
    private final String sharedClusterName = "topic-scalability-shared-cluster-name";
    final String topicPrefix = "example-topic";
    final int defaultPartitionCount = 2;

    @IsolatedTest
    void testBigAmountOfTopicsCreatingViaK8s(ExtensionContext extensionContext) {

        KafkaTopicScalabilityUtils.createTopicsViaK8s(extensionContext, sharedClusterName, topicPrefix,
                NUMBER_OF_TOPICS, 4, 3, 2);
        KafkaTopicScalabilityUtils.checkTopicsReady(topicPrefix, NUMBER_OF_TOPICS, SAMPLE_OFFSET);

        LOGGER.info("Verifying that we've created {} topics", NUMBER_OF_TOPICS);
        assertEquals(NUMBER_OF_TOPICS, KafkaTopicUtils.getAllKafkaTopicsWithPrefix(INFRA_NAMESPACE, topicPrefix).size());
    }

    @IsolatedTest
    void testModifyBigAmountOfTopicPartitions(ExtensionContext extensionContext) {

        // Create topics
        KafkaTopicScalabilityUtils.createTopicsViaK8s(extensionContext, sharedClusterName, topicPrefix,
                NUMBER_OF_TOPICS, defaultPartitionCount, 1, 1);
        KafkaTopicScalabilityUtils.checkTopicsReady(topicPrefix, NUMBER_OF_TOPICS, SAMPLE_OFFSET);

        // Decrease partitions and expect not ready status
        KafkaTopicScalabilityUtils.modifyTopics(topicPrefix, NUMBER_OF_TOPICS, defaultPartitionCount - 1);
        KafkaTopicScalabilityUtils.checkTopicsNotReady(topicPrefix, NUMBER_OF_TOPICS, SAMPLE_OFFSET);

        // Set back to default and check if topic becomes ready
        KafkaTopicScalabilityUtils.modifyTopics(topicPrefix, NUMBER_OF_TOPICS, defaultPartitionCount);
        KafkaTopicScalabilityUtils.checkTopicsReady(topicPrefix, NUMBER_OF_TOPICS, SAMPLE_OFFSET);
    }

    @IsolatedTest
    void testModifyBigAmountOfTopicConfigs(ExtensionContext extensionContext) {

        Map<String, Object> modifiedConfig = new HashMap<>();

        // Create topics
        KafkaTopicScalabilityUtils.createTopicsViaK8s(extensionContext, sharedClusterName, topicPrefix,
                NUMBER_OF_TOPICS, defaultPartitionCount, 3, 2);
        KafkaTopicScalabilityUtils.checkTopicsReady(topicPrefix, NUMBER_OF_TOPICS, SAMPLE_OFFSET);

        // Add set of configs and expect topics to have ready status
        modifiedConfig.put("compression.type", "gzip");
        modifiedConfig.put("cleanup.policy", "delete");
        modifiedConfig.put("message.timestamp.type", "LogAppendTime");
        modifiedConfig.put("min.insync.replicas", 6);

        KafkaTopicScalabilityUtils.modifyTopics(topicPrefix, NUMBER_OF_TOPICS, modifiedConfig);
        KafkaTopicScalabilityUtils.checkTopicsReady(topicPrefix, NUMBER_OF_TOPICS, SAMPLE_OFFSET);
        KafkaTopicScalabilityUtils.checkTopicConfigContains(sharedClusterName, topicPrefix, NUMBER_OF_TOPICS, SAMPLE_OFFSET, modifiedConfig);

        // Set time configs
        modifiedConfig.clear();
        modifiedConfig.put("max.compaction.lag.ms", 54321);
        modifiedConfig.put("min.compaction.lag.ms", 54);
        modifiedConfig.put("retention.ms", 3690);
        modifiedConfig.put("segment.ms", 123456);
        modifiedConfig.put("flush.ms", 456123);

        KafkaTopicScalabilityUtils.modifyTopics(topicPrefix, NUMBER_OF_TOPICS, modifiedConfig);
        KafkaTopicScalabilityUtils.checkTopicsReady(topicPrefix, NUMBER_OF_TOPICS, SAMPLE_OFFSET);

        // Set size configs
        modifiedConfig.clear();
        modifiedConfig.put("retention.bytes", 9876543);
        modifiedConfig.put("segment.bytes", 321654);
        modifiedConfig.put("max.message.bytes", 654321);
        modifiedConfig.put("flush.messages", 456123);

        KafkaTopicScalabilityUtils.modifyTopics(topicPrefix, NUMBER_OF_TOPICS, modifiedConfig);
        KafkaTopicScalabilityUtils.checkTopicsReady(topicPrefix, NUMBER_OF_TOPICS, SAMPLE_OFFSET);

        // Set back to default state
        modifiedConfig.clear();
        KafkaTopicScalabilityUtils.modifyTopics(topicPrefix, NUMBER_OF_TOPICS, modifiedConfig);
        KafkaTopicScalabilityUtils.checkTopicsReady(topicPrefix, NUMBER_OF_TOPICS, SAMPLE_OFFSET);
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        clusterOperator.unInstall();
        clusterOperator.defaultInstallation().createInstallation().runInstallation();
        LOGGER.info("Deploying shared Kafka across all test cases in {} namespace", INFRA_NAMESPACE);
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(sharedClusterName, 3, 1)
            .editMetadata()
                .withNamespace(INFRA_NAMESPACE)
            .endMetadata()
            .build());
    }

}
