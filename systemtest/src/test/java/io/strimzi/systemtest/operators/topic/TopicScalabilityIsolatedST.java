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

    @IsolatedTest
    void testBigAmountOfTopicsCreatingViaK8s(ExtensionContext extensionContext) {
        final String topicPrefix = "topic-example";

        KafkaTopicScalabilityUtils.createTopicsViaK8s(extensionContext, sharedClusterName, topicPrefix, NUMBER_OF_TOPICS, 3, 1, 1);
        KafkaTopicScalabilityUtils.checkTopicsReady(topicPrefix, NUMBER_OF_TOPICS, SAMPLE_OFFSET);

        LOGGER.info("Verifying that we've created {} topics", NUMBER_OF_TOPICS);
        assertEquals(NUMBER_OF_TOPICS, KafkaTopicUtils.getAllKafkaTopicsWithPrefix(INFRA_NAMESPACE, topicPrefix).size());
    }

    @IsolatedTest
    void testModifyBigAmountOfTopics(ExtensionContext extensionContext) {
        final String topicPrefix = "example-topic";
        Map<String, Object> modifiedConfig = new HashMap<>();

        int defaultPartitionCount = 2;

        // Create topics
        KafkaTopicScalabilityUtils.createTopicsViaK8s(extensionContext, sharedClusterName, topicPrefix, NUMBER_OF_TOPICS, defaultPartitionCount, 1, 1);
        KafkaTopicScalabilityUtils.checkTopicsReady(topicPrefix, NUMBER_OF_TOPICS, SAMPLE_OFFSET);


        // Increase ISR over limits expect topics to fail
        KafkaTopicScalabilityUtils.modifyTopics(topicPrefix, NUMBER_OF_TOPICS, defaultPartitionCount - 1, modifiedConfig);
        KafkaTopicScalabilityUtils.checkTopicsNotReady(topicPrefix, NUMBER_OF_TOPICS, SAMPLE_OFFSET);


        // Modify config and expect topics to be in error state
        modifiedConfig.put("min.insync.replicas", 2);
        KafkaTopicScalabilityUtils.modifyTopics(topicPrefix, NUMBER_OF_TOPICS, defaultPartitionCount, modifiedConfig);
        KafkaTopicScalabilityUtils.checkTopicsReady(topicPrefix, NUMBER_OF_TOPICS, SAMPLE_OFFSET);


        // Increase ISR over limits expect topics to fail
        modifiedConfig.put("min.insync.replicas", 6);
        KafkaTopicScalabilityUtils.modifyTopics(topicPrefix, NUMBER_OF_TOPICS, defaultPartitionCount, modifiedConfig);
        KafkaTopicScalabilityUtils.checkTopicsNotReady(topicPrefix, NUMBER_OF_TOPICS, SAMPLE_OFFSET);
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
