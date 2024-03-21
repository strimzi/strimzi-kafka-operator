/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators.topic;

import io.strimzi.api.kafka.model.topic.KafkaTopicSpecBuilder;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.NamespaceManager;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicScalabilityUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.test.annotations.IsolatedTest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.TestConstants.SCALABILITY;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(SCALABILITY)
public class TopicScalabilityST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(TopicScalabilityST.class);
    private static final int NUMBER_OF_TOPICS = 200;
    private TestStorage sharedTestStorage;
    final String topicPrefix = "example-topic";


    @IsolatedTest("This test needs to run isolated due to access problems in parallel execution - using the same namespace")
    void testBigAmountOfTopicsCreatingViaK8s() {

        KafkaTopicScalabilityUtils.createTopicsViaK8s(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getClusterName(), topicPrefix,
                NUMBER_OF_TOPICS, 4, 3, 2);
        KafkaTopicScalabilityUtils.waitForTopicsReady(Environment.TEST_SUITE_NAMESPACE, topicPrefix, NUMBER_OF_TOPICS);

        LOGGER.info("Verifying that we've created {} Topics", NUMBER_OF_TOPICS);
        assertThat(KafkaTopicUtils.getAllKafkaTopicsWithPrefix(Environment.TEST_SUITE_NAMESPACE, topicPrefix).size(), is(NUMBER_OF_TOPICS));
    }

    @IsolatedTest
    void testModifyBigAmountOfTopics() {
        Map<String, Object> modifiedConfig = new HashMap<>();
        final int defaultPartitionCount = 1;
        final int modifiedPartitionCount = defaultPartitionCount + 1;

        // Create topics
        KafkaTopicScalabilityUtils.createTopicsViaK8s(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getClusterName(), topicPrefix,
                NUMBER_OF_TOPICS, defaultPartitionCount, 3, 1);
        KafkaTopicScalabilityUtils.waitForTopicsReady(Environment.TEST_SUITE_NAMESPACE, topicPrefix, NUMBER_OF_TOPICS);

        // Add set of configs and expect topics to have ready status
        modifiedConfig.put("compression.type", "gzip");
        modifiedConfig.put("cleanup.policy", "delete");
        modifiedConfig.put("message.timestamp.type", "LogAppendTime");
        modifiedConfig.put("min.insync.replicas", 2);

        KafkaTopicScalabilityUtils.modifyBigAmountOfTopics(Environment.TEST_SUITE_NAMESPACE, topicPrefix, NUMBER_OF_TOPICS,
                new KafkaTopicSpecBuilder().withConfig(modifiedConfig).build());
        KafkaTopicScalabilityUtils.waitForTopicsContainConfig(Environment.TEST_SUITE_NAMESPACE, topicPrefix, NUMBER_OF_TOPICS, modifiedConfig);

        // Set time configs
        modifiedConfig.clear();
        modifiedConfig.put("max.compaction.lag.ms", 54321);
        modifiedConfig.put("min.compaction.lag.ms", 54);
        modifiedConfig.put("retention.ms", 3690);
        modifiedConfig.put("segment.ms", 123456);
        modifiedConfig.put("flush.ms", 456123);

        KafkaTopicScalabilityUtils.modifyBigAmountOfTopics(Environment.TEST_SUITE_NAMESPACE, topicPrefix, NUMBER_OF_TOPICS,
                new KafkaTopicSpecBuilder().withConfig(modifiedConfig).build());
        KafkaTopicScalabilityUtils.waitForTopicsContainConfig(Environment.TEST_SUITE_NAMESPACE, topicPrefix, NUMBER_OF_TOPICS, modifiedConfig);

        // Set size configs
        modifiedConfig.clear();
        modifiedConfig.put("retention.bytes", 9876543);
        modifiedConfig.put("segment.bytes", 321654);
        modifiedConfig.put("max.message.bytes", 654321);
        modifiedConfig.put("flush.messages", 456123);

        KafkaTopicScalabilityUtils.modifyBigAmountOfTopics(Environment.TEST_SUITE_NAMESPACE, topicPrefix, NUMBER_OF_TOPICS,
                new KafkaTopicSpecBuilder().withConfig(modifiedConfig).build());
        KafkaTopicScalabilityUtils.waitForTopicsContainConfig(Environment.TEST_SUITE_NAMESPACE, topicPrefix, NUMBER_OF_TOPICS, modifiedConfig);

        // Set back to default state
        modifiedConfig.clear();
        KafkaTopicScalabilityUtils.modifyBigAmountOfTopics(Environment.TEST_SUITE_NAMESPACE, topicPrefix, NUMBER_OF_TOPICS,
                new KafkaTopicSpecBuilder().withConfig(modifiedConfig).build());
        KafkaTopicScalabilityUtils.waitForTopicsReady(Environment.TEST_SUITE_NAMESPACE, topicPrefix, NUMBER_OF_TOPICS);

        // Try increasing partitions as this should create more load
        KafkaTopicScalabilityUtils.modifyBigAmountOfTopics(Environment.TEST_SUITE_NAMESPACE, topicPrefix, NUMBER_OF_TOPICS,
                new KafkaTopicSpecBuilder().withPartitions(modifiedPartitionCount).build());
        KafkaTopicScalabilityUtils.waitForTopicsPartitions(Environment.TEST_SUITE_NAMESPACE, topicPrefix, NUMBER_OF_TOPICS, modifiedPartitionCount);
        KafkaTopicScalabilityUtils.waitForTopicsReady(Environment.TEST_SUITE_NAMESPACE, topicPrefix, NUMBER_OF_TOPICS);
    }

    @BeforeAll
    void setup() {
        sharedTestStorage = new TestStorage(ResourceManager.getTestContext(), Environment.TEST_SUITE_NAMESPACE);

        clusterOperator.defaultInstallation()
            .createInstallation()
            .runInstallation();

        LOGGER.info("Deploying shared Kafka across all test cases in Namespace: {}", Environment.TEST_SUITE_NAMESPACE);

        NamespaceManager.getInstance().createNamespaceAndPrepare(Environment.TEST_SUITE_NAMESPACE);

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(sharedTestStorage.getNamespaceName(), sharedTestStorage.getBrokerPoolName(), sharedTestStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(sharedTestStorage.getNamespaceName(), sharedTestStorage.getControllerPoolName(), sharedTestStorage.getClusterName(), 1).build()
            )
        );

        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(sharedTestStorage.getClusterName(), 3, 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .build());
    }

}
