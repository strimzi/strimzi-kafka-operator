/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators.topic;

import io.strimzi.api.kafka.model.KafkaTopicSpecBuilder;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicScalabilityUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.test.annotations.IsolatedTest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.Constants.INFRA_NAMESPACE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static io.strimzi.systemtest.Constants.SCALABILITY;


@Tag(SCALABILITY)
@IsolatedSuite
@Disabled("TO is not so stable with large number of topics. After new version of TO, these should be enabled again")
public class TopicScalabilityIsolatedST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(TopicScalabilityIsolatedST.class);
    private static final int NUMBER_OF_TOPICS = 200;
    private final String sharedClusterName = "topic-scalability-shared-cluster-name";
    final String topicPrefix = "example-topic";


    @IsolatedTest("This test needs to run isolated due to access problems in parallel execution - using the same namespace")
    void testBigAmountOfTopicsCreatingViaK8s(ExtensionContext extensionContext) {

        KafkaTopicScalabilityUtils.createTopicsViaK8s(extensionContext, clusterOperator.getDeploymentNamespace(), sharedClusterName, topicPrefix,
                NUMBER_OF_TOPICS, 4, 3, 2);
        KafkaTopicScalabilityUtils.waitForTopicsReady(clusterOperator.getDeploymentNamespace(), topicPrefix, NUMBER_OF_TOPICS);

        LOGGER.info("Verifying that we've created {} topics", NUMBER_OF_TOPICS);
        assertThat(KafkaTopicUtils.getAllKafkaTopicsWithPrefix(clusterOperator.getDeploymentNamespace(), topicPrefix).size(), is(NUMBER_OF_TOPICS));
    }

    @IsolatedTest
    void testModifyBigAmountOfTopicPartitions(ExtensionContext extensionContext) {
        final int defaultPartitionCount = 2;

        // Create topics
        KafkaTopicScalabilityUtils.createTopicsViaK8s(extensionContext, clusterOperator.getDeploymentNamespace(), sharedClusterName, topicPrefix,
                NUMBER_OF_TOPICS, defaultPartitionCount, 1, 1);
        KafkaTopicScalabilityUtils.waitForTopicsReady(clusterOperator.getDeploymentNamespace(), topicPrefix, NUMBER_OF_TOPICS);

        // Decrease partitions and expect not ready status
        KafkaTopicScalabilityUtils.modifyBigAmountOfTopics(clusterOperator.getDeploymentNamespace(), topicPrefix, NUMBER_OF_TOPICS,
                new KafkaTopicSpecBuilder().withPartitions(defaultPartitionCount - 1).build());
        KafkaTopicScalabilityUtils.waitForTopicsNotReady(clusterOperator.getDeploymentNamespace(), topicPrefix, NUMBER_OF_TOPICS);

        // Set back to default and check if topic becomes ready
        KafkaTopicScalabilityUtils.modifyBigAmountOfTopics(clusterOperator.getDeploymentNamespace(), topicPrefix, NUMBER_OF_TOPICS,
                new KafkaTopicSpecBuilder().withPartitions(defaultPartitionCount).build());
        KafkaTopicScalabilityUtils.waitForTopicsReady(clusterOperator.getDeploymentNamespace(), topicPrefix, NUMBER_OF_TOPICS);
    }

    @IsolatedTest
    void testModifyBigAmountOfTopicConfigs(ExtensionContext extensionContext) {
        Map<String, Object> modifiedConfig = new HashMap<>();

        // Create topics
        KafkaTopicScalabilityUtils.createTopicsViaK8s(extensionContext, clusterOperator.getDeploymentNamespace(), sharedClusterName, topicPrefix,
                NUMBER_OF_TOPICS, 2, 3, 2);
        KafkaTopicScalabilityUtils.waitForTopicsReady(clusterOperator.getDeploymentNamespace(), topicPrefix, NUMBER_OF_TOPICS);

        // Add set of configs and expect topics to have ready status
        modifiedConfig.put("compression.type", "gzip");
        modifiedConfig.put("cleanup.policy", "delete");
        modifiedConfig.put("message.timestamp.type", "LogAppendTime");
        modifiedConfig.put("min.insync.replicas", 6);

        KafkaTopicScalabilityUtils.modifyBigAmountOfTopics(clusterOperator.getDeploymentNamespace(), topicPrefix, NUMBER_OF_TOPICS,
                new KafkaTopicSpecBuilder().withConfig(modifiedConfig).build());
        KafkaTopicScalabilityUtils.waitForTopicsContainConfig(clusterOperator.getDeploymentNamespace(), topicPrefix, NUMBER_OF_TOPICS, modifiedConfig);

        // Set time configs
        modifiedConfig.clear();
        modifiedConfig.put("max.compaction.lag.ms", 54321);
        modifiedConfig.put("min.compaction.lag.ms", 54);
        modifiedConfig.put("retention.ms", 3690);
        modifiedConfig.put("segment.ms", 123456);
        modifiedConfig.put("flush.ms", 456123);

        KafkaTopicScalabilityUtils.modifyBigAmountOfTopics(clusterOperator.getDeploymentNamespace(), topicPrefix, NUMBER_OF_TOPICS,
                new KafkaTopicSpecBuilder().withConfig(modifiedConfig).build());
        KafkaTopicScalabilityUtils.waitForTopicsContainConfig(clusterOperator.getDeploymentNamespace(), topicPrefix, NUMBER_OF_TOPICS, modifiedConfig);


        // Set size configs
        modifiedConfig.clear();
        modifiedConfig.put("retention.bytes", 9876543);
        modifiedConfig.put("segment.bytes", 321654);
        modifiedConfig.put("max.message.bytes", 654321);
        modifiedConfig.put("flush.messages", 456123);

        KafkaTopicScalabilityUtils.modifyBigAmountOfTopics(clusterOperator.getDeploymentNamespace(), topicPrefix, NUMBER_OF_TOPICS,
                new KafkaTopicSpecBuilder().withConfig(modifiedConfig).build());
        KafkaTopicScalabilityUtils.waitForTopicsContainConfig(clusterOperator.getDeploymentNamespace(), topicPrefix, NUMBER_OF_TOPICS, modifiedConfig);


        // Set back to default state
        modifiedConfig.clear();
        KafkaTopicScalabilityUtils.modifyBigAmountOfTopics(clusterOperator.getDeploymentNamespace(), topicPrefix, NUMBER_OF_TOPICS,
                new KafkaTopicSpecBuilder().withConfig(modifiedConfig).build());
        KafkaTopicScalabilityUtils.waitForTopicsReady(clusterOperator.getDeploymentNamespace(), topicPrefix, NUMBER_OF_TOPICS);
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        clusterOperator.unInstall();
        clusterOperator.defaultInstallation().createInstallation().runInstallation();
        LOGGER.info("Deploying shared Kafka across all test cases in {} namespace", clusterOperator.getDeploymentNamespace());
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(sharedClusterName, 3, 1)
            .editMetadata()
                .withNamespace(INFRA_NAMESPACE)
            .endMetadata()
            .build());
    }

}
