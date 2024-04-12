/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.kafkaclients.internalClients.admin.AdminClient;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;

public class AdminClientUtils {
    private static final Logger LOGGER = LogManager.getLogger(AdminClientUtils.class);

    private AdminClientUtils() {}

    public static boolean isTopicPresent(AdminClient adminClient, String topicName) {
        final String newLineSeparatedTopics = adminClient.listTopics();
        LOGGER.trace("topics present in Kafka:\n{}", newLineSeparatedTopics);
        return newLineSeparatedTopics.isEmpty() ? false : Arrays.asList(newLineSeparatedTopics.split("\n")).contains(topicName);
    }

    public static void waitForTopicPresence(AdminClient adminClient, String topicName) {
        LOGGER.info("Waiting for topic: {} to be present in Kafka", topicName);
        TestUtils.waitFor("Topic: " + topicName + " to be present in Kafka", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT_SHORT,
            () -> isTopicPresent(adminClient, topicName));
    }

    public static void waitForTopicAbsence(AdminClient adminClient, String topicName) {
        LOGGER.info("Waiting for topic: {} to be absent in Kafka", topicName);
        TestUtils.waitFor("Topic: " + topicName + " to be absent in Kafka", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT_SHORT,
            () -> !isTopicPresent(adminClient, topicName));
    }

    /**
     * Waits for the number of partitions for a specified Kafka topic until the number matches.
     *
     * @param adminClient admin Client.
     * @param topicName The name of the Kafka topic.
     * @param expectedPartition The expected number of partitions for the topic to reach.
     */
    public static void waitForTopicPartitionInKafka(AdminClient adminClient, String topicName, int expectedPartition) {
        TestUtils.waitFor("KafkaTopic partition count to have desired value", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_STATUS_TIMEOUT,
            () -> adminClient.describeTopic(topicName).partitionCount() == expectedPartition);
    }
}
