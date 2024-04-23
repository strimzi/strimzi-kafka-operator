/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.enums.DeploymentTypes;
import io.strimzi.systemtest.kafkaclients.internalClients.admin.AdminClient;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class AdminClientUtils {
    private static final Logger LOGGER = LogManager.getLogger(AdminClientUtils.class);

    private AdminClientUtils() {}

    /**
     * Checks if the specified Kafka topic is present.
     *
     * @param adminClient The Kafka {@link AdminClient} used to list topics.
     * @param topicName The name of the topic to check for presence.
     * @return {@code true} if the topic is present, {@code false} otherwise.
     */
    public static boolean isTopicPresent(AdminClient adminClient, String topicName) {
        final String newLineSeparatedTopics = adminClient.listTopics();
        LOGGER.trace("topics present in Kafka:\n{}", newLineSeparatedTopics);
        return newLineSeparatedTopics.isEmpty() ? false : Arrays.asList(newLineSeparatedTopics.split("\n")).contains(topicName);
    }

    /**
     * Waits for a specified Kafka topic to be present
     *
     * Periodically checks if a Kafka topic is present and waits until it appears
     * or until the timeout expires.
     *
     * @param adminClient The Kafka {@link AdminClient} used to check the topic's presence.
     * @param topicName The name of the topic to wait for.
     */
    public static void waitForTopicPresence(AdminClient adminClient, String topicName) {
        LOGGER.info("Waiting for topic: {} to be present in Kafka", topicName);
        TestUtils.waitFor("Topic: " + topicName + " to be present in Kafka", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT_SHORT,
            () -> isTopicPresent(adminClient, topicName));
    }

    /**
     * Waits for a specified Kafka topic to be absent.
     *
     * Periodically checks if a Kafka topic is absent and waits until it dissappears
     * or until the timeout expires.
     *
     * @param adminClient The Kafka {@link AdminClient} used to check the topic's absence.
     * @param topicName The name of the topic to wait for its absence.
     */
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

    ///////////////////////////////////////////
    //   Admin Client Pod deploying Utility
    ///////////////////////////////////////////

    /**
     * Constructs and configures an {@link AdminClient} for managing Kafka resources.
     *
     * @param namespace The Kubernetes namespace where the admin client pod is expected to be located.
     * @param adminName The name of the admin client, used to locate admin client Pod.
     * @return An {@link AdminClient} instance that has been configured with the necessary environment-based
     *         settings to interact with a Kafka cluster
     */
    public static AdminClient getConfiguredAdminClient(String namespace, String adminName) {
        final String adminClientPodName = KubeClusterResource.kubeClient().listPods(namespace, getLabelSelector(adminName)).get(0).getMetadata().getName();
        final AdminClient targetClusterAdminClient = new AdminClient(namespace, adminClientPodName);
        targetClusterAdminClient.configureFromEnv();

        return targetClusterAdminClient;
    }

    /**
     * Creates a label selector for Kubernetes resources to later identify admin client Pod.
     *
     * @param adminName The name of the admin client controller.
     * @return A {@link LabelSelector} configured with a set of labels.
     */
    private static LabelSelector getLabelSelector(String adminName) {
        Map<String, String> matchLabels = new HashMap<>();
        matchLabels.put(TestConstants.APP_POD_LABEL, TestConstants.ADMIN_CLIENT_NAME);
        matchLabels.put(TestConstants.KAFKA_ADMIN_CLIENT_LABEL_KEY, TestConstants.KAFKA_ADMIN_CLIENT_LABEL_VALUE);
        matchLabels.put(TestConstants.DEPLOYMENT_TYPE, DeploymentTypes.AdminClient.name());
        matchLabels.put(TestConstants.APP_CONTROLLER_LABEL, adminName);

        return new LabelSelectorBuilder()
            .withMatchLabels(matchLabels)
            .build();
    }
}
