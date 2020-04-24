/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.strimzi.systemtest.resources.crd.KafkaTopicResource.kafkaTopicClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaTopicUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaTopicUtils.class);

    private KafkaTopicUtils() {}

    /**
     * Method which return UID for specific topic
     * @param topicName topic name
     * @return topic UID
     */
    public static String topicSnapshot(String topicName) {
        return kafkaTopicClient().inNamespace(kubeClient().getNamespace()).withName(topicName).get().getMetadata().getUid();
    }

    /**
     * Method which wait until topic has rolled form one generation to another.
     * @param topicName topic name
     * @param topicUid topic UID
     * @return topic new UID
     */
    public static String waitTopicHasRolled(String topicName, String topicUid) {
        TestUtils.waitFor("Topic " + topicName + " has rolled", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> !topicUid.equals(topicSnapshot(topicName)));
        return topicSnapshot(topicName);
    }

    public static void waitForKafkaTopicCreation(String topicName) {
        LOGGER.info("Waiting for KafkaTopic {} creation ", topicName);
        TestUtils.waitFor("KafkaTopic creation " + topicName, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> kafkaTopicClient().inNamespace(kubeClient().getNamespace())
                    .withName(topicName).get().getStatus().getConditions().get(0).getType().equals("Ready"),
            () -> LOGGER.info(kafkaTopicClient().inNamespace(kubeClient().getNamespace()).withName(topicName).get())
        );
    }

    public static void waitForKafkaTopicCreationByNamePrefix(String topicNamePrefix) {
        LOGGER.info("Waiting for KafkaTopic {} creation", topicNamePrefix);
        TestUtils.waitFor("KafkaTopic creation " + topicNamePrefix, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> kafkaTopicClient().inNamespace(kubeClient().getNamespace()).list().getItems().stream()
                    .filter(topic -> topic.getMetadata().getName().contains(topicNamePrefix))
                    .findFirst().get().getStatus().getConditions().get(0).getType().equals("Ready")
        );
    }

    public static void waitForKafkaTopicDeletion(String topicName) {
        LOGGER.info("Waiting for KafkaTopic {} deletion", topicName);
        TestUtils.waitFor("KafkaTopic deletion " + topicName, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> kafkaTopicClient().inNamespace(kubeClient().getNamespace()).withName(topicName).get() == null,
            () -> LOGGER.info(kafkaTopicClient().inNamespace(kubeClient().getNamespace()).withName(topicName).get())
        );
    }

    public static void waitForKafkaTopicPartitionChange(String topicName, int partitions) {
        LOGGER.info("Waiting for KafkaTopic change {}", topicName);
        TestUtils.waitFor("KafkaTopic change " + topicName, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> kafkaTopicClient().inNamespace(kubeClient().getNamespace()).withName(topicName).get().getSpec().getPartitions() == partitions,
            () -> LOGGER.info(kafkaTopicClient().inNamespace(kubeClient().getNamespace()).withName(topicName).get())
        );
    }

    /**
     * Wait until KafkaTopic is in desired status
     * @param topicName name of KafkaTopic
     * @param state desired state
     */
    public static void waitForKafkaTopicStatus(String topicName, String state) {
        LOGGER.info("Wait until KafkaTopic {} is in desired state: {}", topicName, state);
        TestUtils.waitFor("KafkaTopic " + topicName + " status is not in desired state: " + state, Constants.GLOBAL_POLL_INTERVAL, Constants.CONNECT_STATUS_TIMEOUT,
            () -> kafkaTopicClient().inNamespace(kubeClient().getNamespace())
                    .withName(topicName).get().getStatus().getConditions().get(0).getType().equals(state),
            () -> LOGGER.info(kafkaTopicClient().inNamespace(kubeClient().getNamespace()).withName(topicName).get())
        );
        LOGGER.info("Kafka Topic {} is in desired state: {}", topicName, state);
    }

    public static void waitForKafkaTopicReady(String topicName) {
        waitForKafkaTopicStatus(topicName, "Ready");
    }

    public static void waitForKafkaTopicNotReady(String topicName) {
        waitForKafkaTopicStatus(topicName, "NotReady");
    }

    public static void waitForKafkaTopicsCount(int topicCount, String clusterName) {
        LOGGER.info("Wait until we create {} KafkaTopics", topicCount);
        TestUtils.waitFor(topicCount + " KafkaTopics creation",
            Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> KafkaCmdClient.listTopicsUsingPodCli(clusterName, 0).size() == topicCount);
        LOGGER.info("{} KafkaTopics were created", topicCount);
    }
}
