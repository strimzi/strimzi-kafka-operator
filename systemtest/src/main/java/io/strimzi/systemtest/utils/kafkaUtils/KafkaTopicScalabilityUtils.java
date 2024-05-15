/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.model.topic.KafkaTopicSpec;
import io.strimzi.systemtest.enums.ConditionStatus;
import io.strimzi.systemtest.enums.CustomResourceStatus;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * This class contains crucial methods to create, modify and check large amount of KafkaTopics
 * */
public class KafkaTopicScalabilityUtils {


    private static final Logger LOGGER = LogManager.getLogger(KafkaTopicUtils.class);
    private KafkaTopicScalabilityUtils() {}

    /**
     * Creates Kafka topics in specified batches via Kubernetes within the given namespace.
     *
     * @param namespaceName         the Kubernetes namespace where the topics will be created.
     * @param clusterName           the name of the Kafka cluster with which the topics are associated.
     * @param topicPrefix           the prefix string for the topic names, to which a numeric suffix will be added.
     * @param start                 the starting index for the topic creation batch.
     * @param end                   the ending index for the topic creation batch (exclusive).
     * @param numberOfPartitions    the number of partitions each topic should have.
     * @param numberOfReplicas      the number of replicas for each topic.
     * @param minInSyncReplicas     the minimum number of replicas that must be in sync.
     */
    public static void createTopicsViaK8s(String namespaceName, String clusterName, String topicPrefix,
                                          int start, int end, int numberOfPartitions, int numberOfReplicas, int minInSyncReplicas) {
        LOGGER.info("Creating topics from {} to {} via Kubernetes", start, end - 1);

        for (int i = start; i < end; i++) {
            String currentTopicName = topicPrefix + "-" + i;
            ResourceManager.getInstance().createResourceWithoutWait(KafkaTopicTemplates.topic(
                clusterName, currentTopicName, numberOfPartitions, numberOfReplicas, minInSyncReplicas, namespaceName).build());
        }
    }

    public static void createTopicsViaK8s(String namespaceName, String clusterName, String topicPrefix,
                                          int numberOfTopics, int numberOfPartitions, int numberOfReplicas, int minInSyncReplicas) {
        createTopicsViaK8s(namespaceName, clusterName, topicPrefix, 0, numberOfTopics, numberOfPartitions, numberOfReplicas, minInSyncReplicas);
    }

    public static void waitForTopicStatus(String namespaceName, String topicPrefix, int numberOfTopics, Enum<?> conditionType) {
        waitForTopicStatus(namespaceName, topicPrefix, numberOfTopics, conditionType, ConditionStatus.True);
    }

    /**
     * Waits for a specific condition to be met for a range of Kafka topics in a given namespace.
     *
     * @param namespaceName     The Kubernetes namespace where the topics reside.
     * @param topicPrefix       The prefix used for topic names, to which a numeric suffix will be added.
     * @param start             The starting index for checking topic statuses.
     * @param end               The ending index for checking topic statuses (exclusive).
     * @param conditionType     The type of condition to wait for (e.g., READY, NOT_READY).
     * @param conditionStatus   The desired status of the condition (e.g., TRUE, FALSE, UNKNOWN).
     */
    public static void waitForTopicStatus(String namespaceName, String topicPrefix, int start, int end, Enum<?> conditionType, ConditionStatus conditionStatus) {
        LOGGER.info("Verifying that topics from {} to {} are in {} state", start, end - 1, conditionType.toString());

        for (int i = start; i < end; i++) {
            final String currentTopic = topicPrefix + "-" + i;
            KafkaTopicUtils.waitForKafkaTopicStatus(namespaceName, currentTopic, conditionType, conditionStatus);
        }
    }

    public static void waitForTopicStatus(String namespaceName, String topicPrefix, int numberOfTopics, Enum<?> conditionType, ConditionStatus conditionStatus) {
        waitForTopicStatus(namespaceName, topicPrefix, 0, numberOfTopics, conditionType, conditionStatus);
    }

    public static void waitForTopicsReady(String namespaceName, String topicPrefix, int numberOfTopics) {
        waitForTopicStatus(namespaceName, topicPrefix, numberOfTopics, CustomResourceStatus.Ready);
    }

    public static void waitForTopicsContainConfig(String namespaceName, String topicPrefix, int numberOfTopics, Map<String, Object> config) {
        LOGGER.info("Verifying that {} Topics contain right config", numberOfTopics);
        List<CompletableFuture<?>> topics = new ArrayList<>();

        for (int i = 0; i < numberOfTopics; i++) {
            String currentTopic = topicPrefix + i;
            topics.add(CompletableFuture.runAsync(() -> {
                KafkaTopicUtils.waitForTopicConfigContains(namespaceName, currentTopic, config);
            }));
        }

        CompletableFuture<Void> allTopics = CompletableFuture.allOf(topics.toArray(new CompletableFuture[0]))
                .thenRun(() -> LOGGER.info("All Topics contain right config"));

        allTopics.join();
    }

    public static void waitForTopicsPartitions(String namespaceName, String topicPrefix, int numberOfTopics, int numberOfPartitions) {
        LOGGER.info("Verifying that {} Topics have correct number of partitions: {}", numberOfTopics, numberOfPartitions);
        List<CompletableFuture<?>> topics = new ArrayList<>();

        for (int i = 0; i < numberOfTopics; i++) {
            String currentTopic = topicPrefix + i;
            topics.add(CompletableFuture.runAsync(() -> {
                KafkaTopicUtils.waitForKafkaTopicPartitionChange(namespaceName, currentTopic, numberOfPartitions);
            }));
        }

        CompletableFuture<Void> allTopics = CompletableFuture.allOf(topics.toArray(new CompletableFuture[0]))
                .thenRun(() -> LOGGER.info("All Topics have correct number of partitions"));

        allTopics.join();
    }

    public static void modifyBigAmountOfTopics(String namespaceName, String topicPrefix, int numberOfTopics, KafkaTopicSpec topicSpec) {
        LOGGER.info("Modify {} Topics via Kubernetes", numberOfTopics);

        for (int i = 0; i < numberOfTopics; i++) {
            String currentTopicName = topicPrefix + i;
            KafkaTopicResource.replaceTopicResourceInSpecificNamespace(currentTopicName, kafkaTopic -> kafkaTopic.setSpec(topicSpec), namespaceName);
        }
    }

}
