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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This class contains crucial methods to create, modify and check large amount of KafkaTopics
 * */
public class KafkaTopicScalabilityUtils {


    private static final Logger LOGGER = LogManager.getLogger(KafkaTopicUtils.class);
    private KafkaTopicScalabilityUtils() {}

    public static void createTopicsViaK8s(String namespaceName, String clusterName, String topicPrefix,
                                          int numberOfTopics, int numberOfPartitions, int numberOfReplicas, int minInSyncReplicas) {
        LOGGER.info("Creating {} Topics via Kubernetes", numberOfTopics);

        for (int i = 0; i < numberOfTopics; i++) {
            String currentTopicName = topicPrefix + i;
            ResourceManager.getInstance().createResourceWithoutWait(KafkaTopicTemplates.topic(
                            clusterName, currentTopicName, numberOfPartitions, numberOfReplicas, minInSyncReplicas, namespaceName).build());
        }
    }

    public static void waitForTopicStatus(String namespaceName, String topicPrefix, int numberOfTopics, Enum<?> conditionType) {
        waitForTopicStatus(namespaceName, topicPrefix, numberOfTopics, conditionType, ConditionStatus.True);
    }

    public static void waitForTopicStatus(String namespaceName, String topicPrefix, int numberOfTopics, Enum<?> conditionType, ConditionStatus conditionStatus) {
        LOGGER.info("Verifying that {} Topics are in {} state", numberOfTopics, conditionType.toString());

        // Determine the appropriate number of threads
        int numberOfThreads = Math.min(Runtime.getRuntime().availableProcessors(), numberOfTopics);
        ExecutorService customExecutor = Executors.newFixedThreadPool(numberOfThreads);

        try {
            List<CompletableFuture<?>> topics = new ArrayList<>();

            for (int i = 0; i < numberOfTopics; i++) {
                final String currentTopic = topicPrefix + i;
                topics.add(CompletableFuture.runAsync(() ->
                    KafkaTopicUtils.waitForKafkaTopicStatus(namespaceName, currentTopic, conditionType, conditionStatus),
                        customExecutor // Use the custom executor
                ));
            }

            CompletableFuture<Void> allTopics = CompletableFuture.allOf(topics.toArray(new CompletableFuture[0]))
                    .thenRunAsync(() -> LOGGER.info("All Topics are in correct state"), customExecutor);

            allTopics.join();
        } finally {
            // Attempt to shut down now to immediately terminate ongoing tasks
            List<Runnable> notExecutedTasks = customExecutor.shutdownNow();
            if (!notExecutedTasks.isEmpty()) {
                LOGGER.warn("There were {} tasks that did not start", notExecutedTasks.size());
            }
            try {
                // Wait a while for tasks to respond to being cancelled
                if (!customExecutor.awaitTermination(30, TimeUnit.SECONDS))
                    LOGGER.error("Executor did not terminate");
            } catch (InterruptedException ie) {
                // Preserve interrupt status
                Thread.currentThread().interrupt();
            }
        }
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
