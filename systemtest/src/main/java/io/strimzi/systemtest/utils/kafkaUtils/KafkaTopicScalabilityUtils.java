/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.model.KafkaTopicSpec;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import io.strimzi.systemtest.resources.ResourceManager;
import org.junit.jupiter.api.extension.ExtensionContext;
import io.strimzi.systemtest.enums.CustomResourceStatus;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.strimzi.systemtest.Constants.INFRA_NAMESPACE;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;


@SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
public class KafkaTopicScalabilityUtils {
    private static final Logger LOGGER = LogManager.getLogger(KafkaTopicUtils.class);
    protected static ResourceManager resourceManager = ResourceManager.getInstance();

    public static void createTopicsViaK8s(ExtensionContext extensionContext, String clusterName, String topicPrefix,
                                          int numberOfTopics, int numberOfPartitions, int numberOfReplicas, int minInSyncReplicas) {
        LOGGER.info("Creating topics via Kubernetes");

        for (int i = 0; i < numberOfTopics; i++) {
            String currentTopicName = topicPrefix + i;
            LOGGER.debug("Creating topic {}", currentTopicName);

            CompletableFuture.runAsync(() ->
                    resourceManager.createResource(extensionContext, false, KafkaTopicTemplates.topic(
                            clusterName, currentTopicName, numberOfPartitions, numberOfReplicas, minInSyncReplicas, INFRA_NAMESPACE).build()));
        }
    }

    public static void waitForTopicStatus(String topicPrefix, int numberOfTopics, Enum<?> state) {
        List<CompletableFuture<?>> topics = new ArrayList<>();

        for (int i = 0; i < numberOfTopics; i++) {
            String currentTopic = topicPrefix + i;
            topics.add(CompletableFuture.runAsync(() ->
                    KafkaTopicUtils.waitForKafkaTopicStatus(INFRA_NAMESPACE, currentTopic, state)
            ));
        }

        CompletableFuture<Void> allTopics = CompletableFuture.allOf(topics.toArray(new CompletableFuture[0]))
                .thenRun(() -> LOGGER.info("All topics are in correct state"));

        allTopics.join();
    }

    public static void waitForTopicsNotReady(String topicPrefix, int numberOfTopics) {
        LOGGER.info("Verifying that topics are in NotReady state");
        KafkaTopicScalabilityUtils.waitForTopicStatus(topicPrefix, numberOfTopics, CustomResourceStatus.NotReady);
    }

    public static void waitForTopicsReady(String topicPrefix, int numberOfTopics) {
        LOGGER.info("Verifying that topics are in Ready state");
        KafkaTopicScalabilityUtils.waitForTopicStatus(topicPrefix, numberOfTopics, CustomResourceStatus.Ready);
    }

    public static void waitForTopicsContainConfig(String topicPrefix, int numberOfTopics, Map<String, Object> config) {
        LOGGER.info("Verifying that topics contain right config");
        List<CompletableFuture<?>> topics = new ArrayList<>();

        for (int i = 0; i < numberOfTopics; i++) {
            String currentTopic = topicPrefix + i;
            topics.add(CompletableFuture.runAsync(() -> {
                LOGGER.info("Verifying topic " + currentTopic);
                KafkaTopicUtils.waitForTopicConfigContains(currentTopic, INFRA_NAMESPACE, config);
            }));
        }

        CompletableFuture<Void> allTopics = CompletableFuture.allOf(topics.toArray(new CompletableFuture[0]))
                .thenRun(() -> LOGGER.info("All topics contain right config"));

        allTopics.join();
    }

    public static void modifyBigAmountOfTopics(String topicPrefix, int numberOfTopics, KafkaTopicSpec topicSpec) {
        LOGGER.info("Modify topics via Kubernetes");
        List<CompletableFuture<?>> topics = new ArrayList<>();

        for (int i = 0; i < numberOfTopics; i++) {
            String currentTopicName = topicPrefix + i;
            LOGGER.info("Modify topic {}", currentTopicName);

            topics.add(CompletableFuture.runAsync(() -> {
                KafkaTopicResource.replaceTopicResource(currentTopicName, kafkaTopic -> kafkaTopic.setSpec(topicSpec));
            }));
        }
    }

}
