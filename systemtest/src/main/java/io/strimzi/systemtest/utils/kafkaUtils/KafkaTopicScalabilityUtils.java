/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import io.strimzi.systemtest.resources.ResourceManager;
import org.junit.jupiter.api.extension.ExtensionContext;
import io.strimzi.systemtest.enums.CustomResourceStatus;

import java.util.*;
import java.util.concurrent.CompletableFuture;

import static io.strimzi.systemtest.Constants.INFRA_NAMESPACE;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.junit.jupiter.api.Assertions.*;


@SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
public class KafkaTopicScalabilityUtils {
    private static final Logger LOGGER = LogManager.getLogger(KafkaTopicUtils.class);
    protected static ResourceManager resourceManager = ResourceManager.getInstance();

    public static void createTopicsViaK8s(ExtensionContext extensionContext, String clusterName, String topicPrefix,
                                          int numberOfTopics, int numberOfPartitions, int numberOfReplicas, int minInSyncReplicas) {
        LOGGER.info("Creating topics via Kubernetes");

        ArrayList<CompletableFuture<?>> topics = new ArrayList<>();

        for (int i = 0; i < numberOfTopics; i++){
            String currentTopicName = topicPrefix + i;
            LOGGER.debug("Creating topic {}", currentTopicName);

            topics.add(CompletableFuture.runAsync(() ->
                    resourceManager.createResource(extensionContext, false, KafkaTopicTemplates.topic(
                            clusterName, currentTopicName , numberOfPartitions, numberOfReplicas, minInSyncReplicas, INFRA_NAMESPACE).build())));
        }
    }

    public static void checkTopicsState(String topicPrefix, int numberOfTopics, Enum<?> state){
        List<CompletableFuture<?>> topics = new ArrayList<>();

        for (int i = 0; i < numberOfTopics; i++){
            String currentTopic = topicPrefix + i;
            topics.add(CompletableFuture.runAsync(() ->
                    KafkaTopicUtils.waitForKafkaTopicStatus(INFRA_NAMESPACE, currentTopic, state)
            ));
        }

        CompletableFuture<Void> allTopics = CompletableFuture.allOf(topics.toArray(new CompletableFuture[0]))
                .thenRun(() -> LOGGER.info("All topics are in correct state"));

        allTopics.join();
    }

    public static void checkTopicsNotReady(String topicPrefix, int numberOfTopics) {
        LOGGER.info("Verifying that topics are in NotReady state");
        KafkaTopicScalabilityUtils.checkTopicsState(topicPrefix, numberOfTopics, CustomResourceStatus.NotReady);
    }

    public static void checkTopicsReady(String topicPrefix, int numberOfTopics) {
        LOGGER.info("Verifying that topics are in Ready state");
        KafkaTopicScalabilityUtils.checkTopicsState(topicPrefix, numberOfTopics, CustomResourceStatus.Ready);
    }

    public static void checkTopicsContainConfig(String topicPrefix, int numberOfTopics, Map<String, Object> config) {
        LOGGER.info("Verifying that topics contain right config");
        List<CompletableFuture<?>> topics = new ArrayList<>();

        for (int i = 0; i < numberOfTopics; i++){
            String currentTopic = topicPrefix + i;
            topics.add(CompletableFuture.runAsync(() ->{
                try {
                    String json = cmdKubeClient(kubeClient().getNamespace()).exec("get", "kafkatopic", currentTopic, "-o", "jsonpath='{.spec.config}'").out();
                    HashMap<?,?> mapping = new ObjectMapper().readValue(json.replaceAll("'",""), HashMap.class);
                    assertEquals(mapping, config);
                } catch (JsonProcessingException e) {
                    fail(e.getMessage());
                }
            }));
        }

        CompletableFuture<Void> allTopics = CompletableFuture.allOf(topics.toArray(new CompletableFuture[0]))
                .thenRun(() -> LOGGER.info("All topics contain right config"));

        allTopics.join();
    }

    public static void modifyBigAmountOfTopics(String topicPrefix, int numberOfTopics, int numberOfPartitions, Map<String, Object> config) {
        LOGGER.info("Modify topics via Kubernetes");
        List<CompletableFuture<?>> topics = new ArrayList<>();

        for (int i = 0; i < numberOfTopics; i++){
            String currentTopicName = topicPrefix + i;
            LOGGER.info("Modify topic {}", currentTopicName);

            topics.add(CompletableFuture.runAsync(() -> {
                if (numberOfPartitions != 0){
                    KafkaTopicResource.replaceTopicResource(currentTopicName, kafkaTopic -> kafkaTopic.getSpec().setPartitions(numberOfPartitions));
                }
                if (!config.isEmpty()){
                    KafkaTopicResource.replaceTopicResource(currentTopicName, kafkaTopic -> kafkaTopic.getSpec().setConfig(config));
                }
            }));
        }
    }

    public static void modifyBigAmountOfTopics(String topicPrefix, int numberOfTopics, int numberOfPartitions) {
       KafkaTopicScalabilityUtils.modifyBigAmountOfTopics(topicPrefix, numberOfTopics, numberOfPartitions, new HashMap<>());
    }

    public static void modifyBigAmountOfTopics(String topicPrefix, int numberOfTopics, Map<String, Object> config) {
        KafkaTopicScalabilityUtils.modifyBigAmountOfTopics(topicPrefix, numberOfTopics,0, config);
    }

}
