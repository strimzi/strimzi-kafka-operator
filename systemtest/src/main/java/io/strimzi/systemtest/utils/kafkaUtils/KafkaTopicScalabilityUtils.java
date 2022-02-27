package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.Crds;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.test.WaitException;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import io.strimzi.systemtest.resources.ResourceManager;
import org.junit.jupiter.api.extension.ExtensionContext;
import io.strimzi.systemtest.enums.CustomResourceStatus;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static io.strimzi.systemtest.Constants.INFRA_NAMESPACE;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;


public class KafkaTopicScalabilityUtils {
    private static final Logger LOGGER = LogManager.getLogger(KafkaTopicUtils.class);
    protected static ResourceManager resourceManager = ResourceManager.getInstance();

    public static void createTopicsViaK8s(ExtensionContext extensionContext, String clusterName, String topicPrefix,
                                          int numberOfTopics, int numberOfPartitions, int numberOfReplicas, int minInSyncReplicas) {
        LOGGER.info("Creating topics via Kubernetes");

        List<CompletableFuture> topics = new ArrayList();

        for (int i = 0; i < numberOfTopics; i++){
            String currentTopicName = topicPrefix + i;
            LOGGER.debug("Creating {}", currentTopicName);

            topics.add(CompletableFuture.runAsync(() ->
                    resourceManager.createResource(extensionContext, false, KafkaTopicTemplates.topic(
                            clusterName, currentTopicName , numberOfPartitions, numberOfReplicas, minInSyncReplicas, INFRA_NAMESPACE).build())));
        }
    }

    public static void checkTopicsState(String topicPrefix, int numberOfTopics, int sampleOffset, Enum<?> state){
        List<CompletableFuture> topics = new ArrayList();

        for (int i = 0; i < numberOfTopics; i+=sampleOffset){
            String currentTopic = topicPrefix + i;
            topics.add(CompletableFuture.runAsync(() ->{
                    KafkaTopicUtils.waitForKafkaTopicStatus(INFRA_NAMESPACE, currentTopic, state, 600_000);
            }));
        }

        CompletableFuture<Void> allTopics = CompletableFuture.allOf(topics.toArray(new CompletableFuture[0]))
                .thenRun(() -> LOGGER.info("All topics are in correct state"));

        do {
            try {
                allTopics.get();
            } catch (WaitException | ExecutionException | InterruptedException e){
                fail(e.getMessage());
            }
        } while (!allTopics.isDone());
    }

    public static void checkTopicsNotReady(String topicPrefix, int numberOfTopics, int sampleOffset) {
        LOGGER.info("Verifying that topics are in NotReady state");
        KafkaTopicScalabilityUtils.checkTopicsState(topicPrefix, numberOfTopics, sampleOffset, CustomResourceStatus.NotReady);
    }

    public static void checkTopicsReady(String topicPrefix, int numberOfTopics, int sampleOffset) {
        LOGGER.info("Verifying that topics are in Ready state");
        KafkaTopicScalabilityUtils.checkTopicsState(topicPrefix, numberOfTopics, sampleOffset, CustomResourceStatus.Ready);
    }

    public static void checkTopicConfigContains(String clusterName, String topicPrefix, int numberOfTopics, int sampleOffset, Map<String, Object> config) {
        LOGGER.info("Verifying that topics contain right config");
        List<CompletableFuture> topics = new ArrayList();

        for (int i = 0; i < numberOfTopics; i+=sampleOffset){
            String currentTopic = topicPrefix + i;
            topics.add(CompletableFuture.runAsync(() ->{
                String crds = cmdKubeClient(kubeClient().getNamespace()).exec("get", "kafkatopic", "currentTopic", "-o", "jsonpath='{.items[*].metadata.name}'").out();
                for (Map.Entry<String, Object> conf: config.entrySet() ){
                    assertThat(crds.contains(conf.getKey()));
                }

            }));
        }

        CompletableFuture<Void> allTopics = CompletableFuture.allOf(topics.toArray(new CompletableFuture[0]))
                .thenRun(() -> LOGGER.info("All topics contain right config"));

        do {
            try {
                allTopics.get();
            } catch (WaitException | ExecutionException | InterruptedException e){
                fail(e.getMessage());
            }
        } while (!allTopics.isDone());
    }

    public static void modifyTopics(String topicPrefix, int numberOfTopics,int numberOfPartitions, Map<String, Object> config) {
        LOGGER.info("Modify topics via Kubernetes");

        List<CompletableFuture> topics = new ArrayList<CompletableFuture>();

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

    public static void modifyTopics(String topicPrefix, int numberOfTopics, int numberOfPartitions) {
       KafkaTopicScalabilityUtils.modifyTopics(topicPrefix, numberOfTopics, numberOfPartitions, new HashMap<>());
    }

    public static void modifyTopics(String topicPrefix, int numberOfTopics, Map<String, Object> config) {
        KafkaTopicScalabilityUtils.modifyTopics(topicPrefix, numberOfTopics,0, config);
    }

}
