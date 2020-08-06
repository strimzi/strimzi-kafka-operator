/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Random;

import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaTopicUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaTopicUtils.class);
    private static final String TOPIC_NAME_PREFIX = "my-topic-";
    private static final long READINESS_TIMEOUT = ResourceOperation.getTimeoutForResourceReadiness(KafkaTopic.RESOURCE_KIND);
    private static final long DELETION_TIMEOUT = ResourceOperation.getTimeoutForResourceDeletion();

    private KafkaTopicUtils() {}

    /**
     * Generated random name for the KafkaTopic resource
     * @return random name with additional salt
     */
    public static String generateRandomNameOfTopic() {
        String salt = new Random().nextInt(Integer.MAX_VALUE) + "-" + new Random().nextInt(Integer.MAX_VALUE);

        return  TOPIC_NAME_PREFIX + salt;
    }

    /**
     * Method which return UID for specific topic
     * @param topicName topic name
     * @return topic UID
     */
    public static String topicSnapshot(String topicName) {
        return KafkaTopicResource.kafkaTopicClient().inNamespace(kubeClient().getNamespace()).withName(topicName).get().getMetadata().getUid();
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
        TestUtils.waitFor("KafkaTopic creation " + topicName, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, READINESS_TIMEOUT,
            () -> KafkaTopicResource.kafkaTopicClient().inNamespace(kubeClient().getNamespace())
                    .withName(topicName).get().getStatus().getConditions().get(0).getType().equals(Ready.toString()),
            () -> LOGGER.info(KafkaTopicResource.kafkaTopicClient().inNamespace(kubeClient().getNamespace()).withName(topicName).get())
        );
    }

    public static void waitForKafkaTopicCreationByNamePrefix(String topicNamePrefix) {
        LOGGER.info("Waiting for KafkaTopic {} creation", topicNamePrefix);
        TestUtils.waitFor("KafkaTopic creation " + topicNamePrefix, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, READINESS_TIMEOUT,
            () -> KafkaTopicResource.kafkaTopicClient().inNamespace(kubeClient().getNamespace()).list().getItems().stream()
                    .filter(topic -> topic.getMetadata().getName().contains(topicNamePrefix))
                    .findFirst().get().getStatus().getConditions().get(0).getType().equals(Ready.toString())
        );
    }

    public static void waitForKafkaTopicDeletion(String topicName) {
        LOGGER.info("Waiting for KafkaTopic {} deletion", topicName);
        TestUtils.waitFor("KafkaTopic deletion " + topicName, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, DELETION_TIMEOUT,
            () -> KafkaTopicResource.kafkaTopicClient().inNamespace(kubeClient().getNamespace()).withName(topicName).get() == null,
            () -> LOGGER.info(KafkaTopicResource.kafkaTopicClient().inNamespace(kubeClient().getNamespace()).withName(topicName).get())
        );
    }

    public static void waitForKafkaTopicPartitionChange(String topicName, int partitions) {
        LOGGER.info("Waiting for KafkaTopic change {}", topicName);
        TestUtils.waitFor("KafkaTopic change " + topicName, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.GLOBAL_TIMEOUT,
            () -> KafkaTopicResource.kafkaTopicClient().inNamespace(kubeClient().getNamespace()).withName(topicName).get().getSpec().getPartitions() == partitions,
            () -> LOGGER.info(KafkaTopicResource.kafkaTopicClient().inNamespace(kubeClient().getNamespace()).withName(topicName).get())
        );
    }

    /**
     * Wait until KafkaTopic is in desired status
     * @param topicName name of KafkaTopic
     * @param state desired state
     */
    public static void waitForKafkaTopicStatus(String topicName, Enum<?>  state) {
        KafkaTopic kafkaTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(kubeClient().getNamespace()).withName(topicName).get();
        ResourceManager.waitForResourceStatus(KafkaTopicResource.kafkaTopicClient(), kafkaTopic, state);
    }

    public static void waitForKafkaTopicReady(String topicName) {
        waitForKafkaTopicStatus(topicName, Ready);
    }

    public static void waitForKafkaTopicNotReady(String topicName) {
        waitForKafkaTopicStatus(topicName, NotReady);
    }

    public static void waitForKafkaTopicsCount(int topicCount, String clusterName) {
        LOGGER.info("Wait until we create {} KafkaTopics", topicCount);
        TestUtils.waitFor(topicCount + " KafkaTopics creation",
            Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> KafkaCmdClient.listTopicsUsingPodCli(clusterName, 0).size() == topicCount);
        LOGGER.info("{} KafkaTopics were created", topicCount);
    }
}
