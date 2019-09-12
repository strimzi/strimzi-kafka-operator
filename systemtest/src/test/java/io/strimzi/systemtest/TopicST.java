/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.SCALABILITY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag(REGRESSION)
public class TopicST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(TopicST.class);

    public static final String NAMESPACE = "topic-cluster-test";

    @Test
    void testMoreReplicasThanAvailableBrokers() {
        final String topicName = "topic-example";
        int topicReplicationFactor = 5;
        int topicPartitions = 5;

        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3, 1).done();
        KafkaTopic kafkaTopic =  testMethodResources().topic(CLUSTER_NAME, topicName, topicPartitions, topicReplicationFactor).done();

        assertThat("Topic exists in Kafka CR (Kubernetes)", hasTopicInCRK8s(kafkaTopic, topicName));
        assertThat("Topic doesn't exists in Kafka itself", !hasTopicInKafka(topicName));

        // Checking TO logs
        String tOPodName = cmdKubeClient().listResourcesByLabel("pod", "strimzi.io/name=my-cluster-entity-operator").get(0);
        String errorMessage = "Replication factor: 5 larger than available brokers: 3";

        StUtils.waitUntilMessageIsInLogs(tOPodName, "topic-operator", errorMessage);

        String tOlogs = kubeClient().logs(tOPodName, "topic-operator");

        assertThat(tOlogs, containsString(errorMessage));

        LOGGER.info("Delete topic {}", topicName);
        cmdKubeClient().deleteByName("kafkatopic", topicName);
        StUtils.waitForKafkaTopicDeletion(topicName);

        topicReplicationFactor = 3;

        final String newTopicName = "topic-example-new";

        kafkaTopic = testMethodResources().topic(CLUSTER_NAME, newTopicName, topicPartitions, topicReplicationFactor).done();

        TestUtils.waitFor("Waiting for " + newTopicName + " to be created in Kafka", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_TOPIC_CREATION,
            () -> listTopicsUsingPodCLI(CLUSTER_NAME, 0).contains(newTopicName)
        );

        assertThat("Topic exists in Kafka itself", hasTopicInKafka(newTopicName));
        assertThat("Topic exists in Kafka CR (Kubernetes)", hasTopicInCRK8s(kafkaTopic, newTopicName));

        LOGGER.info("Delete topic {}", newTopicName);
        cmdKubeClient().deleteByName("kafkatopic", newTopicName);
        StUtils.waitForKafkaTopicDeletion(newTopicName);
    }

    @Tag(SCALABILITY)
    @Test
    void testBigAmountOfTopicsCreatingViaK8s() {
        final String topicName = "topic-example";
        String currentTopic;
        int numberOfTopics = 50;
        int topicPartitions = 3;

        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3, 1).done();

        LOGGER.info("Creating topics via Kubernetes");
        for (int i = 0; i < numberOfTopics; i++) {
            currentTopic = topicName + i;
            testMethodResources().topic(CLUSTER_NAME, currentTopic, topicPartitions).done();
        }

        for (int i = 0; i < numberOfTopics; i++) {
            currentTopic = topicName + i;
            verifyTopicViaKafka(currentTopic, topicPartitions);
        }

        topicPartitions = 5;
        LOGGER.info("Editing topic via Kubernetes settings to partitions {}", topicPartitions);

        for (int i = 0; i < numberOfTopics; i++) {
            currentTopic = topicName + i;

            replaceTopicResource(currentTopic, topic -> topic.getSpec().setPartitions(5));
        }

        for (int i = 0; i < numberOfTopics; i++) {
            currentTopic = topicName + i;
            LOGGER.info("Waiting for kafka topic {} will change partitions to {}", currentTopic, topicPartitions);
            StUtils.waitForKafkaTopicPartitionChange(currentTopic, topicPartitions);
            verifyTopicViaKafka(currentTopic, topicPartitions);
        }

        LOGGER.info("Deleting all topics");
        for (int i = 0; i < numberOfTopics; i++) {
            currentTopic = topicName + i;
            cmdKubeClient().deleteByName("kafkatopic", currentTopic);
            StUtils.waitForKafkaTopicDeletion(currentTopic);
        }
    }

    @Tag(SCALABILITY)
    @Test
    void testBigAmountOfTopicsCreatingViaKafka() {
        final String topicName = "topic-example";
        String currentTopic;
        int numberOfTopics = 50;
        int topicPartitions = 3;

        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3, 1).done();

        for (int i = 0; i < numberOfTopics; i++) {
            currentTopic = topicName + i;
            LOGGER.info("Creating topic {} with {} replicas and {} partitions", currentTopic, 3, topicPartitions);
            createTopicUsingPodCLI(CLUSTER_NAME, 0, currentTopic, 3, topicPartitions);
        }

        for (int i = 0; i < numberOfTopics; i++) {
            currentTopic = topicName + i;
            StUtils.waitForKafkaTopicCreation(currentTopic);
            KafkaTopic kafkaTopic = testMethodResources().kafkaTopic().inNamespace(NAMESPACE).withName(currentTopic).get();
            verifyTopicViaKafkaTopicCRK8s(kafkaTopic, currentTopic, topicPartitions);
        }

        topicPartitions = 5;
        LOGGER.info("Editing topic via Kafka, settings to partitions {}", topicPartitions);

        for (int i = 0; i < numberOfTopics; i++) {
            currentTopic = topicName + i;
            updateTopicPartitionsCountUsingPodCLI(CLUSTER_NAME, 0, currentTopic, topicPartitions);
        }

        for (int i = 0; i < numberOfTopics; i++) {
            currentTopic = topicName + i;
            StUtils.waitForKafkaTopicPartitionChange(currentTopic, topicPartitions);
            verifyTopicViaKafka(currentTopic, topicPartitions);
        }

        LOGGER.info("Deleting all topics");
        for (int i = 0; i < numberOfTopics; i++) {
            currentTopic = topicName + i;
            cmdKubeClient().deleteByName("kafkatopic", currentTopic);
            StUtils.waitForKafkaTopicDeletion(currentTopic);
        }
    }

    boolean hasTopicInKafka(String topicName) {
        LOGGER.info("Checking topic {} in Kafka", topicName);
        return listTopicsUsingPodCLI(CLUSTER_NAME, 0).contains(topicName);
    }

    boolean hasTopicInCRK8s(KafkaTopic kafkaTopic, String topicName) {
        LOGGER.info("Checking in KafkaTopic CR that topic {} exists", topicName);
        return kafkaTopic.getMetadata().getName().equals(topicName);
    }

    void verifyTopicViaKafka(String topicName, int topicPartitions) {
        LOGGER.info("Checking topic in Kafka {}", describeTopicUsingPodCLI(CLUSTER_NAME, 0, topicName));
        assertThat(describeTopicUsingPodCLI(CLUSTER_NAME, 0, topicName),
                hasItems("Topic:" + topicName, "PartitionCount:" + topicPartitions));
    }

    void verifyTopicViaKafkaTopicCRK8s(KafkaTopic kafkaTopic, String topicName, int topicPartitions) {
        LOGGER.info("Checking in KafkaTopic CR that topic {} was created with expected settings", topicName);
        assertNotNull(kafkaTopic);
        assertThat(listTopicsUsingPodCLI(CLUSTER_NAME, 0), hasItem(topicName));
        assertEquals(topicName, kafkaTopic.getMetadata().getName());
        assertEquals(topicPartitions, kafkaTopic.getSpec().getPartitions());
    }

    @BeforeEach
    void createTestResources()  {
        createTestMethodResources();
    }

    @BeforeAll
    void setupEnvironment() {
        LOGGER.info("Creating resources before the test class");
        prepareEnvForOperator(NAMESPACE);

        createTestClassResources();
        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        testClassResources().clusterOperator(NAMESPACE).done();
    }

    @Override
    protected void tearDownEnvironmentAfterEach() throws Exception {
        deleteTestMethodResources();
        waitForDeletion(Constants.TIMEOUT_TEARDOWN);
    }
}
