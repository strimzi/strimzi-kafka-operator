/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.SCALABILITY;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@Tag(REGRESSION)
public class TopicST extends BaseST {

    private static final Logger LOGGER = LogManager.getLogger(TopicST.class);

    public static final String NAMESPACE = "topic-cluster-test";

    @Test
    void testMoreReplicasThanAvailableBrokers() {
        final String topicName = "topic-example";
        int topicReplicationFactor = 5;
        int topicPartitions = 5;

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1).done();
        KafkaTopic kafkaTopic =
                KafkaTopicResource.topicWithoutWait(KafkaTopicResource.defaultTopic(CLUSTER_NAME, topicName, topicPartitions, topicReplicationFactor, 1).build());

        assertThat("Topic exists in Kafka CR (Kubernetes)", hasTopicInCRK8s(kafkaTopic, topicName));
        assertThat("Topic doesn't exists in Kafka itself", !hasTopicInKafka(topicName));

        // Checking TO logs
        String tOPodName = cmdKubeClient().listResourcesByLabel("pod", Labels.STRIMZI_NAME_LABEL + "=my-cluster-entity-operator").get(0);
        String errorMessage = "Replication factor: 5 larger than available brokers: 3";

        PodUtils.waitUntilMessageIsInLogs(tOPodName, "topic-operator", errorMessage);

        String tOlogs = kubeClient().logs(tOPodName, "topic-operator");

        assertThat(tOlogs, containsString(errorMessage));

        LOGGER.info("Delete topic {}", topicName);
        cmdKubeClient().deleteByName("kafkatopic", topicName);
        KafkaTopicUtils.waitForKafkaTopicDeletion(topicName);

        topicReplicationFactor = 3;

        final String newTopicName = "topic-example-new";

        kafkaTopic = KafkaTopicResource.topic(CLUSTER_NAME, newTopicName, topicPartitions, topicReplicationFactor).done();

        TestUtils.waitFor("Waiting for " + newTopicName + " to be created in Kafka", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_TOPIC_CREATION,
            () -> KafkaCmdClient.listTopicsUsingPodCli(CLUSTER_NAME, 0).contains(newTopicName)
        );

        assertThat("Topic exists in Kafka itself", hasTopicInKafka(newTopicName));
        assertThat("Topic exists in Kafka CR (Kubernetes)", hasTopicInCRK8s(kafkaTopic, newTopicName));

        LOGGER.info("Delete topic {}", newTopicName);
        cmdKubeClient().deleteByName("kafkatopic", newTopicName);
        KafkaTopicUtils.waitForKafkaTopicDeletion(newTopicName);
    }

    @Tag(SCALABILITY)
    @Test
    void testBigAmountOfTopicsCreatingViaK8s() {
        final String topicName = "topic-example";
        String currentTopic;
        int numberOfTopics = 50;
        int topicPartitions = 3;

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1).done();

        LOGGER.info("Creating topics via Kubernetes");
        for (int i = 0; i < numberOfTopics; i++) {
            currentTopic = topicName + i;
            KafkaTopicResource.topic(CLUSTER_NAME, currentTopic, topicPartitions).done();
        }

        for (int i = 0; i < numberOfTopics; i++) {
            currentTopic = topicName + i;
            verifyTopicViaKafka(currentTopic, topicPartitions);
        }

        topicPartitions = 5;
        LOGGER.info("Editing topic via Kubernetes settings to partitions {}", topicPartitions);

        for (int i = 0; i < numberOfTopics; i++) {
            currentTopic = topicName + i;

            KafkaTopicResource.replaceTopicResource(currentTopic, topic -> topic.getSpec().setPartitions(5));
        }

        for (int i = 0; i < numberOfTopics; i++) {
            currentTopic = topicName + i;
            LOGGER.info("Waiting for kafka topic {} will change partitions to {}", currentTopic, topicPartitions);
            KafkaTopicUtils.waitForKafkaTopicPartitionChange(currentTopic, topicPartitions);
            verifyTopicViaKafka(currentTopic, topicPartitions);
        }

        LOGGER.info("Deleting all topics");
        for (int i = 0; i < numberOfTopics; i++) {
            currentTopic = topicName + i;
            cmdKubeClient().deleteByName("kafkatopic", currentTopic);
            KafkaTopicUtils.waitForKafkaTopicDeletion(currentTopic);
        }
    }

    @Tag(SCALABILITY)
    @Test
    void testBigAmountOfTopicsCreatingViaKafka() {
        final String topicName = "topic-example";
        String currentTopic;
        int numberOfTopics = 50;
        int topicPartitions = 3;

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1).done();

        for (int i = 0; i < numberOfTopics; i++) {
            currentTopic = topicName + i;
            LOGGER.info("Creating topic {} with {} replicas and {} partitions", currentTopic, 3, topicPartitions);
            KafkaCmdClient.createTopicUsingPodCli(CLUSTER_NAME, 0, currentTopic, 3, topicPartitions);
        }

        for (int i = 0; i < numberOfTopics; i++) {
            currentTopic = topicName + i;
            KafkaTopicUtils.waitForKafkaTopicCreation(currentTopic);
            KafkaTopic kafkaTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(currentTopic).get();
            verifyTopicViaKafkaTopicCRK8s(kafkaTopic, currentTopic, topicPartitions);
        }

        topicPartitions = 5;
        LOGGER.info("Editing topic via Kafka, settings to partitions {}", topicPartitions);

        for (int i = 0; i < numberOfTopics; i++) {
            currentTopic = topicName + i;
            KafkaCmdClient.updateTopicPartitionsCountUsingPodCli(CLUSTER_NAME, 0, currentTopic, topicPartitions);
        }

        for (int i = 0; i < numberOfTopics; i++) {
            currentTopic = topicName + i;
            KafkaTopicUtils.waitForKafkaTopicPartitionChange(currentTopic, topicPartitions);
            verifyTopicViaKafka(currentTopic, topicPartitions);
        }

        LOGGER.info("Deleting all topics");
        for (int i = 0; i < numberOfTopics; i++) {
            currentTopic = topicName + i;
            cmdKubeClient().deleteByName("kafkatopic", currentTopic);
            KafkaTopicUtils.waitForKafkaTopicDeletion(currentTopic);
        }
    }

    @Test
    void testTopicModificationOfReplicationFactor() {
        String topicName = "topic-with-changed-replication";

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 2, 1).done();

        KafkaTopicResource.topic(CLUSTER_NAME, topicName)
                .editSpec()
                    .withReplicas(2)
                .endSpec()
                .done();

        TestUtils.waitFor("Waiting to " + topicName + " to be ready", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_TOPIC_CREATION,
            () ->  KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(topicName).get().getStatus().getConditions().get(0).getType().equals("Ready")
        );

        KafkaTopicResource.replaceTopicResource(topicName, t -> t.getSpec().setReplicas(1));

        String exceptedMessage = "Changing 'spec.replicas' is not supported. This KafkaTopic's 'spec.replicas' should be reverted to 2 and then the replication should be changed directly in Kafka.";

        TestUtils.waitFor("Waiting for " + topicName + " to has to contains message" + exceptedMessage, Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_TOPIC_CREATION,
            () ->  KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(topicName).get().getStatus().getConditions().get(0).getMessage().contains(exceptedMessage)
        );

        String topicCRDMessage = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(topicName).get().getStatus().getConditions().get(0).getMessage();

        assertThat(topicCRDMessage, containsString(exceptedMessage));

        cmdKubeClient().deleteByName("kafkatopic", topicName);
        KafkaTopicUtils.waitForKafkaTopicDeletion(topicName);
    }

    @Test
    void testDeleteTopicEnableFalse() throws Exception {
        String topicName = "my-deleted-topic";
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 1, 1)
            .editSpec()
                .editKafka()
                    .addToConfig("delete.topic.enable", false)
                .endKafka()
            .endSpec()
            .done();

        KafkaClientsResource.deployKafkaClients(false, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).done();

        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();

        KafkaTopicUtils.waitForKafkaTopicCreation(topicName);
        LOGGER.info("Topic {} was created", topicName);

        String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();
        internalKafkaClient.setPodName(kafkaClientsPodName);

        int sent = internalKafkaClient.sendMessages(topicName, NAMESPACE, CLUSTER_NAME, 50);

        String topicUid = KafkaTopicUtils.topicSnapshot(topicName);
        LOGGER.info("Going to delete topic {}", topicName);
        KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(topicName).cascading(true).delete();
        LOGGER.info("Topic {} deleted", topicName);

        KafkaTopicUtils.waitTopicHasRolled(topicName, topicUid);

        LOGGER.info("Wait topic {} recreation", topicName);
        KafkaTopicUtils.waitForKafkaTopicCreation(topicName);
        LOGGER.info("Topic {} recreated", topicName);

        int received = internalKafkaClient.receiveMessages(topicName, NAMESPACE, CLUSTER_NAME, 50, CONSUMER_GROUP_NAME);
        assertThat(received, is(sent));

    }

    @Test
    void testSendingMessagesToNonExistingTopic() {
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1).done();

        KafkaClientsResource.deployKafkaClients(false, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).done();

        String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        internalKafkaClient.setPodName(defaultKafkaClientsPodName);

        LOGGER.info("Checking if {} is on topic list", topicName);
        boolean created = hasTopicInKafka(topicName);
        assertThat(created, is(false));
        LOGGER.info("Topic with name {} is not created yet", topicName);

        LOGGER.info("Trying to send messages to non-existing topic {}", topicName);
        internalKafkaClient.assertSentAndReceivedMessages(
                internalKafkaClient.sendMessages(topicName, NAMESPACE, CLUSTER_NAME, MESSAGE_COUNT),
                internalKafkaClient.receiveMessages(topicName, NAMESPACE, CLUSTER_NAME, MESSAGE_COUNT, CONSUMER_GROUP_NAME + "-" + rng.nextInt(Integer.MAX_VALUE))
        );

        LOGGER.info("Checking if {} is on topic list", topicName);
        created = hasTopicInKafka(topicName);
        assertThat(created, is(true));

        assertThat(KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(topicName).get().getStatus().getConditions().get(0).getType(), is("Ready"));
        LOGGER.info("Topic successfully created");
    }

    boolean hasTopicInKafka(String topicName) {
        LOGGER.info("Checking topic {} in Kafka", topicName);
        return KafkaCmdClient.listTopicsUsingPodCli(CLUSTER_NAME, 0).contains(topicName);
    }

    boolean hasTopicInCRK8s(KafkaTopic kafkaTopic, String topicName) {
        LOGGER.info("Checking in KafkaTopic CR that topic {} exists", topicName);
        return kafkaTopic.getMetadata().getName().equals(topicName);
    }

    void verifyTopicViaKafka(String topicName, int topicPartitions) {
        List<String> topicInfo = KafkaCmdClient.describeTopicUsingPodCli(CLUSTER_NAME, 0, topicName);
        LOGGER.info("Checking topic {} in Kafka {}", topicName, CLUSTER_NAME);
        LOGGER.debug("Topic {} info: {}", topicName, topicInfo);
        assertThat(topicInfo, hasItems("Topic:" + topicName, "PartitionCount:" + topicPartitions));
    }

    void verifyTopicViaKafkaTopicCRK8s(KafkaTopic kafkaTopic, String topicName, int topicPartitions) {
        LOGGER.info("Checking in KafkaTopic CR that topic {} was created with expected settings", topicName);
        assertThat(kafkaTopic, is(notNullValue()));
        assertThat(KafkaCmdClient.listTopicsUsingPodCli(CLUSTER_NAME, 0), hasItem(topicName));
        assertThat(kafkaTopic.getMetadata().getName(), is(topicName));
        assertThat(kafkaTopic.getSpec().getPartitions(), is(topicPartitions));
    }

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();
        prepareEnvForOperator(NAMESPACE);

        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        KubernetesResource.clusterOperator(NAMESPACE).done();
    }
}
