/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators.topic;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.KafkaTopicStatus;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.fail;

@Tag(REGRESSION)
public class TopicST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(TopicST.class);
    static final String NAMESPACE = "topic-cluster-test";
    private static final String TOPIC_CLUSTER_NAME = "topic-cluster-name";

    @Test
    void testMoreReplicasThanAvailableBrokers() {
        final String topicName = "topic-example";
        int topicReplicationFactor = 5;
        int topicPartitions = 5;

        KafkaTopic kafkaTopic =
                KafkaTopicResource.topicWithoutWait(KafkaTopicResource.defaultTopic(TOPIC_CLUSTER_NAME, topicName, topicPartitions, topicReplicationFactor, 1).build());

        assertThat("Topic exists in Kafka CR (Kubernetes)", hasTopicInCRK8s(kafkaTopic, topicName));
        assertThat("Topic doesn't exists in Kafka itself", !hasTopicInKafka(topicName, TOPIC_CLUSTER_NAME));

        String errorMessage = "Replication factor: 5 larger than available brokers: 3";

        KafkaTopicUtils.waitForKafkaTopicNotReady(topicName);
        KafkaTopicStatus kafkaTopicStatus = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(topicName).get().getStatus();

        assertThat(kafkaTopicStatus.getConditions().get(0).getMessage(), containsString(errorMessage));
        assertThat(kafkaTopicStatus.getConditions().get(0).getReason(), containsString("InvalidReplicationFactorException"));

        LOGGER.info("Delete topic {}", topicName);
        cmdKubeClient().deleteByName("kafkatopic", topicName);
        KafkaTopicUtils.waitForKafkaTopicDeletion(topicName);

        topicReplicationFactor = 3;

        final String newTopicName = "topic-example-new";

        kafkaTopic = KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(TOPIC_CLUSTER_NAME, newTopicName, topicPartitions, topicReplicationFactor).build());

        assertThat("Topic exists in Kafka itself", hasTopicInKafka(newTopicName, TOPIC_CLUSTER_NAME));
        assertThat("Topic exists in Kafka CR (Kubernetes)", hasTopicInCRK8s(kafkaTopic, newTopicName));
    }

    @Test
    void testCreateTopicViaKafka() {
        int topicPartitions = 3;

        LOGGER.debug("Creating topic {} with {} replicas and {} partitions", TOPIC_NAME, 3, topicPartitions);
        KafkaCmdClient.createTopicUsingPodCli(TOPIC_CLUSTER_NAME, 0, TOPIC_NAME, 3, topicPartitions);

        KafkaTopic kafkaTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(TOPIC_NAME).get();

        verifyTopicViaKafkaTopicCRK8s(kafkaTopic, TOPIC_NAME, topicPartitions, TOPIC_CLUSTER_NAME);

        topicPartitions = 5;
        LOGGER.info("Editing topic via Kafka, settings to partitions {}", topicPartitions);

        KafkaCmdClient.updateTopicPartitionsCountUsingPodCli(TOPIC_CLUSTER_NAME, 0, TOPIC_NAME, topicPartitions);
        LOGGER.debug("Topic {} updated from {} to {} partitions", TOPIC_NAME, 3, topicPartitions);

        KafkaTopicUtils.waitForKafkaTopicPartitionChange(TOPIC_NAME, topicPartitions);
        verifyTopicViaKafka(TOPIC_NAME, topicPartitions, TOPIC_CLUSTER_NAME);
    }

    @Tag(NODEPORT_SUPPORTED)
    @Test
    void testCreateTopicViaAdminClient() throws ExecutionException, InterruptedException, TimeoutException {
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(clusterName, 3, 3)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9094)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(false)
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        Properties properties = new Properties();

        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaResource.kafkaClient().inNamespace(NAMESPACE)
            .withName(clusterName).get().getStatus().getListeners().stream()
            .filter(listener -> listener.getType().equals(Constants.EXTERNAL_LISTENER_DEFAULT_NAME))
            .findFirst()
            .orElseThrow(RuntimeException::new)
            .getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(properties)) {

            Set<String> topics = adminClient.listTopics().names().get(Constants.GLOBAL_CLIENTS_TIMEOUT, TimeUnit.MILLISECONDS);
            int topicsSize = topics.size(); // new KafkaStreamsTopicStore has topology input topics

            LOGGER.info("Creating async topic {} via Admin client", TOPIC_NAME);
            CreateTopicsResult crt = adminClient.createTopics(singletonList(new NewTopic(TOPIC_NAME, 1, (short) 1)));
            crt.all().get();

            topics = adminClient.listTopics().names().get(Constants.GLOBAL_CLIENTS_TIMEOUT, TimeUnit.MILLISECONDS);

            LOGGER.info("Verify that in Kafka cluster contains {} topics", topicsSize + 1);
            assertThat(topics.size(), is(topicsSize + 1));
            assertThat(topics.contains(TOPIC_NAME), is(true));

            KafkaTopicUtils.waitForKafkaTopicCreation(TOPIC_NAME);
            KafkaTopicUtils.waitForKafkaTopicReady(TOPIC_NAME);
        }

        KafkaTopic kafkaTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(TOPIC_NAME).get();
        ResourceManager.getPointerResources().push(() -> ResourceManager.deleteLater(KafkaTopicResource.kafkaTopicClient(), kafkaTopic));

        LOGGER.info("Verify that corresponding {} KafkaTopic custom resources were created and topic is in Ready state", 1);
        assertThat(kafkaTopic.getStatus().getConditions().get(0).getType(), is(Ready.toString()));
        assertThat(kafkaTopic.getSpec().getPartitions(), is(1));
        assertThat(kafkaTopic.getSpec().getReplicas(), is(1));
    }

    @Tag(NODEPORT_SUPPORTED)
    @Test
    void testCreateDeleteCreate() throws InterruptedException {
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(clusterName, 3, 3)
                .editSpec()
                    .editKafka()
                        .withNewListeners()
                            .addNewGenericKafkaListener()
                                .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(false)
                            .endGenericKafkaListener()
                        .endListeners()
                    .endKafka()
                    .editEntityOperator()
                        .editTopicOperator()
                            .withReconciliationIntervalSeconds(120)
                        .endTopicOperator()
                    .endEntityOperator()
                .endSpec()
                .build());

        Properties properties = new Properties();

        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaResource.kafkaClient().inNamespace(NAMESPACE)
                .withName(clusterName).get().getStatus().getListeners().stream()
                .filter(listener -> listener.getType().equals(Constants.EXTERNAL_LISTENER_DEFAULT_NAME))
                .findFirst()
                .orElseThrow(RuntimeException::new)
                .getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(properties)) {

            String topicName = "topic-create-delete-create";

            KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(clusterName, topicName)
                    .editSpec()
                        .withReplicas(3)
                    .endSpec()
                    .build());
            KafkaTopicUtils.waitForKafkaTopicReady(topicName);

            adminClient.describeTopics(singletonList(topicName)).values().get(topicName);

            for (int i = 0; i < 10; i++) {
                Thread.sleep(2_000);
                LOGGER.info("Iteration {}: Deleting {}", i, topicName);
                cmdKubeClient().deleteByName(KafkaTopic.RESOURCE_KIND, topicName);
                KafkaTopicUtils.waitForKafkaTopicDeletion(topicName);
                TestUtils.waitFor("Deletion of topic " + topicName, 1000, 15_000, () -> {
                    try {
                        return !adminClient.listTopics().names().get().contains(topicName);
                    } catch (ExecutionException | InterruptedException e) {
                        return false;
                    }
                });
                Thread.sleep(2_000);
                long t0 = System.currentTimeMillis();
                LOGGER.info("Iteration {}: Recreating {}", i, topicName);
                KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(clusterName, topicName)
                        .editSpec()
                            .withReplicas(3)
                        .endSpec()
                        .build());
                ResourceManager.waitForResourceStatus(KafkaTopicResource.kafkaTopicClient(), "KafkaTopic", NAMESPACE, topicName, Ready, 15_000);
                TestUtils.waitFor("Recreation of topic " + topicName, 1000, 2_000, () -> {
                    try {
                        return adminClient.listTopics().names().get().contains(topicName);
                    } catch (ExecutionException | InterruptedException e) {
                        return false;
                    }
                });
                if (System.currentTimeMillis() - t0 > 10_000) {
                    fail("Took too long to recreate");
                }
            }
        }
    }

    @Test
    void testTopicModificationOfReplicationFactor() {
        String topicName = "topic-with-changed-replication";

        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(TOPIC_CLUSTER_NAME, topicName)
                .editSpec()
                    .withReplicas(3)
                .endSpec()
                .build());

        KafkaTopicResource.replaceTopicResource(topicName, t -> t.getSpec().setReplicas(1));
        KafkaTopicUtils.waitForKafkaTopicNotReady(topicName);

        String exceptedMessage = "Changing 'spec.replicas' is not supported. This KafkaTopic's 'spec.replicas' should be reverted to 3 and then the replication should be changed directly in Kafka.";
        assertThat(KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(topicName).get().getStatus().getConditions().get(0).getMessage().contains(exceptedMessage), is(true));

        String topicCRDMessage = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(topicName).get().getStatus().getConditions().get(0).getMessage();

        assertThat(topicCRDMessage, containsString(exceptedMessage));

        cmdKubeClient().deleteByName(KafkaTopic.RESOURCE_SINGULAR, topicName);
        KafkaTopicUtils.waitForKafkaTopicDeletion(topicName);
    }

    @Test
    @Tag(INTERNAL_CLIENTS_USED)
    void testSendingMessagesToNonExistingTopic() {
        int sent = 0;

        KafkaClientsResource.createAndWaitForReadiness(KafkaClientsResource.deployKafkaClients(false, TOPIC_CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).build());

        String defaultKafkaClientsPodName =
                ResourceManager.kubeClient().listPodsByPrefixInName(TOPIC_CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(defaultKafkaClientsPodName)
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(TOPIC_CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        LOGGER.info("Checking if {} is on topic list", TOPIC_NAME);
        boolean created = hasTopicInKafka(TOPIC_NAME, TOPIC_CLUSTER_NAME);
        assertThat(created, is(false));
        LOGGER.info("Topic with name {} is not created yet", TOPIC_NAME);

        LOGGER.info("Trying to send messages to non-existing topic {}", TOPIC_NAME);
        // Try produce multiple times in case first try will fail because topic is not exists yet
        for (int retry = 0; retry < 3; retry++) {
            sent = internalKafkaClient.sendMessagesPlain();
            if (MESSAGE_COUNT == sent) {
                break;
            }
        }

        assertThat(sent, greaterThan(0));

        internalKafkaClient.assertSentAndReceivedMessages(
                sent,
                internalKafkaClient.receiveMessagesPlain()
        );

        LOGGER.info("Checking if {} is on topic list", TOPIC_NAME);
        created = hasTopicInKafka(TOPIC_NAME, TOPIC_CLUSTER_NAME);
        assertThat(created, is(true));

        KafkaTopic kafkaTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(TOPIC_NAME).get();
        assertThat(kafkaTopic, notNullValue());
        ResourceManager.getPointerResources().push(() -> ResourceManager.deleteLater(KafkaTopicResource.kafkaTopicClient(), kafkaTopic));

        assertThat(kafkaTopic.getStatus(), notNullValue());
        assertThat(kafkaTopic.getStatus().getConditions(), notNullValue());
        assertThat(kafkaTopic.getStatus().getConditions().isEmpty(), is(false));
        assertThat(kafkaTopic.getStatus().getConditions().get(0).getType(), is(Ready.toString()));
        LOGGER.info("Topic successfully created");
    }

    @Test
    @Tag(INTERNAL_CLIENTS_USED)
    void testDeleteTopicEnableFalse() {
        String topicName = "my-deleted-topic";
        String isolatedKafkaCluster = clusterName + "-isolated";

        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(isolatedKafkaCluster, 1, 1)
            .editSpec()
                .editKafka()
                    .addToConfig("delete.topic.enable", false)
                .endKafka()
            .endSpec()
            .build());

        KafkaClientsResource.createAndWaitForReadiness(KafkaClientsResource.deployKafkaClients(false, isolatedKafkaCluster + "-" + Constants.KAFKA_CLIENTS).build());

        KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(isolatedKafkaCluster, topicName).build());

        String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(isolatedKafkaCluster + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(NAMESPACE)
            .withClusterName(isolatedKafkaCluster)
            .withMessageCount(MESSAGE_COUNT)
            .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
            .build();

        int sent = internalKafkaClient.sendMessagesPlain();

        String topicUid = KafkaTopicUtils.topicSnapshot(topicName);
        LOGGER.info("Going to delete topic {}", topicName);
        KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(topicName).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        LOGGER.info("Topic {} deleted", topicName);

        KafkaTopicUtils.waitTopicHasRolled(topicName, topicUid);

        LOGGER.info("Wait topic {} recreation", topicName);
        KafkaTopicUtils.waitForKafkaTopicCreation(topicName);
        LOGGER.info("Topic {} recreated", topicName);

        int received = internalKafkaClient.receiveMessagesPlain();
        assertThat(received, is(sent));
    }

    @Test
    void testCreateTopicAfterUnsupportedOperation() {
        String topicName = "topic-with-replication-to-change";
        String newTopicName = "another-topic";

        KafkaTopic kafkaTopic = KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(TOPIC_CLUSTER_NAME, topicName)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(3)
                .endSpec()
                .build());

        KafkaTopicResource.replaceTopicResource(topicName, t -> {
            t.getSpec().setReplicas(1);
            t.getSpec().setPartitions(1);
        });
        KafkaTopicUtils.waitForKafkaTopicNotReady(topicName);

        String exceptedMessage = "Number of partitions cannot be decreased";
        assertThat(KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(topicName).get().getStatus().getConditions().get(0).getMessage(), is(exceptedMessage));

        String topicCRDMessage = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(topicName).get().getStatus().getConditions().get(0).getMessage();

        assertThat(topicCRDMessage, containsString(exceptedMessage));

        KafkaTopic newKafkaTopic = KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(TOPIC_CLUSTER_NAME, newTopicName, 1, 1).build());

        assertThat("Topic exists in Kafka itself", hasTopicInKafka(topicName, TOPIC_CLUSTER_NAME));
        assertThat("Topic exists in Kafka CR (Kubernetes)", hasTopicInCRK8s(kafkaTopic, topicName));
        assertThat("Topic exists in Kafka itself", hasTopicInKafka(newTopicName, TOPIC_CLUSTER_NAME));
        assertThat("Topic exists in Kafka CR (Kubernetes)", hasTopicInCRK8s(newKafkaTopic, newTopicName));

        cmdKubeClient().deleteByName(KafkaTopic.RESOURCE_SINGULAR, topicName);
        KafkaTopicUtils.waitForKafkaTopicDeletion(topicName);
        cmdKubeClient().deleteByName(KafkaTopic.RESOURCE_SINGULAR, newTopicName);
        KafkaTopicUtils.waitForKafkaTopicDeletion(newTopicName);
    }

    boolean hasTopicInKafka(String topicName, String clusterName) {
        LOGGER.info("Checking topic {} in Kafka", topicName);
        return KafkaCmdClient.listTopicsUsingPodCli(clusterName, 0).contains(topicName);
    }

    boolean hasTopicInCRK8s(KafkaTopic kafkaTopic, String topicName) {
        LOGGER.info("Checking in KafkaTopic CR that topic {} exists", topicName);
        return kafkaTopic.getMetadata().getName().equals(topicName);
    }

    void verifyTopicViaKafka(String topicName, int topicPartitions, String clusterName) {
        TestUtils.waitFor("Describing topic " + topicName + " using pod CLI", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.GLOBAL_TIMEOUT,
            () -> {
                try {
                    List<String> topicInfo =  KafkaCmdClient.describeTopicUsingPodCli(clusterName, 0, topicName);
                    LOGGER.info("Checking topic {} in Kafka {}", topicName, clusterName);
                    LOGGER.debug("Topic {} info: {}", topicName, topicInfo);
                    assertThat(topicInfo, hasItems("Topic:" + topicName, "PartitionCount:" + topicPartitions));
                    return true;
                } catch (KubeClusterException e) {
                    LOGGER.info("Describing topic using pod cli occurred following error:{}", e.getMessage());
                    return false;
                }
            });
    }

    void verifyTopicViaKafkaTopicCRK8s(KafkaTopic kafkaTopic, String topicName, int topicPartitions, String clusterName) {
        LOGGER.info("Checking in KafkaTopic CR that topic {} was created with expected settings", topicName);
        assertThat(kafkaTopic, is(notNullValue()));
        assertThat(KafkaCmdClient.listTopicsUsingPodCli(clusterName, 0), hasItem(topicName));
        assertThat(kafkaTopic.getMetadata().getName(), is(topicName));
        assertThat(kafkaTopic.getSpec().getPartitions(), is(topicPartitions));
    }

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE);

        LOGGER.info("Deploying shared kafka across all test cases in {} namespace", NAMESPACE);
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaEphemeral(TOPIC_CLUSTER_NAME, 3, 1).build());
    }
}
