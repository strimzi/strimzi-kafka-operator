/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators.topic;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.KafkaTopicStatus;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.HashSet;
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Tag(REGRESSION)
@ParallelSuite
@KRaftNotSupported("TopicOperator is not supported by KRaft mode and is used in this test class")
public class TopicST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(TopicST.class);
    private static final String TOPIC_CLUSTER_NAME = "topic-cluster-name";
    private static final String SCRAPER_NAME = TOPIC_CLUSTER_NAME + "-" + Constants.SCRAPER_NAME;

    private final String namespace = testSuiteNamespaceManager.getMapOfAdditionalNamespaces().get(TopicST.class.getSimpleName()).stream().findFirst().get();
    private String scraperPodName;

    @ParallelTest
    void testMoreReplicasThanAvailableBrokers(ExtensionContext extensionContext) {
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        int topicReplicationFactor = 5;
        int topicPartitions = 5;

        KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(TOPIC_CLUSTER_NAME, topicName, topicPartitions, topicReplicationFactor, 1, namespace).build();
        resourceManager.createResource(extensionContext, false, kafkaTopic);

        assertThat("Topic exists in Kafka CR (Kubernetes)", hasTopicInCRK8s(kafkaTopic, topicName));
        assertThat("Topic doesn't exists in Kafka itself", !hasTopicInKafka(topicName, TOPIC_CLUSTER_NAME));

        String errorMessage = "org.apache.kafka.common.errors.InvalidReplicationFactorException: Replication factor: 5 larger than available brokers: 3";

        KafkaTopicUtils.waitForKafkaTopicNotReady(namespace, topicName);
        KafkaTopicStatus kafkaTopicStatus = KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).withName(topicName).get().getStatus();

        assertThat(kafkaTopicStatus.getConditions().get(0).getMessage(), containsString(errorMessage));
        assertThat(kafkaTopicStatus.getConditions().get(0).getReason(), containsString("CompletionException"));

        LOGGER.info("Delete topic {}", topicName);
        cmdKubeClient(namespace).deleteByName("kafkatopic", topicName);
        KafkaTopicUtils.waitForKafkaTopicDeletion(namespace, topicName);

        topicReplicationFactor = 3;
        final String newTopicName = "topic-example-new";

        kafkaTopic = KafkaTopicTemplates.topic(TOPIC_CLUSTER_NAME, newTopicName, topicPartitions, topicReplicationFactor)
            .editMetadata()
                .withNamespace(namespace)
            .endMetadata()
            .build();
        resourceManager.createResource(extensionContext, kafkaTopic);

        assertThat("Topic exists in Kafka itself", hasTopicInKafka(newTopicName, TOPIC_CLUSTER_NAME));
        assertThat("Topic exists in Kafka CR (Kubernetes)", hasTopicInCRK8s(kafkaTopic, newTopicName));
    }

    @ParallelTest
    void testCreateTopicViaKafka(ExtensionContext extensionContext) {
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        int topicPartitions = 3;

        LOGGER.debug("Creating topic {} with {} replicas and {} partitions", topicName, 3, topicPartitions);
        KafkaCmdClient.createTopicUsingPodCli(namespace, scraperPodName, KafkaResources.plainBootstrapAddress(TOPIC_CLUSTER_NAME), topicName, 3, topicPartitions);

        KafkaTopic kafkaTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).withName(topicName).get();

        verifyTopicViaKafkaTopicCRK8s(kafkaTopic, topicName, topicPartitions, TOPIC_CLUSTER_NAME);

        topicPartitions = 5;
        LOGGER.info("Editing topic via Kafka, settings to partitions {}", topicPartitions);

        KafkaCmdClient.updateTopicPartitionsCountUsingPodCli(namespace, scraperPodName, KafkaResources.plainBootstrapAddress(TOPIC_CLUSTER_NAME), topicName, topicPartitions);
        LOGGER.debug("Topic {} updated from {} to {} partitions", topicName, 3, topicPartitions);

        KafkaTopicUtils.waitForKafkaTopicPartitionChange(namespace, topicName, topicPartitions);
        verifyTopicViaKafka(namespace, topicName, topicPartitions, TOPIC_CLUSTER_NAME);
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(NODEPORT_SUPPORTED)
    void testCreateTopicViaAdminClient(ExtensionContext extensionContext) throws ExecutionException, InterruptedException, TimeoutException {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 3)
            .editMetadata()
                .withNamespace(namespace)
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                            .withPort(9094)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(false)
                            .build())
                .endKafka()
            .endSpec()
            .build());

        Properties properties = new Properties();

        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaResource.kafkaClient().inNamespace(namespace)
            .withName(clusterName).get().getStatus().getListeners().stream()
            .filter(listener -> listener.getType().equals(Constants.EXTERNAL_LISTENER_DEFAULT_NAME))
            .findFirst()
            .orElseThrow(RuntimeException::new)
            .getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(properties)) {

            Set<String> topics = adminClient.listTopics().names().get(Constants.GLOBAL_CLIENTS_TIMEOUT, TimeUnit.MILLISECONDS);
            int topicsSize = topics.size(); // new KafkaStreamsTopicStore has topology input topics

            LOGGER.info("Creating async topic {} via Admin client", topicName);
            CreateTopicsResult crt = adminClient.createTopics(singletonList(new NewTopic(topicName, 1, (short) 1)));
            crt.all().get();

            TestUtils.waitFor("Wait until Kafka cluster has " + (topicsSize + 1) + " KafkaTopic", Constants.GLOBAL_POLL_INTERVAL,
                Constants.GLOBAL_TIMEOUT, () -> {
                    Set<String> updatedKafkaTopics = new HashSet<>();
                    try {
                        updatedKafkaTopics = adminClient.listTopics().names().get(Constants.GLOBAL_CLIENTS_TIMEOUT, TimeUnit.MILLISECONDS);
                        LOGGER.info("Verify that in Kafka cluster contains {} topics", topicsSize + 1);

                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                        e.printStackTrace();
                    }
                    return updatedKafkaTopics.size() == topicsSize + 1 && updatedKafkaTopics.contains(topicName);

                });

            KafkaTopicUtils.waitForKafkaTopicCreation(namespace, topicName);
            KafkaTopicUtils.waitForKafkaTopicReady(namespace, topicName);
        }

        KafkaTopic kafkaTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).withName(topicName).get();

        LOGGER.info("Verify that corresponding {} KafkaTopic custom resources were created and topic is in Ready state", 1);
        assertThat(kafkaTopic.getStatus().getConditions().get(0).getType(), is(Ready.toString()));
        assertThat(kafkaTopic.getSpec().getPartitions(), is(1));
        assertThat(kafkaTopic.getSpec().getReplicas(), is(1));
    }

    @Tag(NODEPORT_SUPPORTED)
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testCreateDeleteCreate(ExtensionContext extensionContext) throws InterruptedException {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 3)
            .editMetadata()
                .withNamespace(namespace)
            .endMetadata()
            .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName(Constants.EXTERNAL_LISTENER_DEFAULT_NAME)
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(false)
                                .build())
                    .endKafka()
                    .editEntityOperator()
                        .editTopicOperator()
                            .withReconciliationIntervalSeconds(120)
                        .endTopicOperator()
                    .endEntityOperator()
                .endSpec()
                .build());

        Properties properties = new Properties();

        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaResource.kafkaClient().inNamespace(namespace)
                .withName(clusterName).get().getStatus().getListeners().stream()
                .filter(listener -> listener.getType().equals(Constants.EXTERNAL_LISTENER_DEFAULT_NAME))
                .findFirst()
                .orElseThrow(RuntimeException::new)
                .getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(properties)) {

            String topicName = KafkaTopicUtils.generateRandomNameOfTopic();

            resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName, namespace)
                .editSpec()
                    .withReplicas(3)
                .endSpec()
                .build());
            KafkaTopicUtils.waitForKafkaTopicReady(namespace, topicName);

            adminClient.describeTopics(singletonList(topicName)).topicNameValues().get(topicName);

            for (int i = 0; i < 10; i++) {
                Thread.sleep(2_000);
                LOGGER.info("Iteration {}: Deleting {}", i, topicName);
                cmdKubeClient(namespace).deleteByName(KafkaTopic.RESOURCE_KIND, topicName);
                KafkaTopicUtils.waitForKafkaTopicDeletion(namespace, topicName);
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
                resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName, namespace)
                    .editSpec()
                        .withReplicas(3)
                    .endSpec()
                    .build());
                ResourceManager.waitForResourceStatus(KafkaTopicResource.kafkaTopicClient(), "KafkaTopic", namespace, topicName, Ready, 15_000);
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

    @ParallelTest
    @Disabled("TopicOperator allows forbidden settings - https://github.com/strimzi/strimzi-kafka-operator/issues/6884")
    void testTopicModificationOfReplicationFactor(ExtensionContext extensionContext) {
        String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(TOPIC_CLUSTER_NAME, topicName, namespace)
            .editSpec()
                .withReplicas(3)
            .endSpec()
            .build());

        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName, t -> t.getSpec().setReplicas(1), namespace);
        KafkaTopicUtils.waitForKafkaTopicNotReady(namespace, topicName);

        String exceptedMessage = "Changing 'spec.replicas' is not supported. This KafkaTopic's 'spec.replicas' should be reverted to 3 and then the replication should be changed directly in Kafka.";
        assertThat(KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).withName(topicName).get().getStatus().getConditions().get(0).getMessage().contains(exceptedMessage), is(true));

        String topicCRDMessage = KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).withName(topicName).get().getStatus().getConditions().get(0).getMessage();

        assertThat(topicCRDMessage, containsString(exceptedMessage));

        cmdKubeClient(namespace).deleteByName(KafkaTopic.RESOURCE_SINGULAR, topicName);
        KafkaTopicUtils.waitForKafkaTopicDeletion(namespace, topicName);
    }

    @ParallelTest
    @Tag(INTERNAL_CLIENTS_USED)
    void testSendingMessagesToNonExistingTopic(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, namespace);

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(TOPIC_CLUSTER_NAME))
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .build();

        LOGGER.info("Checking if {} is on topic list", testStorage.getTopicName());
        assertFalse(hasTopicInKafka(testStorage.getTopicName(), TOPIC_CLUSTER_NAME));
        LOGGER.info("Topic with name {} is not created yet", testStorage.getTopicName());

        LOGGER.info("Trying to send messages to non-existing topic {}", testStorage.getTopicName());

        resourceManager.createResource(extensionContext, clients.producerStrimzi(), clients.consumerStrimzi());
        ClientUtils.waitForClientsSuccess(testStorage);

        LOGGER.info("Checking if {} is on topic list", testStorage.getTopicName());
        assertTrue(hasTopicInKafka(testStorage.getTopicName(), TOPIC_CLUSTER_NAME));

        KafkaTopicUtils.waitForKafkaTopicCreation(namespace, testStorage.getTopicName());
        KafkaTopic kafkaTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).withName(testStorage.getTopicName()).get();
        assertThat(kafkaTopic, notNullValue());

        assertThat(kafkaTopic.getStatus(), notNullValue());
        assertThat(kafkaTopic.getStatus().getConditions(), notNullValue());
        assertThat(kafkaTopic.getStatus().getConditions().isEmpty(), is(false));
        assertThat(kafkaTopic.getStatus().getConditions().get(0).getType(), is(Ready.toString()));
        LOGGER.info("Topic successfully created");
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(INTERNAL_CLIENTS_USED)
    void testDeleteTopicEnableFalse(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, namespace);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 1, 1)
            .editMetadata()
                .withNamespace(namespace)
            .endMetadata()
            .editSpec()
                .editKafka()
                    .addToConfig("delete.topic.enable", false)
                .endKafka()
            .endSpec()
            .build());

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .build();

        resourceManager.createResource(extensionContext, clients.producerStrimzi());
        ClientUtils.waitForProducerClientSuccess(testStorage);

        String topicUid = KafkaTopicUtils.topicSnapshot(namespace, testStorage.getTopicName());
        LOGGER.info("Deleting KafkaTopic: {}", testStorage.getTopicName());
        KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).withName(testStorage.getTopicName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        LOGGER.info("KafkaTopic {} deleted", testStorage.getTopicName());

        KafkaTopicUtils.waitTopicHasRolled(namespace, testStorage.getTopicName(), topicUid);

        LOGGER.info("Wait KafkaTopic {} recreation", testStorage.getTopicName());
        KafkaTopicUtils.waitForKafkaTopicCreation(namespace, testStorage.getTopicName());
        LOGGER.info("KafkaTopic {} recreated", testStorage.getTopicName());

        resourceManager.createResource(extensionContext, clients.consumerStrimzi());
        ClientUtils.waitForConsumerClientSuccess(testStorage);
    }

    @ParallelTest
    @Disabled("TopicOperator allows forbidden settings - https://github.com/strimzi/strimzi-kafka-operator/issues/6884")
    void testCreateTopicAfterUnsupportedOperation(ExtensionContext extensionContext) {
        String topicName = "topic-with-replication-to-change";
        String newTopicName = "another-topic";

        KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(TOPIC_CLUSTER_NAME, topicName, namespace)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(3)
                .endSpec()
                .build();

        resourceManager.createResource(extensionContext, kafkaTopic);

        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName, t -> {
            t.getSpec().setReplicas(1);
            t.getSpec().setPartitions(1);
        }, namespace);
        KafkaTopicUtils.waitForKafkaTopicNotReady(namespace, topicName);

        String exceptedMessage = "Number of partitions cannot be decreased";
        assertThat(KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).withName(topicName).get().getStatus().getConditions().get(0).getMessage(), is(exceptedMessage));

        String topicCRDMessage = KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).withName(topicName).get().getStatus().getConditions().get(0).getMessage();

        assertThat(topicCRDMessage, containsString(exceptedMessage));

        KafkaTopic newKafkaTopic = KafkaTopicTemplates.topic(TOPIC_CLUSTER_NAME, newTopicName, 1, 1)
            .editMetadata()
                .withNamespace(namespace)
            .endMetadata()
            .build();

        resourceManager.createResource(extensionContext, newKafkaTopic);

        assertThat("Topic exists in Kafka itself", hasTopicInKafka(topicName, TOPIC_CLUSTER_NAME));
        assertThat("Topic exists in Kafka CR (Kubernetes)", hasTopicInCRK8s(kafkaTopic, topicName));
        assertThat("Topic exists in Kafka itself", hasTopicInKafka(newTopicName, TOPIC_CLUSTER_NAME));
        assertThat("Topic exists in Kafka CR (Kubernetes)", hasTopicInCRK8s(newKafkaTopic, newTopicName));

        cmdKubeClient(namespace).deleteByName(KafkaTopic.RESOURCE_SINGULAR, topicName);
        KafkaTopicUtils.waitForKafkaTopicDeletion(namespace, topicName);
        cmdKubeClient(namespace).deleteByName(KafkaTopic.RESOURCE_SINGULAR, newTopicName);
        KafkaTopicUtils.waitForKafkaTopicDeletion(namespace, newTopicName);
    }

    boolean hasTopicInKafka(String topicName, String clusterName) {
        LOGGER.info("Checking topic {} in Kafka", topicName);
        return KafkaCmdClient.listTopicsUsingPodCli(namespace, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName)).contains(topicName);
    }

    boolean hasTopicInCRK8s(KafkaTopic kafkaTopic, String topicName) {
        LOGGER.info("Checking in KafkaTopic CR that topic {} exists", topicName);
        return kafkaTopic.getMetadata().getName().equals(topicName);
    }

    void verifyTopicViaKafka(final String namespaceName, String topicName, int topicPartitions, String clusterName) {
        TestUtils.waitFor("Describing topic " + topicName + " using pod CLI", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.GLOBAL_TIMEOUT,
            () -> {
                try {
                    String topicInfo =  KafkaCmdClient.describeTopicUsingPodCli(namespaceName, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName), topicName);
                    LOGGER.info("Checking topic {} in Kafka {}", topicName, clusterName);
                    LOGGER.debug("Topic {} info: {}", topicName, topicInfo);
                    assertThat(topicInfo, containsString("Topic: " + topicName));
                    assertThat(topicInfo, containsString("PartitionCount: " + topicPartitions));
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
        assertThat(KafkaCmdClient.listTopicsUsingPodCli(namespace, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName)), hasItem(topicName));
        assertThat(kafkaTopic.getMetadata().getName(), is(topicName));
        assertThat(kafkaTopic.getSpec().getPartitions(), is(topicPartitions));
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        LOGGER.info("Deploying shared Kafka across all test cases in {} namespace", namespace);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(TOPIC_CLUSTER_NAME, 3, 1)
            .editMetadata()
                .withNamespace(namespace)
            .endMetadata()
            .build(),
            ScraperTemplates.scraperPod(namespace, SCRAPER_NAME).build()
        );

        scraperPodName = kubeClient().listPodsByPrefixInName(namespace, SCRAPER_NAME).get(0).getMetadata().getName();
    }
}
