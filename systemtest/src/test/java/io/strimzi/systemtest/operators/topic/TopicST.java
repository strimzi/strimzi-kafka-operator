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
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.KRaftWithoutUTONotSupported;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.annotations.UTONotSupported;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.enums.ConditionStatus;
import io.strimzi.systemtest.enums.CustomResourceStatus;
import io.strimzi.systemtest.kafkaclients.internalClients.AdminClientOperation;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaAdminClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaAdminClientsBuilder;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.metrics.MetricsCollector;
import io.strimzi.systemtest.resources.ComponentType;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.specific.ScraperUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.enums.ConditionStatus.True;
import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricResourceNotNull;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricResourceState;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricResourcesHigherThanOrEqualTo;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(REGRESSION)
@KRaftWithoutUTONotSupported
public class TopicST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(TopicST.class);
    private static final String KAFKA_CLUSTER_NAME = "topic-cluster-name";
    private static final String SCRAPER_NAME = KAFKA_CLUSTER_NAME + "-" + Constants.SCRAPER_NAME;

    private String scraperPodName;

    private static int topicOperatorReconciliationInterval;

    @ParallelTest
    void testMoreReplicasThanAvailableBrokers(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);
        final String topicName = testStorage.getTopicName();
        int topicReplicationFactor = 5;
        int topicPartitions = 5;

        KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(KAFKA_CLUSTER_NAME, topicName, topicPartitions, topicReplicationFactor, 1, Constants.TEST_SUITE_NAMESPACE).build();
        resourceManager.createResourceWithoutWait(extensionContext, kafkaTopic);

        assertThat("Topic exists in Kafka CR (Kubernetes)", hasTopicInCRK8s(kafkaTopic, topicName));
        assertThat("Topic doesn't exists in Kafka itself", !hasTopicInKafka(topicName, KAFKA_CLUSTER_NAME));

        String errorMessage = Environment.isUnidirectionalTopicOperatorEnabled() ?
            "org.apache.kafka.common.errors.InvalidReplicationFactorException: Unable to replicate the partition 5 time(s): The target replication factor of 5 cannot be reached because only 3 broker(s) are registered." :
            "org.apache.kafka.common.errors.InvalidReplicationFactorException: Replication factor: 5 larger than available brokers: 3";

        KafkaTopicUtils.waitForKafkaTopicNotReady(Constants.TEST_SUITE_NAMESPACE, topicName);
        KafkaTopicStatus kafkaTopicStatus = KafkaTopicResource.kafkaTopicClient().inNamespace(Constants.TEST_SUITE_NAMESPACE).withName(topicName).get().getStatus();

        assertThat(kafkaTopicStatus.getConditions().get(0).getMessage(), containsString(errorMessage));
        assertThat(kafkaTopicStatus.getConditions().get(0).getReason(), containsString(Environment.isUnidirectionalTopicOperatorEnabled() ? "KafkaError" : "CompletionException"));

        LOGGER.info("Delete Topic: {}", topicName);
        cmdKubeClient(Constants.TEST_SUITE_NAMESPACE).deleteByName("kafkatopic", topicName);
        KafkaTopicUtils.waitForKafkaTopicDeletion(Constants.TEST_SUITE_NAMESPACE, topicName);

        topicReplicationFactor = 3;
        final String newTopicName = "topic-example-new";

        kafkaTopic = KafkaTopicTemplates.topic(KAFKA_CLUSTER_NAME, newTopicName, topicPartitions, topicReplicationFactor, Constants.TEST_SUITE_NAMESPACE).build();
        resourceManager.createResourceWithWait(extensionContext, kafkaTopic);

        assertThat("Topic exists in Kafka itself", hasTopicInKafka(newTopicName, KAFKA_CLUSTER_NAME));
        assertThat("Topic exists in Kafka CR (Kubernetes)", hasTopicInCRK8s(kafkaTopic, newTopicName));
    }

    @ParallelTest
    @UTONotSupported
    void testCreateTopicViaKafka(ExtensionContext extensionContext) {
        final TestStorage testStorage = storageMap.get(extensionContext);
        String topicName = testStorage.getTopicName();
        int topicPartitions = 3;

        LOGGER.debug("Creating Topic: {} with {} replicas and {} partitions", topicName, 3, topicPartitions);
        KafkaCmdClient.createTopicUsingPodCli(Constants.TEST_SUITE_NAMESPACE, scraperPodName, KafkaResources.plainBootstrapAddress(KAFKA_CLUSTER_NAME), topicName, 3, topicPartitions);

        KafkaTopic kafkaTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(Constants.TEST_SUITE_NAMESPACE).withName(topicName).get();

        verifyTopicViaKafkaTopicCRK8s(kafkaTopic, topicName, topicPartitions, KAFKA_CLUSTER_NAME);

        topicPartitions = 5;
        LOGGER.info("Editing Topic via Kafka, settings to partitions {}", topicPartitions);

        KafkaCmdClient.updateTopicPartitionsCountUsingPodCli(Constants.TEST_SUITE_NAMESPACE, scraperPodName, KafkaResources.plainBootstrapAddress(KAFKA_CLUSTER_NAME), topicName, topicPartitions);
        LOGGER.debug("Topic: {} updated from {} to {} partitions", topicName, 3, topicPartitions);

        KafkaTopicUtils.waitForKafkaTopicPartitionChange(Constants.TEST_SUITE_NAMESPACE, topicName, topicPartitions);
        verifyTopicViaKafka(Constants.TEST_SUITE_NAMESPACE, topicName, topicPartitions, KAFKA_CLUSTER_NAME);
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(NODEPORT_SUPPORTED)
    @UTONotSupported
    void testCreateTopicViaAdminClient(ExtensionContext extensionContext) throws ExecutionException, InterruptedException, TimeoutException {
        final TestStorage testStorage = storageMap.get(extensionContext);
        String clusterName = testStorage.getClusterName();
        String topicName = testStorage.getTopicName();

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3, 3)
            .editMetadata()
                .withNamespace(Constants.TEST_SUITE_NAMESPACE)
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

        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaResource.kafkaClient().inNamespace(Constants.TEST_SUITE_NAMESPACE)
            .withName(clusterName).get().getStatus().getListeners().stream()
            .filter(listener -> listener.getName().equals(Constants.EXTERNAL_LISTENER_DEFAULT_NAME))
            .findFirst()
            .orElseThrow(RuntimeException::new)
            .getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(properties)) {

            Set<String> topics = adminClient.listTopics().names().get(Constants.GLOBAL_CLIENTS_TIMEOUT, TimeUnit.MILLISECONDS);
            int topicsSize = topics.size(); // new KafkaStreamsTopicStore has topology input topics

            LOGGER.info("Creating async Topic: {} via Admin client", topicName);
            CreateTopicsResult crt = adminClient.createTopics(singletonList(new NewTopic(topicName, 1, (short) 1)));
            crt.all().get();

            TestUtils.waitFor("Kafka cluster has " + (topicsSize + 1) + " KafkaTopic", Constants.GLOBAL_POLL_INTERVAL,
                Constants.GLOBAL_TIMEOUT, () -> {
                    Set<String> updatedKafkaTopics = new HashSet<>();
                    try {
                        updatedKafkaTopics = adminClient.listTopics().names().get(Constants.GLOBAL_CLIENTS_TIMEOUT, TimeUnit.MILLISECONDS);
                        LOGGER.info("Verifying that in Kafka cluster contains {} Topics", topicsSize + 1);

                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                        e.printStackTrace();
                    }
                    return updatedKafkaTopics.size() == topicsSize + 1 && updatedKafkaTopics.contains(topicName);

                });

            KafkaTopicUtils.waitForKafkaTopicCreation(Constants.TEST_SUITE_NAMESPACE, topicName);
            KafkaTopicUtils.waitForKafkaTopicReady(Constants.TEST_SUITE_NAMESPACE, topicName);
        }

        KafkaTopic kafkaTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(Constants.TEST_SUITE_NAMESPACE).withName(topicName).get();

        LOGGER.info("Verifying that corresponding {} KafkaTopic custom resources were created and Topic is in Ready state", 1);
        assertThat(kafkaTopic.getStatus().getConditions().get(0).getType(), is(Ready.toString()));
        assertThat(kafkaTopic.getSpec().getPartitions(), is(1));
        assertThat(kafkaTopic.getSpec().getReplicas(), is(1));
    }

    @ParallelTest
    void testCreateDeleteCreate(ExtensionContext extensionContext) throws InterruptedException {
        final TestStorage testStorage = new TestStorage(extensionContext, Constants.TEST_SUITE_NAMESPACE);

        KafkaAdminClients listTopicJob = new KafkaAdminClientsBuilder()
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(KAFKA_CLUSTER_NAME))
            .withNamespaceName(testStorage.getNamespaceName())
            .withAdminName(testStorage.getAdminName())
            .withTopicName("")
            .withAdminOperation(AdminClientOperation.LIST_TOPICS)
            .build();

        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(KAFKA_CLUSTER_NAME, testStorage.getTopicName(), Constants.TEST_SUITE_NAMESPACE)
            .editMetadata()
                .withNamespace(Constants.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .editSpec()
                .withReplicas(3)
            .endSpec()
            .build());

        resourceManager.createResourceWithWait(extensionContext, listTopicJob.defaultAdmin());
        ClientUtils.waitForClientContainsMessage(testStorage.getAdminName(), testStorage.getNamespaceName(), testStorage.getTopicName());

        for (int i = 0; i < 10; i++) {
            Thread.sleep(2_000);

            LOGGER.info("Iteration {}: Deleting {}", i, testStorage.getTopicName());
            cmdKubeClient(Constants.TEST_SUITE_NAMESPACE).deleteByName(KafkaTopic.RESOURCE_KIND, testStorage.getTopicName());
            KafkaTopicUtils.waitForKafkaTopicDeletion(Constants.TEST_SUITE_NAMESPACE, testStorage.getTopicName());

            resourceManager.createResourceWithWait(extensionContext, listTopicJob.defaultAdmin());
            ClientUtils.waitForClientNotContainsMessage(testStorage.getAdminName(), testStorage.getNamespaceName(), testStorage.getTopicName());

            Thread.sleep(2_000);
            long t0 = System.currentTimeMillis();

            LOGGER.info("Iteration {}: Recreating {}", i, testStorage.getTopicName());
            resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(KAFKA_CLUSTER_NAME, testStorage.getTopicName(), Constants.TEST_SUITE_NAMESPACE)
                .editSpec()
                    .withReplicas(3)
                .endSpec()
                .build());

            resourceManager.createResourceWithWait(extensionContext, listTopicJob.defaultAdmin());
            ClientUtils.waitForClientContainsMessage(testStorage.getAdminName(), testStorage.getNamespaceName(), testStorage.getTopicName());
        }
    }

    @ParallelTest
    @Tag(INTERNAL_CLIENTS_USED)
    @UTONotSupported
    void testSendingMessagesToNonExistingTopic(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, Constants.TEST_SUITE_NAMESPACE);

        KafkaClients clients = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(KAFKA_CLUSTER_NAME))
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withMessageCount(testStorage.getMessageCount())
            .build();

        LOGGER.info("Checking if {} is on Topic list", testStorage.getTopicName());
        assertFalse(hasTopicInKafka(testStorage.getTopicName(), KAFKA_CLUSTER_NAME));
        LOGGER.info("Topic with name {} is not created yet", testStorage.getTopicName());

        LOGGER.info("Trying to send messages to non-existing Topic: {}", testStorage.getTopicName());

        resourceManager.createResourceWithWait(extensionContext, clients.producerStrimzi(), clients.consumerStrimzi());
        ClientUtils.waitForClientsSuccess(testStorage);

        LOGGER.info("Checking if {} is on Topic list", testStorage.getTopicName());
        assertTrue(hasTopicInKafka(testStorage.getTopicName(), KAFKA_CLUSTER_NAME));

        KafkaTopicUtils.waitForKafkaTopicCreation(Constants.TEST_SUITE_NAMESPACE, testStorage.getTopicName());
        KafkaTopic kafkaTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(Constants.TEST_SUITE_NAMESPACE).withName(testStorage.getTopicName()).get();
        assertThat(kafkaTopic, notNullValue());

        assertThat(kafkaTopic.getStatus(), notNullValue());
        assertThat(kafkaTopic.getStatus().getConditions(), notNullValue());
        assertThat(kafkaTopic.getStatus().getConditions().isEmpty(), is(false));
        assertThat(kafkaTopic.getStatus().getConditions().get(0).getType(), is(Ready.toString()));
        LOGGER.info("Topic successfully created");
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(INTERNAL_CLIENTS_USED)
    @UTONotSupported
    void testDeleteTopicEnableFalse(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext, Constants.TEST_SUITE_NAMESPACE);

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3, 1)
            .editMetadata()
                .withNamespace(Constants.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .editSpec()
                .editKafka()
                    .addToConfig("delete.topic.enable", false)
                .endKafka()
                .editOrNewEntityOperator()
                    .withNewTopicOperator()
                    .endTopicOperator()
                .endEntityOperator()
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

        resourceManager.createResourceWithWait(extensionContext, clients.producerStrimzi());
        ClientUtils.waitForProducerClientSuccess(testStorage);

        String topicUid = KafkaTopicUtils.topicSnapshot(Constants.TEST_SUITE_NAMESPACE, testStorage.getTopicName());
        LOGGER.info("Deleting KafkaTopic: {}/{}", Constants.TEST_SUITE_NAMESPACE, testStorage.getTopicName());
        KafkaTopicResource.kafkaTopicClient().inNamespace(Constants.TEST_SUITE_NAMESPACE).withName(testStorage.getTopicName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        LOGGER.info("KafkaTopic: {}/{} deleted", Constants.TEST_SUITE_NAMESPACE, testStorage.getTopicName());

        KafkaTopicUtils.waitTopicHasRolled(Constants.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), topicUid);

        LOGGER.info("Waiting for KafkaTopic: {}/{} recreation", Constants.TEST_SUITE_NAMESPACE, testStorage.getTopicName());
        KafkaTopicUtils.waitForKafkaTopicCreation(Constants.TEST_SUITE_NAMESPACE, testStorage.getTopicName());
        LOGGER.info("KafkaTopic: {}/{} recreated", Constants.TEST_SUITE_NAMESPACE, testStorage.getTopicName());

        resourceManager.createResourceWithWait(extensionContext, clients.consumerStrimzi());
        ClientUtils.waitForConsumerClientSuccess(testStorage);
    }

    @ParallelTest
    void testCreateTopicAfterUnsupportedOperation(ExtensionContext extensionContext) {
        String topicName = "topic-with-replication-to-change";
        String newTopicName = "another-topic";

        KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(KAFKA_CLUSTER_NAME, topicName, Constants.TEST_SUITE_NAMESPACE)
            .editSpec()
                .withReplicas(3)
                .withPartitions(3)
            .endSpec()
            .build();

        resourceManager.createResourceWithWait(extensionContext, kafkaTopic);
        KafkaTopicUtils.waitForKafkaTopicReady(Constants.TEST_SUITE_NAMESPACE, topicName);

        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName, t -> {
            t.getSpec().setReplicas(1);
            t.getSpec().setPartitions(1);
        }, Constants.TEST_SUITE_NAMESPACE);
        KafkaTopicUtils.waitForKafkaTopicNotReady(Constants.TEST_SUITE_NAMESPACE, topicName);

        String exceptedMessage = Environment.isUnidirectionalTopicOperatorEnabled() ? "Decreasing partitions not supported" : "Number of partitions cannot be decreased";
        assertThat(KafkaTopicResource.kafkaTopicClient().inNamespace(Constants.TEST_SUITE_NAMESPACE).withName(topicName).get().getStatus().getConditions().get(0).getMessage(), is(exceptedMessage));

        String topicCRDMessage = KafkaTopicResource.kafkaTopicClient().inNamespace(Constants.TEST_SUITE_NAMESPACE).withName(topicName).get().getStatus().getConditions().get(0).getMessage();

        assertThat(topicCRDMessage, containsString(exceptedMessage));

        KafkaTopic newKafkaTopic = KafkaTopicTemplates.topic(KAFKA_CLUSTER_NAME, newTopicName, 1, 1, Constants.TEST_SUITE_NAMESPACE).build();

        resourceManager.createResourceWithWait(extensionContext, newKafkaTopic);

        assertThat("Topic exists in Kafka itself", hasTopicInKafka(topicName, KAFKA_CLUSTER_NAME));
        assertThat("Topic exists in Kafka CR (Kubernetes)", hasTopicInCRK8s(kafkaTopic, topicName));
        assertThat("Topic exists in Kafka itself", hasTopicInKafka(newTopicName, KAFKA_CLUSTER_NAME));
        assertThat("Topic exists in Kafka CR (Kubernetes)", hasTopicInCRK8s(newKafkaTopic, newTopicName));

        cmdKubeClient(Constants.TEST_SUITE_NAMESPACE).deleteByName(KafkaTopic.RESOURCE_SINGULAR, topicName);
        KafkaTopicUtils.waitForKafkaTopicDeletion(Constants.TEST_SUITE_NAMESPACE, topicName);
        cmdKubeClient(Constants.TEST_SUITE_NAMESPACE).deleteByName(KafkaTopic.RESOURCE_SINGULAR, newTopicName);
        KafkaTopicUtils.waitForKafkaTopicDeletion(Constants.TEST_SUITE_NAMESPACE, newTopicName);
    }

    /**
     * @description This test case checks Topic Operator metrics regarding different states of KafkaTopic.
     *
     * @steps
     *  1. - Create KafkaTopic
     *     - KafkaTopic is ready
     *  2. - Create metrics collector for Topic Operator and collect the metrics
     *     - Metrics collected
     *  3. - Check that TOpic Operator metrics contains data about reconciliations
     *     - Metrics contains proper data
     *  4. - Check that metrics contain info about KafkaTopic with name stored in 'topicName' is Ready
     *     - Metrics contains proper data
     *  5. - Change spec.topicName for topic 'topicName' and wait for NotReady status
     *     - KafkaTopic is in NotReady state
     *  6. - Check that metrics contain info about KafkaTopic 'topicName' cannot be renamed and that KT status has proper values
     *     - Metrics contains proper data and status contains proper values
     *  7. - Revert changes in KafkaTopic and change number of Replicas
     *     - KafkaTopic CR replica count is changed
     *  8. - Check that metrics contain info about KafkaTopic 'topicName' replicas count cannot be changed and KT status has proper values
     *     - Metrics contains proper data and KT status has proper values
     *  9. - Decrease KT number of partitions
     *     - Partitions count changed
     *  10. - Check that metrics contains info about KafkaTopic NotReady status and KT status has proper values (cannot change partition count)
     *      - Metrics contains proper data and KT status has proper values
     *  11. - Set KafkaTopic configuration to default one
     *      - KafkaTopic is in Ready state
     *  12. - Check that metrics contain info about KafkaTopic 'topicName' is Ready
     *      - Metrics contains proper data
     *
     * @testcase
     *  - topic-operator-metrics
     *  - kafkatopic-ready
     *  - kafkatopic-not-ready
     */
    @IsolatedTest
    @KRaftWithoutUTONotSupported
    @UTONotSupported("UTO has currently no metrics")
    void testKafkaTopicDifferentStates(ExtensionContext extensionContext) throws InterruptedException {
        final TestStorage testStorage = storageMap.get(extensionContext);
        String topicName = testStorage.getTopicName();
        int initialReplicas = 1;
        int initialPartitions = 5;
        int decreasePartitions = 1;

        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(KAFKA_CLUSTER_NAME, topicName, initialPartitions, initialReplicas, Constants.TEST_SUITE_NAMESPACE).build());
        KafkaTopicUtils.waitForKafkaTopicReady(Constants.TEST_SUITE_NAMESPACE, topicName);

        LOGGER.info("Found the following Topics:");
        cmdKubeClient().list(KafkaTopic.RESOURCE_KIND).forEach(item -> {
            LOGGER.info("{}: {}", KafkaTopic.RESOURCE_KIND, item);
        });

        MetricsCollector toMetricsCollector = new MetricsCollector.Builder()
            .withNamespaceName(Constants.TEST_SUITE_NAMESPACE)
            .withScraperPodName(scraperPodName)
            .withComponentName(KAFKA_CLUSTER_NAME)
            .withComponentType(ComponentType.TopicOperator)
            .build();

        assertMetricResourceNotNull(toMetricsCollector, "strimzi_reconciliations_successful_total", KafkaTopic.RESOURCE_KIND);
        assertMetricResourceNotNull(toMetricsCollector, "strimzi_reconciliations_duration_seconds_count", KafkaTopic.RESOURCE_KIND);
        assertMetricResourceNotNull(toMetricsCollector, "strimzi_reconciliations_duration_seconds_sum", KafkaTopic.RESOURCE_KIND);
        assertMetricResourceNotNull(toMetricsCollector, "strimzi_reconciliations_duration_seconds_max", KafkaTopic.RESOURCE_KIND);
        assertMetricResourceNotNull(toMetricsCollector, "strimzi_reconciliations_periodical_total", KafkaTopic.RESOURCE_KIND);
        assertMetricResourceNotNull(toMetricsCollector, "strimzi_reconciliations_total", KafkaTopic.RESOURCE_KIND);
        assertMetricResourcesHigherThanOrEqualTo(toMetricsCollector, KafkaTopic.RESOURCE_KIND, 3);

        String reasonMessage = "none";
        String reason = "";

        LOGGER.info("Checking if resource state metric reason message is \"none\" and KafkaTopic is ready");
        assertMetricResourceState(toMetricsCollector, KafkaTopic.RESOURCE_KIND, topicName, Constants.TEST_SUITE_NAMESPACE, 1, reasonMessage);
        assertKafkaTopicStatus(topicName, Constants.TEST_SUITE_NAMESPACE, Ready, True, 1);

        LOGGER.info("Changing Topic name in spec.topicName");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName, kafkaTopic -> kafkaTopic.getSpec().setTopicName("some-other-name"), Constants.TEST_SUITE_NAMESPACE);
        KafkaTopicUtils.waitForKafkaTopicNotReady(Constants.TEST_SUITE_NAMESPACE, topicName);

        reason = "IllegalArgumentException";
        reasonMessage = "Kafka topics cannot be renamed, but KafkaTopic's spec.topicName has changed.";
        assertMetricResourceState(toMetricsCollector, KafkaTopic.RESOURCE_KIND, topicName, Constants.TEST_SUITE_NAMESPACE, 0, reasonMessage);
        assertKafkaTopicStatus(topicName, Constants.TEST_SUITE_NAMESPACE, NotReady, True, reason, reasonMessage, 2);

        LOGGER.info("Changing back to it's original name and scaling replicas to be higher number");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName, kafkaTopic -> {
            kafkaTopic.getSpec().setTopicName(topicName);
            kafkaTopic.getSpec().setReplicas(12);
        }, Constants.TEST_SUITE_NAMESPACE);

        KafkaTopicUtils.waitForKafkaTopicReplicasChange(Constants.TEST_SUITE_NAMESPACE, topicName, 12);

        reason = "ReplicationFactorChangeException";
        reasonMessage = "Changing 'spec.replicas' is not supported.";
        KafkaTopicUtils.waitForKafkaTopicNotReady(Constants.TEST_SUITE_NAMESPACE, topicName);
        assertMetricResourceState(toMetricsCollector, KafkaTopic.RESOURCE_KIND, topicName, Constants.TEST_SUITE_NAMESPACE, 0, reasonMessage);
        assertKafkaTopicStatus(topicName, Constants.TEST_SUITE_NAMESPACE, NotReady, True, reason, reasonMessage, 3);

        LOGGER.info("Changing KafkaTopic's spec to correct state");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName, kafkaTopic -> kafkaTopic.getSpec().setReplicas(initialReplicas), Constants.TEST_SUITE_NAMESPACE);
        KafkaTopicUtils.waitForKafkaTopicReady(Constants.TEST_SUITE_NAMESPACE, topicName);
        assertKafkaTopicStatus(topicName, Constants.TEST_SUITE_NAMESPACE, Ready, True, 4);

        reasonMessage = "none";
        assertMetricResourceState(toMetricsCollector, KafkaTopic.RESOURCE_KIND, topicName, Constants.TEST_SUITE_NAMESPACE, 1, reasonMessage);
        KafkaTopicUtils.waitForKafkaTopicReady(Constants.TEST_SUITE_NAMESPACE, topicName);

        LOGGER.info("Decreasing number of partitions to {}", decreasePartitions);
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName, kafkaTopic -> kafkaTopic.getSpec().setPartitions(decreasePartitions), Constants.TEST_SUITE_NAMESPACE);
        KafkaTopicUtils.waitForKafkaTopicPartitionChange(Constants.TEST_SUITE_NAMESPACE, topicName, decreasePartitions);
        KafkaTopicUtils.waitForKafkaTopicNotReady(Constants.TEST_SUITE_NAMESPACE, topicName);

        reason = "PartitionDecreaseException";
        reasonMessage = "Number of partitions cannot be decreased";
        assertKafkaTopicStatus(topicName, Constants.TEST_SUITE_NAMESPACE, NotReady, True, reason, reasonMessage, 5);

        // Wait some time to check if error is still present in KafkaTopic status
        LOGGER.info("Waiting {} ms for next reconciliation", topicOperatorReconciliationInterval);
        Thread.sleep(topicOperatorReconciliationInterval);
        assertKafkaTopicStatus(topicName, Constants.TEST_SUITE_NAMESPACE, NotReady, True, reason, reasonMessage, 5);

        LOGGER.info("Changing KafkaTopic's spec to correct state");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName, kafkaTopic -> {
            kafkaTopic.getSpec().setReplicas(initialReplicas);
            kafkaTopic.getSpec().setPartitions(initialPartitions);
        }, Constants.TEST_SUITE_NAMESPACE);
        KafkaTopicUtils.waitForKafkaTopicReady(Constants.TEST_SUITE_NAMESPACE, topicName);
        assertKafkaTopicStatus(topicName, Constants.TEST_SUITE_NAMESPACE, Ready, True, 6);

        // Check failed reconciliations
        // This is currently doesn't work properly and will be changed in UTO implementation - https://github.com/strimzi/proposals/pull/76
        // assertMetricValueHigherThan(toMetricsCollector, "strimzi_reconciliations_failed_total{kind=\"" + KafkaTopic.RESOURCE_KIND + ",}", 3);
    }

    @ParallelTest
    void testKafkaTopicChangingMinInSyncReplicas(ExtensionContext extensionContext) throws InterruptedException {
        final TestStorage testStorage = storageMap.get(extensionContext);
        String topicName = testStorage.getTopicName();

        resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(KAFKA_CLUSTER_NAME, topicName, 5, Constants.TEST_SUITE_NAMESPACE).build());
        KafkaTopicUtils.waitForKafkaTopicReady(Constants.TEST_SUITE_NAMESPACE, topicName);
        String invalidValue = "x";
        String reason = Environment.isUnidirectionalTopicOperatorEnabled() ? "KafkaError" : "InvalidConfigurationException";

        // When using UTO, KafkaTopic instead of having condition type "NotReady" and condition status "True", has condition type "Ready" with condition status "False"
        CustomResourceStatus resourceStatus = Environment.isUnidirectionalTopicOperatorEnabled() ? Ready : NotReady;
        ConditionStatus conditionStatus = Environment.isUnidirectionalTopicOperatorEnabled() ? ConditionStatus.False : True;

        String reasonMessage = String.format("Invalid value %s for configuration min.insync.replicas", invalidValue);

        LOGGER.info("Changing min.insync.replicas to random char");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName,
            kafkaTopic -> kafkaTopic.getSpec().getConfig().put("min.insync.replicas", invalidValue),
            Constants.TEST_SUITE_NAMESPACE);
        KafkaTopicUtils.waitForKafkaTopicNotReady(Constants.TEST_SUITE_NAMESPACE, topicName);
        assertKafkaTopicStatus(topicName, Constants.TEST_SUITE_NAMESPACE, resourceStatus, conditionStatus, reason, reasonMessage, 2);

        // Wait some time to check if error is still present in KafkaTopic status
        LOGGER.info("Waiting {} ms for next reconciliation", topicOperatorReconciliationInterval);
        Thread.sleep(topicOperatorReconciliationInterval);
        assertKafkaTopicStatus(topicName, Constants.TEST_SUITE_NAMESPACE, resourceStatus, conditionStatus, reason, reasonMessage, 2);
    }

    /**
     * @description This test case checks that Kafka cluster will not act upon KafkaTopic Custom Resources
     * which are not of its concern, i.e., KafkaTopic Custom Resources are not labeled accordingly.
     *
     * @steps
     *  1. - Deploy Kafka with short reconciliation time configured on Topic Operator
     *     - Kafka is deployed
     *  2. - Create KafkaTopic Custom Resource without any labels provided
     *     - KafkaTopic Custom resource is created
     *  3. - Verify that KafkaTopic specified by created KafkaTopic is not created
     *     - Given KafkaTopic is not present inside Kafka cluster
     *  4. - Delete given KafkaTopic Custom Resource
     *     - KafkaTopic Custom Resource is deleted
     *
     * @testcase
     *  - topic-operator
     *  - kafka-topic
     *  - labels
     */
    @ParallelNamespaceTest
    void testTopicWithoutLabels(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        final String namespaceName = testStorage.getNamespaceName();
        final String clusterName = testStorage.getClusterName();
        final String scraperName = testStorage.getScraperName();
        final String kafkaTopicName = testStorage.getTargetTopicName();
        final int topicOperatorReconciliationSeconds = 10;

        // Negative scenario: creating topic without any labels and make sure that TO can't handle this topic
        resourceManager.createResourceWithWait(extensionContext,
            ScraperTemplates.scraperPod(namespaceName, scraperName).build(),
            KafkaTemplates.kafkaEphemeral(clusterName, 3)
                .editSpec()
                    .editEntityOperator()
                        .editTopicOperator()
                            .withReconciliationIntervalSeconds(topicOperatorReconciliationSeconds)
                        .endTopicOperator()
                    .endEntityOperator()
                .endSpec().build()
        );

        final String scraperPodName =  kubeClient().listPodsByPrefixInName(namespaceName, scraperName).get(0).getMetadata().getName();

        LOGGER.info("Creating KafkaTopic: {}/{} in without any label", namespaceName, kafkaTopicName);
        resourceManager.createResourceWithoutWait(extensionContext, KafkaTopicTemplates.topic(clusterName, kafkaTopicName, 1, 1, 1, namespaceName)
            .editMetadata()
                .withLabels(null)
            .endMetadata().build()
        );

        // Checking that resource was created
        LOGGER.info("Verifying presence of KafkaTopic: {}/{}", namespaceName, kafkaTopicName);
        assertThat(cmdKubeClient(namespaceName).list("kafkatopic"), hasItems(kafkaTopicName));

        // Checking that TO didn't handle new topic and zk pods don't contain new topic
        KafkaTopicUtils.verifyUnchangedTopicAbsence(namespaceName, scraperPodName, clusterName, kafkaTopicName, topicOperatorReconciliationSeconds);

        // Checking TO logs
        String tOPodName = cmdKubeClient(namespaceName).listResourcesByLabel("pod", Labels.STRIMZI_NAME_LABEL + "=" + clusterName + "-entity-operator").get(0);
        String tOlogs = kubeClient(namespaceName).logsInSpecificNamespace(namespaceName, tOPodName, "topic-operator");
        assertThat(tOlogs, not(containsString(String.format("Created topic '%s'", kafkaTopicName))));

        //Deleting topic
        cmdKubeClient(namespaceName).deleteByName("kafkatopic", kafkaTopicName);
        KafkaTopicUtils.waitForKafkaTopicDeletion(namespaceName,  kafkaTopicName);

        //Checking KafkaTopic is not present inside Kafka cluster
        List<String> topics = KafkaCmdClient.listTopicsUsingPodCli(namespaceName, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName));
        assertThat(topics, not(hasItems(kafkaTopicName)));
    }

    void assertKafkaTopicStatus(String topicName, String namespace, CustomResourceStatus status, ConditionStatus conditionStatus, int expectedObservedGeneration) {
        assertKafkaTopicStatus(topicName, namespace,  status, conditionStatus, null, null, expectedObservedGeneration);
    }

    void assertKafkaTopicStatus(String topicName, String namespace, CustomResourceStatus status, ConditionStatus conditionStatus, String reason, String message, int expectedObservedGeneration) {
        KafkaTopicStatus kafkaTopicStatus = KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).withName(topicName).get().getStatus();

        assertThat(kafkaTopicStatus.getConditions().stream()
                .anyMatch(condition -> condition.getType().equals(status.toString()) && condition.getStatus().equals(conditionStatus.toString())), CoreMatchers.is(true));
        assertThat("KafkaTopic status has incorrect Observed Generation", kafkaTopicStatus.getObservedGeneration(), CoreMatchers.is((long) expectedObservedGeneration));
        if (reason != null) {
            assertThat(kafkaTopicStatus.getConditions().stream()
                .anyMatch(condition -> condition.getReason().equals(reason)), CoreMatchers.is(true));
        }
        if (message != null) {
            assertThat(kafkaTopicStatus.getConditions().stream()
                    .anyMatch(condition -> condition.getMessage().contains(message)), CoreMatchers.is(true));
        }
    }

    boolean hasTopicInKafka(String topicName, String clusterName) {
        LOGGER.info("Checking Topic: {} in Kafka", topicName);
        return KafkaCmdClient.listTopicsUsingPodCli(Constants.TEST_SUITE_NAMESPACE, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName)).contains(topicName);
    }

    boolean hasTopicInCRK8s(KafkaTopic kafkaTopic, String topicName) {
        LOGGER.info("Checking in KafkaTopic CR that Topic: {} exists", topicName);
        return kafkaTopic.getMetadata().getName().equals(topicName);
    }

    void verifyTopicViaKafka(final String namespaceName, String topicName, int topicPartitions, String clusterName) {
        TestUtils.waitFor("Describing Topic: " + topicName + " using pod CLI", Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.GLOBAL_TIMEOUT,
            () -> {
                try {
                    String topicInfo =  KafkaCmdClient.describeTopicUsingPodCli(namespaceName, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName), topicName);
                    LOGGER.info("Checking Topic: {} in Kafka: {}", topicName, clusterName);
                    LOGGER.debug("Topic: {} info: {}", topicName, topicInfo);
                    assertThat(topicInfo, containsString("Topic: " + topicName));
                    assertThat(topicInfo, containsString("PartitionCount: " + topicPartitions));
                    return true;
                } catch (KubeClusterException e) {
                    LOGGER.info("Describing Topic using Pod cli occurred following error: {}", e.getMessage());
                    return false;
                }
            });
    }

    void verifyTopicViaKafkaTopicCRK8s(KafkaTopic kafkaTopic, String topicName, int topicPartitions, String clusterName) {
        LOGGER.info("Checking in KafkaTopic CR that Topic: {} was created with expected settings", topicName);
        assertThat(kafkaTopic, is(notNullValue()));
        assertThat(KafkaCmdClient.listTopicsUsingPodCli(Constants.TEST_SUITE_NAMESPACE, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName)), hasItem(topicName));
        assertThat(kafkaTopic.getMetadata().getName(), is(topicName));
        assertThat(kafkaTopic.getSpec().getPartitions(), is(topicPartitions));
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        this.clusterOperator = this.clusterOperator
            .defaultInstallation(extensionContext)
            .createInstallation()
            .runInstallation();

        LOGGER.info("Deploying shared Kafka: {}/{} across all test cases", Constants.TEST_SUITE_NAMESPACE, KAFKA_CLUSTER_NAME);

        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(KAFKA_CLUSTER_NAME, 3, 1)
            .editMetadata()
                .withNamespace(Constants.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .editSpec()
                .editEntityOperator()
                    .editOrNewTopicOperator()
                        .withReconciliationIntervalSeconds((int) Constants.RECONCILIATION_INTERVAL / 1000)
                    .endTopicOperator()
                .endEntityOperator()
            .endSpec()
            .build(),
            ScraperTemplates.scraperPod(Constants.TEST_SUITE_NAMESPACE, SCRAPER_NAME).build()
        );

        scraperPodName = ScraperUtils.getScraperPod(Constants.TEST_SUITE_NAMESPACE).getMetadata().getName();
        topicOperatorReconciliationInterval = KafkaResource.kafkaClient().inNamespace(Constants.TEST_SUITE_NAMESPACE).withName(KAFKA_CLUSTER_NAME).get()
                .getSpec().getEntityOperator().getTopicOperator().getReconciliationIntervalSeconds() * 1000 + 5_000;
    }
}
