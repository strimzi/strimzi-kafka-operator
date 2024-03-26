/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators.topic;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicStatus;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.annotations.UTONotSupported;
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.enums.ConditionStatus;
import io.strimzi.systemtest.enums.CustomResourceStatus;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.admin.AdminClient;
import io.strimzi.systemtest.metrics.MetricsCollector;
import io.strimzi.systemtest.resources.ComponentType;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.AdminClientTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.specific.ScraperUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.List;

import static io.strimzi.systemtest.TestConstants.ARM64_UNSUPPORTED;
import static io.strimzi.systemtest.TestConstants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.systemtest.enums.ConditionStatus.False;
import static io.strimzi.systemtest.enums.ConditionStatus.True;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils.hasTopicInCRK8s;
import static io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils.hasTopicInKafka;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricResourceNotNull;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricResourcesHigherThanOrEqualTo;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricValueHigherThan;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
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
public class TopicST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(TopicST.class);
    private TestStorage sharedTestStorage;
    private String scraperPodName;
    private static int topicOperatorReconciliationInterval;

    @ParallelTest
    void testMoreReplicasThanAvailableBrokers() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        int topicReplicationFactor = 5;
        int topicPartitions = 5;

        KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(sharedTestStorage.getClusterName(), testStorage.getTopicName(), topicPartitions, topicReplicationFactor, 1, Environment.TEST_SUITE_NAMESPACE).build();
        resourceManager.createResourceWithoutWait(kafkaTopic);

        assertThat("Topic exists in Kafka CR (Kubernetes)", hasTopicInCRK8s(kafkaTopic, testStorage.getTopicName()));
        assertThat("Topic doesn't exists in Kafka itself", !hasTopicInKafka(testStorage.getTopicName(), sharedTestStorage.getClusterName(), scraperPodName));

        String errorMessage = Environment.isKRaftModeEnabled() ?
            "org.apache.kafka.common.errors.InvalidReplicationFactorException: Unable to replicate the partition 5 time(s): The target replication factor of 5 cannot be reached because only 3 broker(s) are registered." :
            "org.apache.kafka.common.errors.InvalidReplicationFactorException: Replication factor: 5 larger than available brokers: 3";

        KafkaTopicUtils.waitForKafkaTopicNotReady(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());
        KafkaTopicStatus kafkaTopicStatus = KafkaTopicResource.kafkaTopicClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(testStorage.getTopicName()).get().getStatus();

        assertThat(kafkaTopicStatus.getConditions().get(0).getMessage(), containsString(errorMessage));
        assertThat(kafkaTopicStatus.getConditions().get(0).getReason(), containsString("KafkaError"));

        LOGGER.info("Delete Topic: {}", testStorage.getTopicName());
        cmdKubeClient(Environment.TEST_SUITE_NAMESPACE).deleteByName("kafkatopic", testStorage.getTopicName());
        KafkaTopicUtils.waitForKafkaTopicDeletion(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());

        topicReplicationFactor = 3;
        final String newTopicName = "topic-example-new";

        kafkaTopic = KafkaTopicTemplates.topic(sharedTestStorage.getClusterName(), newTopicName, topicPartitions, topicReplicationFactor, Environment.TEST_SUITE_NAMESPACE).build();
        resourceManager.createResourceWithWait(kafkaTopic);

        assertThat("Topic exists in Kafka itself", hasTopicInKafka(newTopicName, sharedTestStorage.getClusterName(), scraperPodName));
        assertThat("Topic exists in Kafka CR (Kubernetes)", hasTopicInCRK8s(kafkaTopic, newTopicName));
    }

    @ParallelTest
    @UTONotSupported
    void testCreateTopicViaKafka() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        int topicPartitions = 3;

        LOGGER.debug("Creating Topic: {} with {} replicas and {} partitions", testStorage.getTopicName(), 3, topicPartitions);
        KafkaCmdClient.createTopicUsingPodCli(Environment.TEST_SUITE_NAMESPACE, scraperPodName, KafkaResources.plainBootstrapAddress(sharedTestStorage.getClusterName()), testStorage.getTopicName(), 3, topicPartitions);

        KafkaTopic kafkaTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(testStorage.getTopicName()).get();

        verifyTopicViaKafkaTopicCRK8s(kafkaTopic, testStorage.getTopicName(), topicPartitions, sharedTestStorage.getClusterName());

        topicPartitions = 5;
        LOGGER.info("Editing Topic via Kafka, settings to partitions {}", topicPartitions);

        KafkaCmdClient.updateTopicPartitionsCountUsingPodCli(Environment.TEST_SUITE_NAMESPACE, scraperPodName, KafkaResources.plainBootstrapAddress(sharedTestStorage.getClusterName()), testStorage.getTopicName(), topicPartitions);
        LOGGER.debug("Topic: {} updated from {} to {} partitions", testStorage.getTopicName(), 3, topicPartitions);

        KafkaTopicUtils.waitForKafkaTopicPartitionChange(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), topicPartitions);
        verifyTopicViaKafka(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), topicPartitions, sharedTestStorage.getClusterName());
    }

    @Tag(ARM64_UNSUPPORTED) // Due to https://github.com/strimzi/test-clients/issues/75
    @ParallelTest
    void testCreateDeleteCreate() throws InterruptedException {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            AdminClientTemplates.defaultAdminClient(testStorage.getNamespaceName(), testStorage.getAdminName(), KafkaResources.plainBootstrapAddress(sharedTestStorage.getClusterName())).build()
        );

        String adminClientPodName = kubeClient().listPods(testStorage.getNamespaceName(), TestConstants.ADMIN_CLIENT_LABEL_SELECTOR).get(0).getMetadata().getName();

        AdminClient adminClient = new AdminClient(testStorage.getNamespaceName(), adminClientPodName);
        adminClient.configureFromEnv();

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(sharedTestStorage.getClusterName(), testStorage.getTopicName(), Environment.TEST_SUITE_NAMESPACE)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .editSpec()
                .withReplicas(3)
            .endSpec()
            .build());

        assertThat(adminClient.listTopics(), containsString(testStorage.getTopicName()));

        for (int i = 0; i < 10; i++) {
            Thread.sleep(2_000);

            LOGGER.info("Iteration {}: Deleting {}", i, testStorage.getTopicName());
            cmdKubeClient(Environment.TEST_SUITE_NAMESPACE).deleteByName(KafkaTopic.RESOURCE_KIND, testStorage.getTopicName());
            KafkaTopicUtils.waitForKafkaTopicDeletion(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());

            assertThat(adminClient.listTopics(), not(containsString(testStorage.getTopicName())));

            Thread.sleep(2_000);
            long t0 = System.currentTimeMillis();

            LOGGER.info("Iteration {}: Recreating {}", i, testStorage.getTopicName());
            resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(sharedTestStorage.getClusterName(), testStorage.getTopicName(), Environment.TEST_SUITE_NAMESPACE)
                .editSpec()
                    .withReplicas(3)
                .endSpec()
                .build());

            assertThat(adminClient.listTopics(), containsString(testStorage.getTopicName()));
        }
    }

    @ParallelTest
    @Tag(INTERNAL_CLIENTS_USED)
    @UTONotSupported
    void testSendingMessagesToNonExistingTopic() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        LOGGER.info("Checking if {} is on Topic list", testStorage.getTopicName());
        assertFalse(hasTopicInKafka(testStorage.getTopicName(), sharedTestStorage.getClusterName(), scraperPodName));
        LOGGER.info("Topic with name {} is not created yet", testStorage.getTopicName());

        LOGGER.info("Trying to send messages to non-existing Topic: {}", testStorage.getTopicName());
        final KafkaClients clients = ClientUtils.getInstantPlainClients(testStorage, KafkaResources.plainBootstrapAddress(sharedTestStorage.getClusterName()));
        resourceManager.createResourceWithWait(clients.producerStrimzi(), clients.consumerStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);

        LOGGER.info("Checking if {} is on Topic list", testStorage.getTopicName());
        assertTrue(hasTopicInKafka(testStorage.getTopicName(), sharedTestStorage.getClusterName(), scraperPodName));

        KafkaTopicUtils.waitForKafkaTopicCreation(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());
        KafkaTopic kafkaTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(testStorage.getTopicName()).get();
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
    void testDeleteTopicEnableFalse() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3, 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
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

        final KafkaClients clients = ClientUtils.getInstantPlainClients(testStorage);
        resourceManager.createResourceWithWait(clients.producerStrimzi());
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        String topicUid = KafkaTopicUtils.topicSnapshot(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());
        LOGGER.info("Deleting KafkaTopic: {}/{}", Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());
        KafkaTopicResource.kafkaTopicClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(testStorage.getTopicName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        LOGGER.info("KafkaTopic: {}/{} deleted", Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());

        KafkaTopicUtils.waitTopicHasRolled(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), topicUid);

        LOGGER.info("Waiting for KafkaTopic: {}/{} recreation", Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());
        KafkaTopicUtils.waitForKafkaTopicCreation(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());
        LOGGER.info("KafkaTopic: {}/{} recreated", Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());

        resourceManager.createResourceWithWait(clients.consumerStrimzi());
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);
    }

    @ParallelTest
    void testCreateTopicAfterUnsupportedOperation() {
        String topicName = "topic-with-replication-to-change";
        String newTopicName = "another-topic";

        KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(sharedTestStorage.getClusterName(), topicName, Environment.TEST_SUITE_NAMESPACE)
            .editSpec()
                .withReplicas(3)
                .withPartitions(3)
            .endSpec()
            .build();

        resourceManager.createResourceWithWait(kafkaTopic);
        KafkaTopicUtils.waitForKafkaTopicReady(Environment.TEST_SUITE_NAMESPACE, topicName);

        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(topicName, t -> {
            t.getSpec().setReplicas(1);
            t.getSpec().setPartitions(1);
        }, Environment.TEST_SUITE_NAMESPACE);
        KafkaTopicUtils.waitForKafkaTopicNotReady(Environment.TEST_SUITE_NAMESPACE, topicName);

        String exceptedMessage = "Decreasing partitions not supported";
        assertThat(KafkaTopicResource.kafkaTopicClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(topicName).get().getStatus().getConditions().get(0).getMessage(), is(exceptedMessage));

        String topicCRDMessage = KafkaTopicResource.kafkaTopicClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(topicName).get().getStatus().getConditions().get(0).getMessage();

        assertThat(topicCRDMessage, containsString(exceptedMessage));

        KafkaTopic newKafkaTopic = KafkaTopicTemplates.topic(sharedTestStorage.getClusterName(), newTopicName, 1, 1, Environment.TEST_SUITE_NAMESPACE).build();

        resourceManager.createResourceWithWait(newKafkaTopic);

        assertThat("Topic exists in Kafka itself", hasTopicInKafka(topicName, sharedTestStorage.getClusterName(), scraperPodName));
        assertThat("Topic exists in Kafka CR (Kubernetes)", hasTopicInCRK8s(kafkaTopic, topicName));
        assertThat("Topic exists in Kafka itself", hasTopicInKafka(newTopicName, sharedTestStorage.getClusterName(), scraperPodName));
        assertThat("Topic exists in Kafka CR (Kubernetes)", hasTopicInCRK8s(newKafkaTopic, newTopicName));

        cmdKubeClient(Environment.TEST_SUITE_NAMESPACE).deleteByName(KafkaTopic.RESOURCE_SINGULAR, topicName);
        KafkaTopicUtils.waitForKafkaTopicDeletion(Environment.TEST_SUITE_NAMESPACE, topicName);
        cmdKubeClient(Environment.TEST_SUITE_NAMESPACE).deleteByName(KafkaTopic.RESOURCE_SINGULAR, newTopicName);
        KafkaTopicUtils.waitForKafkaTopicDeletion(Environment.TEST_SUITE_NAMESPACE, newTopicName);
    }

    /**
     * @description This test case checks Unidirectional Topic Operator metrics regarding different states of KafkaTopic.
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
    void testKafkaTopicDifferentStatesInUTOMode() throws InterruptedException {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        int initialReplicas = 1;
        int initialPartitions = 5;
        int decreasePartitions = 1;
        int expectedNumOfTopics = 1;
        int expectedObservedGeneration = 1;

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(sharedTestStorage.getClusterName(), testStorage.getTopicName(), initialPartitions, initialReplicas, Environment.TEST_SUITE_NAMESPACE).build());
        KafkaTopicUtils.waitForKafkaTopicReady(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());

        LOGGER.info("Found the following Topics:");
        cmdKubeClient().list(KafkaTopic.RESOURCE_KIND).forEach(item -> {
            LOGGER.info("{}: {}", KafkaTopic.RESOURCE_KIND, item);
        });

        MetricsCollector toMetricsCollector = new MetricsCollector.Builder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withScraperPodName(scraperPodName)
            .withComponentName(sharedTestStorage.getClusterName())
            .withComponentType(ComponentType.TopicOperator)
            .build();

        assertMetricResourceNotNull(toMetricsCollector, "strimzi_reconciliations_successful_total", KafkaTopic.RESOURCE_KIND);
        assertMetricResourceNotNull(toMetricsCollector, "strimzi_reconciliations_duration_seconds_count", KafkaTopic.RESOURCE_KIND);
        assertMetricResourceNotNull(toMetricsCollector, "strimzi_reconciliations_duration_seconds_sum", KafkaTopic.RESOURCE_KIND);
        assertMetricResourceNotNull(toMetricsCollector, "strimzi_reconciliations_duration_seconds_max", KafkaTopic.RESOURCE_KIND);
        assertMetricResourceNotNull(toMetricsCollector, "strimzi_reconciliations_total", KafkaTopic.RESOURCE_KIND);
        assertMetricResourcesHigherThanOrEqualTo(toMetricsCollector, KafkaTopic.RESOURCE_KIND, expectedNumOfTopics);

        LOGGER.info("Checking if resource state metric reason message is \"none\" and KafkaTopic is ready");
        assertKafkaTopicStatus(testStorage.getTopicName(), Environment.TEST_SUITE_NAMESPACE, Ready, True, expectedObservedGeneration);

        LOGGER.info("Changing Topic name in spec.topicName");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(testStorage.getTopicName(), kafkaTopic -> kafkaTopic.getSpec().setTopicName("some-other-name"), Environment.TEST_SUITE_NAMESPACE);
        KafkaTopicUtils.waitForKafkaTopicNotReady(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());

        // reason and message in UTO mode
        String reason = "NotSupported";
        String reasonMessage = "Changing spec.topicName is not supported";

        assertKafkaTopicStatus(testStorage.getTopicName(), Environment.TEST_SUITE_NAMESPACE, Ready, False, reason, reasonMessage, ++expectedObservedGeneration);

        LOGGER.info("Changing back to it's original name and scaling replicas to be higher number");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(testStorage.getTopicName(), kafkaTopic -> {
            kafkaTopic.getSpec().setTopicName(testStorage.getTopicName());
            kafkaTopic.getSpec().setReplicas(12);
        }, Environment.TEST_SUITE_NAMESPACE);

        KafkaTopicUtils.waitForKafkaTopicReplicasChange(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), 12);
        KafkaTopicUtils.waitForKafkaTopicNotReady(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());

        // message in UTO mode
        reasonMessage = "Replication factor change not supported";
        KafkaTopicUtils.waitForTopicStatusMessage(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), reasonMessage);
        assertKafkaTopicStatus(testStorage.getTopicName(), Environment.TEST_SUITE_NAMESPACE, Ready, False, reason, reasonMessage, ++expectedObservedGeneration);

        LOGGER.info("Changing KafkaTopic's spec to correct state");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(testStorage.getTopicName(), kafkaTopic -> kafkaTopic.getSpec().setReplicas(initialReplicas), Environment.TEST_SUITE_NAMESPACE);
        KafkaTopicUtils.waitForKafkaTopicReady(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());
        assertKafkaTopicStatus(testStorage.getTopicName(), Environment.TEST_SUITE_NAMESPACE, Ready, True, ++expectedObservedGeneration);

        LOGGER.info("Decreasing number of partitions to {}", decreasePartitions);
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(testStorage.getTopicName(), kafkaTopic -> kafkaTopic.getSpec().setPartitions(decreasePartitions), Environment.TEST_SUITE_NAMESPACE);
        KafkaTopicUtils.waitForKafkaTopicPartitionChange(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), decreasePartitions);
        KafkaTopicUtils.waitForKafkaTopicNotReady(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());

        // message in UTO mode
        reasonMessage = "Decreasing partitions not supported";
        assertKafkaTopicStatus(testStorage.getTopicName(), Environment.TEST_SUITE_NAMESPACE, Ready, False, reason, reasonMessage, ++expectedObservedGeneration);

        // Wait some time to check if error is still present in KafkaTopic status
        LOGGER.info("Waiting {} ms for next reconciliation", topicOperatorReconciliationInterval);
        Thread.sleep(topicOperatorReconciliationInterval);
        assertKafkaTopicStatus(testStorage.getTopicName(), Environment.TEST_SUITE_NAMESPACE, Ready, False, reason, reasonMessage, expectedObservedGeneration);

        LOGGER.info("Changing KafkaTopic's spec to correct state");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(testStorage.getTopicName(), kafkaTopic -> {
            kafkaTopic.getSpec().setReplicas(initialReplicas);
            kafkaTopic.getSpec().setPartitions(initialPartitions);
        }, Environment.TEST_SUITE_NAMESPACE);
        KafkaTopicUtils.waitForKafkaTopicReady(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());

        assertKafkaTopicStatus(testStorage.getTopicName(), Environment.TEST_SUITE_NAMESPACE, Ready, True, ++expectedObservedGeneration);
        assertMetricValueHigherThan(toMetricsCollector, "strimzi_reconciliations_failed_total\\{kind=\"" + KafkaTopic.RESOURCE_KIND + "\",.*}", 3);
    }

    @ParallelTest
    void testKafkaTopicChangingMinInSyncReplicas() throws InterruptedException {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(sharedTestStorage.getClusterName(), testStorage.getTopicName(), 5, Environment.TEST_SUITE_NAMESPACE).build());
        KafkaTopicUtils.waitForKafkaTopicReady(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());
        String invalidValue = "x";
        String reason = "KafkaError";

        CustomResourceStatus resourceStatus = Ready;
        ConditionStatus conditionStatus = False;

        String reasonMessage = String.format("Invalid value %s for configuration min.insync.replicas", invalidValue);

        LOGGER.info("Changing min.insync.replicas to random char");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(testStorage.getTopicName(),
            kafkaTopic -> kafkaTopic.getSpec().getConfig().put("min.insync.replicas", invalidValue),
            Environment.TEST_SUITE_NAMESPACE);
        KafkaTopicUtils.waitForKafkaTopicNotReady(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());
        assertKafkaTopicStatus(testStorage.getTopicName(), Environment.TEST_SUITE_NAMESPACE, resourceStatus, conditionStatus, reason, reasonMessage, 2);

        // Wait some time to check if error is still present in KafkaTopic status
        LOGGER.info("Waiting {} ms for next reconciliation", topicOperatorReconciliationInterval);
        Thread.sleep(topicOperatorReconciliationInterval);
        assertKafkaTopicStatus(testStorage.getTopicName(), Environment.TEST_SUITE_NAMESPACE, resourceStatus, conditionStatus, reason, reasonMessage, 2);
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
    void testTopicWithoutLabels() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final int topicOperatorReconciliationSeconds = 10;

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        // Negative scenario: creating topic without any labels and make sure that TO can't handle this topic
        resourceManager.createResourceWithWait(
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build(),
            KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3)
                .editSpec()
                    .editEntityOperator()
                        .editTopicOperator()
                            .withReconciliationIntervalSeconds(topicOperatorReconciliationSeconds)
                        .endTopicOperator()
                    .endEntityOperator()
                .endSpec().build()
        );

        final String scraperPodName = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();

        LOGGER.info("Creating KafkaTopic: {}/{} in without any label", testStorage.getNamespaceName(), testStorage.getTargetTopicName());
        resourceManager.createResourceWithoutWait(KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTargetTopicName(), 1, 1, 1, testStorage.getNamespaceName())
            .editMetadata()
                .withLabels(null)
            .endMetadata().build()
        );

        // Checking that resource was created
        LOGGER.info("Verifying presence of KafkaTopic: {}/{}", testStorage.getNamespaceName(), testStorage.getTargetTopicName());
        assertThat(cmdKubeClient(testStorage.getNamespaceName()).list("kafkatopic"), hasItems(testStorage.getTargetTopicName()));

        // Checking that TO didn't handle new topic and zk pods don't contain new topic
        KafkaTopicUtils.verifyUnchangedTopicAbsence(testStorage.getNamespaceName(), scraperPodName, testStorage.getClusterName(), testStorage.getTargetTopicName(), topicOperatorReconciliationSeconds);

        // Checking TO logs
        String tOPodName = cmdKubeClient(testStorage.getNamespaceName()).listResourcesByLabel("pod", Labels.STRIMZI_NAME_LABEL + "=" + testStorage.getClusterName() + "-entity-operator").get(0);
        String tOlogs = kubeClient(testStorage.getNamespaceName()).logsInSpecificNamespace(testStorage.getNamespaceName(), tOPodName, "topic-operator");
        assertThat(tOlogs, not(containsString(String.format("Created topic '%s'", testStorage.getTargetTopicName()))));

        //Deleting topic
        cmdKubeClient(testStorage.getNamespaceName()).deleteByName("kafkatopic", testStorage.getTargetTopicName());
        KafkaTopicUtils.waitForKafkaTopicDeletion(testStorage.getNamespaceName(),  testStorage.getTargetTopicName());

        //Checking KafkaTopic is not present inside Kafka cluster
        List<String> topics = KafkaCmdClient.listTopicsUsingPodCli(testStorage.getNamespaceName(), scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()));
        assertThat(topics, not(hasItems(testStorage.getTargetTopicName())));
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

    void verifyTopicViaKafka(final String namespaceName, String topicName, int topicPartitions, String clusterName) {
        TestUtils.waitFor("Describing Topic: " + topicName + " using pod CLI", TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, TestConstants.GLOBAL_TIMEOUT,
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
        assertThat(KafkaCmdClient.listTopicsUsingPodCli(Environment.TEST_SUITE_NAMESPACE, scraperPodName, KafkaResources.plainBootstrapAddress(clusterName)), hasItem(topicName));
        assertThat(kafkaTopic.getMetadata().getName(), is(topicName));
        assertThat(kafkaTopic.getSpec().getPartitions(), is(topicPartitions));
    }

    @BeforeAll
    void setup() {
        sharedTestStorage = new TestStorage(ResourceManager.getTestContext(), Environment.TEST_SUITE_NAMESPACE);
        
        this.clusterOperator = this.clusterOperator
            .defaultInstallation()
            .createInstallation()
            .runInstallation();

        LOGGER.info("Deploying shared Kafka: {}/{} across all test cases", Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getClusterName());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getBrokerPoolName(), sharedTestStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getControllerPoolName(), sharedTestStorage.getClusterName(), 1).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(sharedTestStorage.getClusterName(), 3, 1)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .editSpec()
                .editEntityOperator()
                    .editOrNewTopicOperator()
                        .withReconciliationIntervalSeconds((int) TestConstants.RECONCILIATION_INTERVAL / 1000)
                    .endTopicOperator()
                .endEntityOperator()
            .endSpec()
            .build(),
            ScraperTemplates.scraperPod(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getScraperName()).build()
        );

        scraperPodName = ScraperUtils.getScraperPod(Environment.TEST_SUITE_NAMESPACE).getMetadata().getName();
        topicOperatorReconciliationInterval = KafkaResource.kafkaClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(sharedTestStorage.getClusterName()).get()
                .getSpec().getEntityOperator().getTopicOperator().getReconciliationIntervalSeconds() * 1000 + 5_000;
    }
}
