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
import io.strimzi.systemtest.cli.KafkaCmdClient;
import io.strimzi.systemtest.enums.ConditionStatus;
import io.strimzi.systemtest.enums.CustomResourceStatus;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.admin.AdminClient;
import io.strimzi.systemtest.metrics.TopicOperatorMetricsComponent;
import io.strimzi.systemtest.performance.gather.collectors.BaseMetricsCollector;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.AdminClientTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.AdminClientUtils;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.specific.ScraperUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.enums.ConditionStatus.False;
import static io.strimzi.systemtest.enums.ConditionStatus.True;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils.hasTopicInCRK8s;
import static io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils.hasTopicInKafka;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricResourceNotNull;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricResourcesHigherThanOrEqualTo;
import static io.strimzi.systemtest.utils.specific.MetricsUtils.assertMetricValueHigherThanOrEqualTo;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(REGRESSION)
public class TopicST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(TopicST.class);
    private TestStorage sharedTestStorage;
    private AdminClient adminClient;
    private String scraperPodName;
    private static long topicOperatorReconciliationIntervalMs;

    @ParallelTest
    void testMoreReplicasThanAvailableBrokers() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        int topicReplicationFactor = 5;
        int topicPartitions = 5;

        KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), sharedTestStorage.getClusterName(), topicPartitions, topicReplicationFactor, 1).build();
        resourceManager.createResourceWithoutWait(kafkaTopic);

        assertThat("Topic exists in Kafka CR (Kubernetes)", hasTopicInCRK8s(kafkaTopic, testStorage.getTopicName()));
        assertThat("Topic doesn't exists in Kafka itself", !hasTopicInKafka(testStorage.getTopicName(), sharedTestStorage.getClusterName(), scraperPodName));

        String errorMessage = "org.apache.kafka.common.errors.InvalidReplicationFactorException: Unable to replicate the partition 5 time(s): The target replication factor of 5 cannot be reached because only 3 broker(s) are registered.";

        KafkaTopicUtils.waitForKafkaTopicNotReady(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());
        KafkaTopicStatus kafkaTopicStatus = KafkaTopicResource.kafkaTopicClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(testStorage.getTopicName()).get().getStatus();

        assertThat(kafkaTopicStatus.getConditions().get(0).getMessage(), containsString(errorMessage));
        assertThat(kafkaTopicStatus.getConditions().get(0).getReason(), containsString("KafkaError"));

        LOGGER.info("Delete Topic: {}", testStorage.getTopicName());
        cmdKubeClient(Environment.TEST_SUITE_NAMESPACE).deleteByName("kafkatopic", testStorage.getTopicName());
        KafkaTopicUtils.waitForKafkaTopicDeletion(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());

        topicReplicationFactor = 3;
        final String newTopicName = "topic-example-new";

        kafkaTopic = KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, newTopicName, sharedTestStorage.getClusterName(), topicPartitions, topicReplicationFactor).build();
        resourceManager.createResourceWithWait(kafkaTopic);

        assertThat("Topic exists in Kafka itself", hasTopicInKafka(newTopicName, sharedTestStorage.getClusterName(), scraperPodName));
        assertThat("Topic exists in Kafka CR (Kubernetes)", hasTopicInCRK8s(kafkaTopic, newTopicName));
    }

    @ParallelTest
    void testCreateDeleteCreate() throws InterruptedException {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), sharedTestStorage.getClusterName())
            .editSpec()
                .withReplicas(3)
            .endSpec()
            .build());

        assertThat(adminClient.listTopics(), containsString(testStorage.getTopicName()));

        for (int i = 0; i < 10; i++) {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2));

            LOGGER.info("Iteration {}: Deleting {}", i, testStorage.getTopicName());
            cmdKubeClient(Environment.TEST_SUITE_NAMESPACE).deleteByName(KafkaTopic.RESOURCE_KIND, testStorage.getTopicName());
            KafkaTopicUtils.waitForKafkaTopicDeletion(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());

            assertThat(adminClient.listTopics(), not(containsString(testStorage.getTopicName())));

            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2));

            LOGGER.info("Iteration {}: Recreating {}", i, testStorage.getTopicName());
            resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), sharedTestStorage.getClusterName())
                .editSpec()
                    .withReplicas(3)
                .endSpec()
                .build());

            assertThat(adminClient.listTopics(), containsString(testStorage.getTopicName()));
        }
    }

    @ParallelTest
    void testSendingMessagesToNonExistingTopic() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        LOGGER.info("Checking if topic {} is present in Kafka", testStorage.getTopicName());
        assertFalse(AdminClientUtils.isTopicPresent(adminClient, testStorage.getTopicName()));
        LOGGER.info("Topic with name {} is not created yet", testStorage.getTopicName());

        LOGGER.info("Sending messages to non-existing Topic: {}, with auto.topic.creation configuration enabled", testStorage.getTopicName());
        final KafkaClients clients = ClientUtils.getInstantPlainClients(testStorage, KafkaResources.plainBootstrapAddress(sharedTestStorage.getClusterName()));
        resourceManager.createResourceWithWait(clients.producerStrimzi(), clients.consumerStrimzi());
        ClientUtils.waitForInstantClientSuccess(testStorage);

        LOGGER.info("Checking if topic {} is present in Kafka", testStorage.getTopicName());
        assertTrue(AdminClientUtils.isTopicPresent(adminClient, testStorage.getTopicName()));
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testDeleteTopicEnableFalse() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
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

        // create Kafka Topic CR and wait for its presence in Kafka cluster.
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage).build());

        resourceManager.createResourceWithWait(
            AdminClientTemplates.plainAdminClient(
                testStorage.getNamespaceName(),
                testStorage.getAdminName(),
                KafkaResources.plainBootstrapAddress(testStorage.getClusterName())
            ).build()
        );
        final AdminClient localKafkaAdminClient = AdminClientUtils.getConfiguredAdminClient(testStorage.getNamespaceName(), testStorage.getAdminName());

        AdminClientUtils.waitForTopicPresence(localKafkaAdminClient, testStorage.getTopicName());

        final KafkaClients clients = ClientUtils.getInstantPlainClients(testStorage);
        resourceManager.createResourceWithWait(clients.producerStrimzi());
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        LOGGER.info("Try to delete KafkaTopic: {}/{}", Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());
        KafkaTopicResource.kafkaTopicClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(testStorage.getTopicName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        KafkaTopicUtils.waitForTopicStatusMessage(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), "TopicDeletionDisabledException");

        resourceManager.createResourceWithWait(clients.consumerStrimzi());
        ClientUtils.waitForInstantConsumerClientSuccess(testStorage);

        LOGGER.info("Enable automatic topic deletion");
        Map<String, String> kafkaPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());
        KafkaResource.replaceKafkaResourceInSpecificNamespace(testStorage.getNamespaceName(), testStorage.getClusterName(), k -> k.getSpec().getKafka().setConfig(Map.of("delete.topic.enable", true)));
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, kafkaPods);

        LOGGER.info("Deleting KafkaTopic: {}/{}", Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());
        KafkaTopicResource.kafkaTopicClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(testStorage.getTopicName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    @ParallelTest
    void testCreateTopicAfterUnsupportedOperation() {
        String topicName = "topic-with-replication-to-change";
        String newTopicName = "another-topic";

        KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, topicName, sharedTestStorage.getClusterName())
            .editSpec()
                .withReplicas(3)
                .withPartitions(3)
            .endSpec()
            .build();

        resourceManager.createResourceWithWait(kafkaTopic);
        KafkaTopicUtils.waitForKafkaTopicReady(Environment.TEST_SUITE_NAMESPACE, topicName);

        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, topicName, t -> {
            t.getSpec().setReplicas(1);
            t.getSpec().setPartitions(1);
        });
        KafkaTopicUtils.waitForKafkaTopicNotReady(Environment.TEST_SUITE_NAMESPACE, topicName);

        String exceptedMessage = "Decreasing partitions not supported";
        assertThat(KafkaTopicResource.kafkaTopicClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(topicName).get().getStatus().getConditions().get(0).getMessage(), is(exceptedMessage));

        String topicCRDMessage = KafkaTopicResource.kafkaTopicClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(topicName).get().getStatus().getConditions().get(0).getMessage();

        assertThat(topicCRDMessage, containsString(exceptedMessage));

        KafkaTopic newKafkaTopic = KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, newTopicName, sharedTestStorage.getClusterName(), 1, 1).build();

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
    void testKafkaTopicDifferentStatesInUTOMode() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        int initialReplicas = 1;
        int initialPartitions = 5;
        int decreasePartitions = 1;
        int expectedNumOfTopics = 1;
        int expectedObservedGeneration = 1;

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), sharedTestStorage.getClusterName(), initialPartitions, initialReplicas).build());
        KafkaTopicUtils.waitForKafkaTopicReady(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());

        LOGGER.info("Found the following Topics:");
        cmdKubeClient().list(KafkaTopic.RESOURCE_KIND).forEach(item -> {
            LOGGER.info("{}: {}", KafkaTopic.RESOURCE_KIND, item);
        });

        BaseMetricsCollector toMetricsCollector = new BaseMetricsCollector.Builder()
            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
            .withScraperPodName(scraperPodName)
            .withComponent(TopicOperatorMetricsComponent.create(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getClusterName()))
            .build();

        assertMetricResourceNotNull(toMetricsCollector, "strimzi_reconciliations_successful_total", KafkaTopic.RESOURCE_KIND);
        assertMetricResourceNotNull(toMetricsCollector, "strimzi_reconciliations_duration_seconds_bucket", KafkaTopic.RESOURCE_KIND);
        assertMetricResourceNotNull(toMetricsCollector, "strimzi_reconciliations_duration_seconds_max", KafkaTopic.RESOURCE_KIND);
        assertMetricResourceNotNull(toMetricsCollector, "strimzi_reconciliations_total", KafkaTopic.RESOURCE_KIND);
        assertMetricResourcesHigherThanOrEqualTo(toMetricsCollector, KafkaTopic.RESOURCE_KIND, expectedNumOfTopics);

        LOGGER.info("Checking if resource state metric reason message is \"none\" and KafkaTopic is ready");
        assertKafkaTopicStatus(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), Ready, True, expectedObservedGeneration);

        LOGGER.info("Changing Topic name in spec.topicName");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), kafkaTopic -> kafkaTopic.getSpec().setTopicName("some-other-name"));
        KafkaTopicUtils.waitForKafkaTopicNotReady(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());

        // reason and message in UTO mode
        String reason = "NotSupported";
        String reasonMessage = "Changing spec.topicName is not supported";

        assertKafkaTopicStatus(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), Ready, False, reason, reasonMessage, ++expectedObservedGeneration);

        LOGGER.info("Changing back to it's original name and scaling replicas to be higher number");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), kafkaTopic -> {
            kafkaTopic.getSpec().setTopicName(testStorage.getTopicName());
            kafkaTopic.getSpec().setReplicas(12);
        });

        KafkaTopicUtils.waitForKafkaTopicReplicasChange(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), 12);
        KafkaTopicUtils.waitForKafkaTopicNotReady(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());

        // message in UTO mode
        reasonMessage = "Replication factor change not supported";
        KafkaTopicUtils.waitForTopicStatusMessage(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), reasonMessage);
        assertKafkaTopicStatus(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), Ready, False, reason, reasonMessage, ++expectedObservedGeneration);

        LOGGER.info("Changing KafkaTopic's spec to correct state");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), kafkaTopic -> kafkaTopic.getSpec().setReplicas(initialReplicas));
        KafkaTopicUtils.waitForKafkaTopicReady(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());
        assertKafkaTopicStatus(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), Ready, True, ++expectedObservedGeneration);

        LOGGER.info("Decreasing number of partitions to {}", decreasePartitions);
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), kafkaTopic -> kafkaTopic.getSpec().setPartitions(decreasePartitions));
        KafkaTopicUtils.waitForKafkaTopicPartitionChange(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), decreasePartitions);
        KafkaTopicUtils.waitForKafkaTopicNotReady(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());

        // message in UTO mode
        reasonMessage = "Decreasing partitions not supported";
        assertKafkaTopicStatus(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), Ready, False, reason, reasonMessage, ++expectedObservedGeneration);

        // Wait some time to check if error is still present in KafkaTopic status
        LOGGER.info("Waiting {} ms for next reconciliation", topicOperatorReconciliationIntervalMs);

        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(topicOperatorReconciliationIntervalMs));
        assertKafkaTopicStatus(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), Ready, False, reason, reasonMessage, expectedObservedGeneration);

        LOGGER.info("Changing KafkaTopic's spec to correct state");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), kafkaTopic -> {
            kafkaTopic.getSpec().setReplicas(initialReplicas);
            kafkaTopic.getSpec().setPartitions(initialPartitions);
        });
        KafkaTopicUtils.waitForKafkaTopicReady(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());

        assertKafkaTopicStatus(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), Ready, True, ++expectedObservedGeneration);
        assertMetricValueHigherThanOrEqualTo(toMetricsCollector, "strimzi_reconciliations_failed_total\\{kind=\"" + KafkaTopic.RESOURCE_KIND + "\",.*}", 3);
    }

    @ParallelTest
    void testKafkaTopicChangingMinInSyncReplicas() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), sharedTestStorage.getClusterName(), 5).build());
        KafkaTopicUtils.waitForKafkaTopicReady(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());
        String invalidValue = "x";
        String reason = "KafkaError";

        CustomResourceStatus resourceStatus = Ready;
        ConditionStatus conditionStatus = False;

        String reasonMessage = String.format("Invalid value %s for configuration min.insync.replicas", invalidValue);

        LOGGER.info("Changing min.insync.replicas to random char");
        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), kafkaTopic -> kafkaTopic.getSpec().getConfig().put("min.insync.replicas", invalidValue)
        );
        KafkaTopicUtils.waitForKafkaTopicNotReady(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName());
        assertKafkaTopicStatus(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), resourceStatus, conditionStatus, reason, reasonMessage, 2);

        // Wait some time to check if error is still present in KafkaTopic status
        LOGGER.info("Waiting {} ms for next reconciliation", topicOperatorReconciliationIntervalMs);
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(topicOperatorReconciliationIntervalMs));
        assertKafkaTopicStatus(Environment.TEST_SUITE_NAMESPACE, testStorage.getTopicName(), resourceStatus, conditionStatus, reason, reasonMessage, 2);
    }

    /**
     * @description This test case checks that Kafka cluster will not act upon KafkaTopic CustomResources
     * which are not of its concern, i.e., KafkaTopic CustomResources are not labeled accordingly.
     *
     * @steps
     *  1. - Deploy Kafka with short reconciliation time configured on Topic Operator
     *     - Kafka is deployed
     *  2. - Create KafkaTopic CustomResource without any labels provided
     *     - KafkaTopic CustomResource is created
     *  3. - Verify that KafkaTopic specified by created KafkaTopic is not created
     *     - Given KafkaTopic is not present inside Kafka cluster
     *  4. - Delete given KafkaTopic CustomResource
     *     - KafkaTopic CustomResource is deleted
     *
     * @testcase
     *  - topic-operator
     *  - kafka-topic
     *  - labels
     */
    @ParallelNamespaceTest
    void testTopicWithoutLabels() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final long topicOperatorReconciliationMs = 10_000;

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        // Negative scenario: creating topic without any labels and make sure that TO can't handle this topic
        resourceManager.createResourceWithWait(
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build(),
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
                .editSpec()
                    .editEntityOperator()
                        .editTopicOperator()
                            .withReconciliationIntervalMs(topicOperatorReconciliationMs)
                        .endTopicOperator()
                    .endEntityOperator()
                .endSpec().build()
        );

        final String scraperPodName = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName()).get(0).getMetadata().getName();

        LOGGER.info("Creating KafkaTopic: {}/{} in without any label", testStorage.getNamespaceName(), testStorage.getTargetTopicName());
        resourceManager.createResourceWithoutWait(KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTargetTopicName(), testStorage.getClusterName(), 1, 1, 1)
            .editMetadata()
                .withLabels(null)
            .endMetadata().build()
        );

        // Checking that resource was created
        LOGGER.info("Verifying presence of KafkaTopic: {}/{}", testStorage.getNamespaceName(), testStorage.getTargetTopicName());
        assertThat(cmdKubeClient(testStorage.getNamespaceName()).list("kafkatopic"), hasItems(testStorage.getTargetTopicName()));

        // Checking that TO didn't handle new topic and controller pods don't contain new topic
        KafkaTopicUtils.verifyUnchangedTopicAbsence(testStorage.getNamespaceName(), scraperPodName, testStorage.getClusterName(), testStorage.getTargetTopicName(), topicOperatorReconciliationMs);

        // Checking TO logs
        String tOPodName = cmdKubeClient(testStorage.getNamespaceName()).listResourcesByLabel("pod", Labels.STRIMZI_NAME_LABEL + "=" + testStorage.getClusterName() + "-entity-operator").get(0);
        String tOlogs = kubeClient(testStorage.getNamespaceName()).logsInSpecificNamespace(testStorage.getNamespaceName(), tOPodName, "topic-operator");
        assertThat("TO's log contains information about created topic", tOlogs.contains(String.format("Created topic '%s'", testStorage.getTargetTopicName())), is(false));

        //Deleting topic
        cmdKubeClient(testStorage.getNamespaceName()).deleteByName("kafkatopic", testStorage.getTargetTopicName());
        KafkaTopicUtils.waitForKafkaTopicDeletion(testStorage.getNamespaceName(),  testStorage.getTargetTopicName());

        //Checking KafkaTopic is not present inside Kafka cluster
        List<String> topics = KafkaCmdClient.listTopicsUsingPodCli(testStorage.getNamespaceName(), scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()));
        assertThat(topics, not(hasItems(testStorage.getTargetTopicName())));
    }

    void assertKafkaTopicStatus(String namespaceName, String topicName, CustomResourceStatus status, ConditionStatus conditionStatus, int expectedObservedGeneration) {
        assertKafkaTopicStatus(namespaceName, topicName, status, conditionStatus, null, null, expectedObservedGeneration);
    }

    void assertKafkaTopicStatus(String namespaceName, String topicName, CustomResourceStatus status, ConditionStatus conditionStatus, String reason, String message, int expectedObservedGeneration) {
        KafkaTopicStatus kafkaTopicStatus = KafkaTopicResource.kafkaTopicClient().inNamespace(namespaceName).withName(topicName).get().getStatus();

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

    @BeforeAll
    void setup() {
        sharedTestStorage = new TestStorage(ResourceManager.getTestContext(), Environment.TEST_SUITE_NAMESPACE);
        
        this.clusterOperator = this.clusterOperator
            .defaultInstallation()
            .createInstallation()
            .runInstallation();

        LOGGER.info("Deploying shared Kafka: {}/{} across all test cases", Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getClusterName());

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getBrokerPoolName(), sharedTestStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getControllerPoolName(), sharedTestStorage.getClusterName(), 1).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getClusterName(), 3)
            .editSpec()
                .editEntityOperator()
                    .editOrNewTopicOperator()
                        .withReconciliationIntervalMs(TestConstants.RECONCILIATION_INTERVAL)
                    .endTopicOperator()
                .endEntityOperator()
            .endSpec()
            .build(),
            ScraperTemplates.scraperPod(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getScraperName()).build()
        );

        LOGGER.info("Deploying admin client across all test cases for namespace: {}", sharedTestStorage.getClusterName());
        resourceManager.createResourceWithWait(
            AdminClientTemplates.plainAdminClient(
                sharedTestStorage.getNamespaceName(),
                sharedTestStorage.getAdminName(),
                KafkaResources.plainBootstrapAddress(sharedTestStorage.getClusterName())
            ).build()
        );
        adminClient = AdminClientUtils.getConfiguredAdminClient(sharedTestStorage.getNamespaceName(), sharedTestStorage.getAdminName());

        scraperPodName = ScraperUtils.getScraperPod(Environment.TEST_SUITE_NAMESPACE).getMetadata().getName();
        topicOperatorReconciliationIntervalMs = KafkaResource.kafkaClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(sharedTestStorage.getClusterName()).get()
                .getSpec().getEntityOperator().getTopicOperator().getReconciliationIntervalMs() + 5_000L;
    }
}
