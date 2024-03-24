/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators.topic;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicStatus;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.specific.ScraperUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.Map;

import static io.strimzi.systemtest.TestConstants.CRUISE_CONTROL;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

@Tag(REGRESSION)
@Tag(CRUISE_CONTROL)
/**
 * This class contains system tests for KafkaTopic resource changes, specifically focusing on testing replication factor adjustments
 * under various conditions. It simulates real-world scenarios such as insufficient brokers for the desired replication factor,
 * positive and negative replication factor changes, and recovery from failure states in the Kafka cluster or related components.
 *
 * The tests are structured to validate:
 * - The ability to handle KafkaTopic creation with a replication factor higher than available brokers, including error handling and retries.
 * - Positive testing scenarios where replication factors are increased and decreased successfully, ensuring the system correctly applies these changes.
 * - Negative testing scenarios where replication factor changes to a value higher than available brokers are attempted, verifying the system's response and error handling.
 * - Recovery of replication factor changes during crashes or restarts of critical components like Cruise Control and the Entity Operator, ensuring the system's resilience and fault tolerance.
 *
 * Each test case follows a specific scenario, with steps to manipulate the KafkaTopic's replication factor and validate the system's response and state transitions.
 * These tests ensure the robustness of KafkaTopic management and the system's ability to handle configuration changes and recover from errors.
 */

public class TopicReplicasChangeST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(TopicReplicasChangeST.class);

    private TestStorage sharedTestStorage;
    private String scraperPodName;

    /**
     * @description Verifies the system's handling of a KafkaTopic with a replication factor higher than the number of available brokers.
     * It tests the system's error recognition and handling capabilities, the ability to adjust to a valid replication factor,
     * and the successful creation and operational status of the topic.
     *
     * @steps
     *  1. - Create a KafkaTopic with more replicas than available brokers to induce a configuration error.
     *     - The topic exists in Kubernetes but not in Kafka, indicating a configuration error.
     *  2. - Check for specific error messages indicating the cause of the failure.
     *     - Specific error messages related to the replication factor issue are present.
     *  3. - Correct the issue by setting a valid replication factor and ensure changes are applied successfully.
     *     - Topic's replication factor is adjusted, and the topic becomes operational.
     *
     * @usecase
     *  - Replication factor management
     *  - Error handling and recovery
     */
    @ParallelTest
    void testMoreReplicasThanAvailableBrokersWithFreshKafkaTopic() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final int topicPartitions = 5;
        final int topicReplicationFactor = 5; // Intentionally set higher than available brokers to induce failure

        // Create and attempt to deploy a KafkaTopic with an invalid replication factor
        KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(sharedTestStorage.getClusterName(), testStorage.getTopicName(), topicPartitions, topicReplicationFactor, 1, sharedTestStorage.getNamespaceName()).build();
        resourceManager.createResourceWithoutWait(kafkaTopic);

        // Validate topic creation in Kubernetes and its absence in Kafka due to invalid configuration
        assertThat("Topic exists in Kafka CR (Kubernetes)", KafkaTopicUtils.hasTopicInCRK8s(kafkaTopic, testStorage.getTopicName()));
        assertThat("Topic doesn't exists in Kafka itself", !KafkaTopicUtils.hasTopicInKafka(testStorage.getTopicName(), sharedTestStorage.getClusterName(), scraperPodName));

        // Wait for the KafkaTopic to reflect the invalid configuration
        KafkaTopicUtils.waitForKafkaTopicNotReady(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        KafkaTopicStatus kafkaTopicStatus = KafkaTopicResource.kafkaTopicClient().inNamespace(sharedTestStorage.getNamespaceName()).withName(testStorage.getTopicName()).get().getStatus();

        String errorMessage = Environment.isKRaftModeEnabled() ?
                "org.apache.kafka.common.errors.InvalidReplicationFactorException: Unable to replicate the partition 5 time(s): The target replication factor of 5 cannot be reached because only 3 broker(s) are registered." :
                "org.apache.kafka.common.errors.InvalidReplicationFactorException: Replication factor: 5 larger than available brokers: 3";

        assertThat(kafkaTopicStatus.getConditions().get(0).getMessage(), containsString(errorMessage));
        assertThat(kafkaTopicStatus.getConditions().get(0).getReason(), containsString("KafkaError"));

        // also we will not see any replicationChange status here because UTO failed on reconciliation
        // to catch such issue, and it does not create a POST request to CC
        KafkaTopicUtils.waitForReplicaChangeStatusNotPresent(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        final int newTopicReplicationFactor = 3;
        long topicObservationGeneration = KafkaTopicUtils.topicObservationGeneration(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(
                testStorage.getTopicName(),
                topic -> topic.getSpec().setReplicas(newTopicReplicationFactor), sharedTestStorage.getNamespaceName());

        // then KafkaTopic replicaChange status should disappear after task is completed by CC
        KafkaTopicUtils.waitForReplicaChangeStatusNotPresent(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        verifyKafkaTopicAfterReplicationChange(testStorage, topicObservationGeneration, newTopicReplicationFactor);

        // exchange messages within such Topic to prove that its operative
        sendAndRecvMessages(testStorage);
    }

    /**
     * @description Tests the ability to change a KafkaTopic's replication factor positively through increasing and decreasing,
     * verifying the system applies these changes correctly and maintains the topic's operational status.
     *
     * @steps
     *  1. - Increase the KafkaTopic's replication factor and verify the change is applied.
     *     - Replication factor is increased, and the topic remains operational.
     *  2. - Return the replication factor to its original value and ensure the topic's functionality.
     *     - Topic's replication factor is reset, and it remains fully functional.
     *
     * @usecase
     *  - Dynamic replication factor adjustments
     *  - KafkaTopic status monitoring
     */
    @ParallelTest
    void testKafkaTopicReplicaChangePositiveRoundTrip() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final int topicPartitions = 3;
        final int startingTopicReplicationFactor = 2;

        KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(sharedTestStorage.getClusterName(), testStorage.getTopicName(), topicPartitions, startingTopicReplicationFactor, 1, sharedTestStorage.getNamespaceName()).build();

        // --- 1st stage (creating a new with 2 replicas)
        resourceManager.createResourceWithWait(kafkaTopic);

        final int increasedTopicReplicationFactor = 3;

        // --- 2nd stage (increasing to 3 replicas)

        KafkaTopicUtils.waitUntilTopicObservationGenerationIsPresent(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        long topicObservationGeneration = KafkaTopicUtils.topicObservationGeneration(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(
                testStorage.getTopicName(),
                topic -> topic.getSpec().setReplicas(increasedTopicReplicationFactor), testStorage.getNamespaceName());

        KafkaTopicUtils.waitUntilReplicaChangeResolved(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // then KafkaTopic replicaChange status should disappear after task is completed by CC
        KafkaTopicUtils.waitForReplicaChangeStatusNotPresent(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        verifyKafkaTopicAfterReplicationChange(testStorage, topicObservationGeneration, increasedTopicReplicationFactor);

        // --- 3rd stage (go back to 2 replicas)
        topicObservationGeneration = KafkaTopicUtils.topicObservationGeneration(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(
                testStorage.getTopicName(),
                topic -> topic.getSpec().setReplicas(startingTopicReplicationFactor), testStorage.getNamespaceName());

        //  replicaChange state should be in ongoing, because UTO does a POST request to CC
        KafkaTopicUtils.waitUntilReplicaChangeOngoing(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        // then KafkaTopic replicaChange status should disappear after task is completed by CC
        KafkaTopicUtils.waitForReplicaChangeStatusNotPresent(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        verifyKafkaTopicAfterReplicationChange(testStorage, topicObservationGeneration, startingTopicReplicationFactor);

        // exchange messages within such Topic to prove that its operative
        sendAndRecvMessages(testStorage);
    }

    /**
     * @description Simulates an attempt to increase a KafkaTopic's replication factor beyond the number of available brokers,
     * followed by a correction to a valid replication factor, to test system's error handling and recovery mechanisms.
     *
     * @steps
     *  1. - Incorrectly increase the KafkaTopic's replication factor beyond available brokers.
     *     - System identifies and reports the error due to insufficient brokers.
     *  2. - Correct the replication factor to a valid number.
     *     - System successfully applies the correction, and the topic becomes operational.
     *
     * @usecase
     *  - Error handling for invalid replication factor increases
     *  - Recovery from configuration errors
     */
    @ParallelTest
    void testKafkaTopicReplicaChangeNegativeRoundTrip() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final int topicPartitions = 3;
        final int startingTopicReplicationFactor = 3;

        KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(sharedTestStorage.getClusterName(), testStorage.getTopicName(), topicPartitions, startingTopicReplicationFactor, 1, sharedTestStorage.getNamespaceName()).build();

        // --- 1st stage (creating a new with 3 replicas)
        resourceManager.createResourceWithWait(kafkaTopic);

        final int wrongTopicReplicationFactor = 5;

        // --- 2nd stage (increasing to 5 replicas)

        KafkaTopicUtils.waitUntilTopicObservationGenerationIsPresent(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        long topicObservationGeneration = KafkaTopicUtils.topicObservationGeneration(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(
                testStorage.getTopicName(),
                topic -> topic.getSpec().setReplicas(wrongTopicReplicationFactor), testStorage.getNamespaceName());

        KafkaTopicUtils.waitTopicHasRolled(sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), topicObservationGeneration);

        //  message: 'Replicas change failed (500), Error processing POST request ''/topic_configuration''
        //        due to: ''com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException:
        //        java.lang.RuntimeException: Unable to change replication factor (RF) of topics
        //        [my-topic-551958661-64449813] to 5 since there are only 3 alive brokers in
        //        the cluster. Requested RF cannot be more than number of alive brokers.''.'
        KafkaTopicUtils.waitForReplicaChangeFailureDueToInsufficientBrokers(sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), wrongTopicReplicationFactor);

        // ----- 3rd stage (back to correct 3 replicas)
        topicObservationGeneration = KafkaTopicUtils.topicObservationGeneration(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(
                testStorage.getTopicName(),
                topic -> topic.getSpec().setReplicas(startingTopicReplicationFactor), testStorage.getNamespaceName());

        // then KafkaTopic replicaChange status should disappear after task is completed by CC
        // replicationChange not present because there is no need to call POST request via UTO to CC because we already have KT 3 replicas
        KafkaTopicUtils.waitForReplicaChangeStatusNotPresent(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        verifyKafkaTopicAfterReplicationChange(testStorage, topicObservationGeneration, startingTopicReplicationFactor);

        // exchange messages within such Topic to prove that its operative
        sendAndRecvMessages(testStorage);
    }

    /**
     * @description Tests the system's resilience and fault tolerance by simulating a replication factor change
     * during a Cruise Control crash, verifying the change continues as expected once Cruise Control recovers.
     *
     * @steps
     *  1. - Initiate a replication factor change for a KafkaTopic.
     *     - Replication change is ongoing.
     *  2. - Simulate a Cruise Control crash during the replication factor change.
     *     - Cruise Control crashes but is then recovered.
     *  3. - Verify the replication factor change completes successfully after recovery.
     *     - The replication factor change is successful, ensuring system resilience.
     *
     * @usecase
     *  - Handling component crashes
     *  - Ensuring replication factor changes under failure conditions
     */
    @IsolatedTest
    void testRecoveryOfReplicationChangeDuringCcCrash() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final int topicPartitions = 3;
        final int startingTopicReplicationFactor = 2;
        final int increasedTopicReplicationFactor = 3;
        final String ccPodName = kubeClient().listPodsByPrefixInName(sharedTestStorage.getNamespaceName(), CruiseControlResources.componentName(sharedTestStorage.getClusterName())).get(0).getMetadata().getName();
        final KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(sharedTestStorage.getClusterName(), testStorage.getTopicName(), topicPartitions, startingTopicReplicationFactor, 1, sharedTestStorage.getNamespaceName()).build();

        // -- 1st stage (start with 2 replicas)
        resourceManager.createResourceWithWait(kafkaTopic);

        // --- 2nd stage (increasing to 3 replicas)
        KafkaTopicUtils.waitUntilTopicObservationGenerationIsPresent(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        long topicObservationGeneration = KafkaTopicUtils.topicObservationGeneration(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(
                testStorage.getTopicName(),
                topic -> topic.getSpec().setReplicas(increasedTopicReplicationFactor), testStorage.getNamespaceName());

        KafkaTopicUtils.waitUntilReplicaChangeResolved(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        LOGGER.info("ReplicaChange appeared for KafkaTopic: {}/{}. Proceeding to the next stage.", sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // we should see that KafkaTopic is ongoing
        KafkaTopicUtils.waitUntilReplicaChangeOngoing(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        final Map<String, String> ccPod = DeploymentUtils.depSnapshot(sharedTestStorage.getNamespaceName(), CruiseControlResources.componentName(sharedTestStorage.getClusterName()));

        // --- 3rd stage (during ongoing replicaChange task we induce chaos and delete CruiseControl Pod)
        kubeClient().deletePodWithName(sharedTestStorage.getNamespaceName(), ccPodName);

        // we should see that KafkaTopic is ongoing
        KafkaTopicUtils.waitUntilReplicaChangeOngoing(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // then KafkaTopic replicaChange status should disappear after task is completed by CC
        KafkaTopicUtils.waitForReplicaChangeStatusNotPresent(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // cc should roll
        DeploymentUtils.waitTillDepHasRolled(sharedTestStorage.getNamespaceName(), CruiseControlResources.componentName(sharedTestStorage.getClusterName()), 1, ccPod);

        verifyKafkaTopicAfterReplicationChange(testStorage, topicObservationGeneration, increasedTopicReplicationFactor);

        // exchange messages within such Topic to prove that its operative
        sendAndRecvMessages(testStorage);
    }

    /**
     * @description Evaluates the system's ability to recover from a replication factor change operation during an unexpected
     * Entity Operator crash, ensuring that KafkaTopic changes are successfully applied and the system remains operational.
     *
     * @steps
     *  1. - Initiate a replication factor change for a KafkaTopic.
     *     - Replication change is in progress.
     *  2. - Simulate an Entity Operator crash during the replication factor change process.
     *     - Entity Operator crashes but is subsequently recovered.
     *  3. - Confirm that the replication factor change is successfully completed after the recovery.
     *     - The replication factor change is completed successfully, demonstrating system robustness.
     *
     * @usecase
     *  - Robustness in handling component failures
     *  - Successful application of KafkaTopic changes post-recovery
     */
    @IsolatedTest
    void testRecoveryOfReplicationChangeDuringEoCrash() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final int topicPartitions = 3;
        final int startingTopicReplicationFactor = 3;
        final String eoPodName = kubeClient().listPods(sharedTestStorage.getNamespaceName(),
                        KafkaResource.getEntityOperatorLabelSelector(sharedTestStorage.getClusterName())).stream()
                .findFirst()
                .orElseThrow()
                .getMetadata().getName();

        final KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(sharedTestStorage.getClusterName(), testStorage.getTopicName(), topicPartitions, startingTopicReplicationFactor, 1, sharedTestStorage.getNamespaceName()).build();

        resourceManager.createResourceWithWait(kafkaTopic);

        final int decreasedTopicReplicationFactor = 2;

        KafkaTopicUtils.waitUntilTopicObservationGenerationIsPresent(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        long topicObservationGeneration = KafkaTopicUtils.topicObservationGeneration(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(
                testStorage.getTopicName(),
                topic -> topic.getSpec().setReplicas(decreasedTopicReplicationFactor), testStorage.getNamespaceName());

        KafkaTopicUtils.waitUntilReplicaChangeResolved(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        LOGGER.info("ReplicaChange appeared for KafkaTopic: {}/{}. Proceeding to the next stage.", sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // we should see that KafkaTopic is ongoing
        KafkaTopicUtils.waitUntilReplicaChangeOngoing(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // make a snapshot before deleting a Pod, inducing Chaos
        Map<String, String> eoPods = DeploymentUtils.depSnapshot(sharedTestStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(sharedTestStorage.getClusterName()));

        // delete EntityOperator Pod during replicaChange process
        kubeClient().deletePodWithName(sharedTestStorage.getNamespaceName(), eoPodName);

        // we should see that KafkaTopic is ongoing
        KafkaTopicUtils.waitUntilReplicaChangeOngoing(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // then KafkaTopic replicaChange status should disappear after task is completed by CC
        KafkaTopicUtils.waitForReplicaChangeStatusNotPresent(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // eo should roll
        DeploymentUtils.waitTillDepHasRolled(sharedTestStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(sharedTestStorage.getClusterName()), 1, eoPods);

        verifyKafkaTopicAfterReplicationChange(testStorage, topicObservationGeneration, decreasedTopicReplicationFactor);

        // exchange messages within such Topic to prove that its operative
        sendAndRecvMessages(testStorage);
    }

    /**
     * Verifies the successful update and readiness of a KafkaTopic after a replication factor change.
     * Ensures the KafkaTopic has rolled to apply the new configuration, is in a ready state, and confirms
     * the replication factor matches the expected value.
     *
     * @param testStorage                The test storage containing context for the test execution.
     * @param topicObservationGeneration The generation of the KafkaTopic at the time of the update request.
     * @param expectedTopicReplicationFactor The expected replication factor for the KafkaTopic after the update.
     */
    private void verifyKafkaTopicAfterReplicationChange(final TestStorage testStorage,
                                                        final long topicObservationGeneration,
                                                        final int expectedTopicReplicationFactor) {
        KafkaTopicUtils.waitTopicHasRolled(sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), topicObservationGeneration);
        KafkaTopicUtils.waitForKafkaTopicReady(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        KafkaTopicUtils.waitForKafkaTopicReplicasChange(sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), expectedTopicReplicationFactor);
    }

    /**
     * Facilitates the sending and receiving of messages to verify the operational status of a Kafka topic.
     * Constructs Kafka clients configured with the necessary details from the provided test storage instance,
     * including topic name, cluster connection details, and client specifics. It then initiates the message
     * exchange process and waits for the operation to complete successfully.
     *
     * @param testStorage The test storage instance providing details for message exchange, including topic and client information.
     */
    private void sendAndRecvMessages(final TestStorage testStorage) {
        KafkaClients kafkaClients = new KafkaClientsBuilder()
                .withTopicName(testStorage.getTopicName())
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(sharedTestStorage.getClusterName()))
                .withNamespaceName(sharedTestStorage.getNamespaceName())
                .withProducerName(testStorage.getProducerName())
                .withConsumerName(testStorage.getConsumerName())
                .withMessageCount(testStorage.getMessageCount())
                .build();

        resourceManager.createResourceWithWait(
                kafkaClients.producerStrimzi(),
                kafkaClients.consumerStrimzi()
        );
        ClientUtils.waitForInstantClientSuccess(testStorage);
    }

    @BeforeAll
    void setup() {
        sharedTestStorage = new TestStorage(ResourceManager.getTestContext(), Environment.TEST_SUITE_NAMESPACE);

        clusterOperator = clusterOperator
                .defaultInstallation()
                .createInstallation()
                .runInstallation();

        LOGGER.info("Deploying shared Kafka: {}/{} across all test cases", sharedTestStorage.getNamespaceName(), sharedTestStorage.getClusterName());

        resourceManager.createResourceWithWait(
                NodePoolsConverter.convertNodePoolsIfNeeded(
                        KafkaNodePoolTemplates.brokerPool(sharedTestStorage.getNamespaceName(), sharedTestStorage.getBrokerPoolName(), sharedTestStorage.getClusterName(), 3).build(),
                        KafkaNodePoolTemplates.controllerPool(sharedTestStorage.getNamespaceName(), sharedTestStorage.getControllerPoolName(), sharedTestStorage.getClusterName(), 1).build()
                )
        );
        // we need to deploy Kafka with CC enabled (to have RF feature)
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(sharedTestStorage.getClusterName(), 3, 3)
                    .editMetadata()
                        .withNamespace(sharedTestStorage.getNamespaceName())
                    .endMetadata()
                    .editSpec()
                        .editKafka()
                            // The interval of collecting and sending the interested metrics.
                            // this reduce test execution time approximately from 15m to 4m
                            .addToConfig("cruise.control.metrics.reporter.metrics.reporting.interval.ms", 10000)
                        .endKafka()
                        .editEntityOperator()
                            .editOrNewTopicOperator()
                                .withReconciliationIntervalSeconds((int) TestConstants.RECONCILIATION_INTERVAL / 1000)
                            .endTopicOperator()
                        .endEntityOperator()
                        .editCruiseControl()
                            // add fixed more resources at startup to make CC faster in building cluster model
                            .withResources(new ResourceRequirementsBuilder()
                                .addToLimits("memory", new Quantity("1Gi"))
                                .addToRequests("memory", new Quantity("1Gi"))
                                .build())
                            // fine tune CC to make it quicker to build inner Cluster model with this settings
                            .addToConfig("metric.sampling.interval.ms", 60000)
                        .endCruiseControl()
                        .endSpec()
                    .build(),
                ScraperTemplates.scraperPod(sharedTestStorage.getNamespaceName(), sharedTestStorage.getScraperName()).build()
        );

        scraperPodName = ScraperUtils.getScraperPod(sharedTestStorage.getNamespaceName()).getMetadata().getName();
    }
}
