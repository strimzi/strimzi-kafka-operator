/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators.topic;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicStatus;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.labels.LabelSelectors;
import io.strimzi.systemtest.resources.CrdClients;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
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

import static io.strimzi.systemtest.TestTags.CRUISE_CONTROL;
import static io.strimzi.systemtest.TestTags.REGRESSION;
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
@SuiteDoc(
    description = @Desc("Validates KafkaTopic replication factor change logic using the Topic Operator and Cruise Control."),
    beforeTestSteps = {
        @Step(value = "Deploy Kafka cluster with Cruise Control and Topic Operator configured for rapid reconciliation.", expected = "Kafka cluster with Cruise Control is deployed and ready."),
        @Step(value = "Deploy scraper pod for topic status validation.", expected = "Scraper pod is running and accessible."),
        @Step(value = "Create initial KafkaTopic resources with valid and invalid replication factors.", expected = "KafkaTopics are created and visible to Topic Operator.")
    },
    labels = {
        @Label(TestDocsLabels.TOPIC_OPERATOR)
    }
)
public class TopicReplicasChangeST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(TopicReplicasChangeST.class);

    private TestStorage sharedTestStorage;
    private String scraperPodName;

    @TestDoc(
        description = @Desc("Verifies behavior when creating a KafkaTopic with a replication factor exceeding the number of brokers."),
        steps = {
            @Step(value = "Create a KafkaTopic with more replicas than available brokers.", expected = "Topic exists in Kubernetes, but not in Kafka."),
            @Step(value = "Check KafkaTopic status for replication factor error.", expected = "Error message and status reason indicate KafkaError."),
            @Step(value = "Adjust replication factor to valid value and wait for reconciliation.", expected = "KafkaTopic becomes Ready and replication status clears.")
        },
        labels = {
            @Label(TestDocsLabels.TOPIC_OPERATOR)
        }
    )
    @ParallelTest
    void testMoreReplicasThanAvailableBrokersWithFreshKafkaTopic() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final int topicPartitions = 5;
        final int topicReplicationFactor = 5; // Intentionally set higher than available brokers to induce failure

        // Create and attempt to deploy a KafkaTopic with an invalid replication factor
        KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), sharedTestStorage.getClusterName(), topicPartitions, topicReplicationFactor, 1).build();
        KubeResourceManager.get().createResourceWithoutWait(kafkaTopic);

        // Validate topic creation in Kubernetes and its absence in Kafka due to invalid configuration
        assertThat("Topic exists in Kafka CR (Kubernetes)", KafkaTopicUtils.hasTopicInCRK8s(kafkaTopic, testStorage.getTopicName()));
        assertThat("Topic doesn't exists in Kafka itself", !KafkaTopicUtils.hasTopicInKafka(testStorage.getTopicName(), sharedTestStorage.getClusterName(), scraperPodName));

        // Wait for the KafkaTopic to reflect the invalid configuration
        KafkaTopicUtils.waitForKafkaTopicNotReady(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        KafkaTopicStatus kafkaTopicStatus = CrdClients.kafkaTopicClient().inNamespace(sharedTestStorage.getNamespaceName()).withName(testStorage.getTopicName()).get().getStatus();

        String errorMessage = "org.apache.kafka.common.errors.InvalidReplicationFactorException: Unable to replicate the partition 5 time(s): The target replication factor of 5 cannot be reached because only 3 broker(s) are registered.";

        assertThat(kafkaTopicStatus.getConditions().get(0).getMessage(), containsString(errorMessage));
        assertThat(kafkaTopicStatus.getConditions().get(0).getReason(), containsString("KafkaError"));

        // also we will not see any replicationChange status here because UTO failed on reconciliation
        // to catch such issue, and it does not create a POST request to CC
        KafkaTopicUtils.waitForReplicaChangeStatusNotPresent(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        final int newTopicReplicationFactor = 3;
        long topicObservationGeneration = KafkaTopicUtils.topicObservationGeneration(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        KafkaTopicUtils.replace(
            sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), topic -> topic.getSpec().setReplicas(newTopicReplicationFactor)
        );

        // then KafkaTopic replicaChange status should disappear after task is completed by CC
        KafkaTopicUtils.waitForReplicaChangeStatusNotPresent(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        verifyKafkaTopicAfterReplicationChange(testStorage, topicObservationGeneration, newTopicReplicationFactor);

        // exchange messages within such Topic to prove that its operative
        sendAndRecvMessages(testStorage);
    }

    @TestDoc(
        description = @Desc("Tests increasing and then decreasing a KafkaTopic’s replication factor, verifying correctness and topic readiness."),
        steps = {
            @Step(value = "Create KafkaTopic with initial replication factor.", expected = "KafkaTopic is created and ready."),
            @Step(value = "Increase the replication factor.", expected = "Replica change is applied and KafkaTopic remains Ready."),
            @Step(value = "Decrease replication factor back to original value.", expected = "KafkaTopic is updated and Ready with correct replica count.")
        },
        labels = {
            @Label(TestDocsLabels.TOPIC_OPERATOR)
        }
    )
    @ParallelTest
    void testKafkaTopicReplicaChangePositiveRoundTrip() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final int topicPartitions = 3;
        final int startingTopicReplicationFactor = 2;

        KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), sharedTestStorage.getClusterName(), topicPartitions, startingTopicReplicationFactor, 1).build();

        // --- 1st stage (creating a new with 2 replicas)
        KubeResourceManager.get().createResourceWithWait(kafkaTopic);

        final int increasedTopicReplicationFactor = 3;

        // --- 2nd stage (increasing to 3 replicas)

        KafkaTopicUtils.waitUntilTopicObservationGenerationIsPresent(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        long topicObservationGeneration = KafkaTopicUtils.topicObservationGeneration(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        KafkaTopicUtils.replace(
            testStorage.getNamespaceName(), testStorage.getTopicName(), topic -> topic.getSpec().setReplicas(increasedTopicReplicationFactor)
        );

        KafkaTopicUtils.waitUntilReplicaChangeResolved(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // then KafkaTopic replicaChange status should disappear after task is completed by CC
        KafkaTopicUtils.waitForReplicaChangeStatusNotPresent(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        verifyKafkaTopicAfterReplicationChange(testStorage, topicObservationGeneration, increasedTopicReplicationFactor);

        // --- 3rd stage (go back to 2 replicas)
        topicObservationGeneration = KafkaTopicUtils.topicObservationGeneration(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        KafkaTopicUtils.replace(
            testStorage.getNamespaceName(), testStorage.getTopicName(), topic -> topic.getSpec().setReplicas(startingTopicReplicationFactor)
        );

        //  replicaChange state should be in ongoing, because UTO does a POST request to CC
        KafkaTopicUtils.waitUntilReplicaChangeOngoing(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        // then KafkaTopic replicaChange status should disappear after task is completed by CC
        KafkaTopicUtils.waitForReplicaChangeStatusNotPresent(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        verifyKafkaTopicAfterReplicationChange(testStorage, topicObservationGeneration, startingTopicReplicationFactor);

        // exchange messages within such Topic to prove that its operative
        sendAndRecvMessages(testStorage);
    }

    @TestDoc(
        description = @Desc("Attempts to increase KafkaTopic replication factor beyond broker count, then corrects it."),
        steps = {
            @Step(value = "Create KafkaTopic with valid replication factor.", expected = "KafkaTopic is created and Ready."),
            @Step(value = "Attempt to increase replication factor beyond available brokers.", expected = "Replica change fails with error due to insufficient brokers."),
            @Step(value = "Restore to valid replication factor.", expected = "KafkaTopic becomes Ready and replica change status clears.")
        },
        labels = {
            @Label(TestDocsLabels.TOPIC_OPERATOR)
        }
    )
    @ParallelTest
    void testKafkaTopicReplicaChangeNegativeRoundTrip() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final int topicPartitions = 3;
        final int startingTopicReplicationFactor = 3;

        KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), sharedTestStorage.getClusterName(), topicPartitions, startingTopicReplicationFactor, 1).build();

        // --- 1st stage (creating a new with 3 replicas)
        KubeResourceManager.get().createResourceWithWait(kafkaTopic);

        final int wrongTopicReplicationFactor = 5;

        // --- 2nd stage (increasing to 5 replicas)

        KafkaTopicUtils.waitUntilTopicObservationGenerationIsPresent(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        long topicObservationGeneration = KafkaTopicUtils.topicObservationGeneration(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        KafkaTopicUtils.replace(
            testStorage.getNamespaceName(), testStorage.getTopicName(), topic -> topic.getSpec().setReplicas(wrongTopicReplicationFactor)
        );

        KafkaTopicUtils.waitTopicHasRolled(sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), topicObservationGeneration);

        //  message: 'Replicas change failed (500), Error processing POST request ''/topic_configuration''
        //        due to: ''com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException:
        //        java.lang.RuntimeException: Unable to change replication factor (RF) of topics
        //        [my-topic-551958661-64449813] to 5 since there are only 3 alive brokers in
        //        the cluster. Requested RF cannot be more than number of alive brokers.''.'
        KafkaTopicUtils.waitForReplicaChangeFailureDueToInsufficientBrokers(sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), wrongTopicReplicationFactor);

        // ----- 3rd stage (back to correct 3 replicas)
        topicObservationGeneration = KafkaTopicUtils.topicObservationGeneration(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        KafkaTopicUtils.replace(
            testStorage.getNamespaceName(), testStorage.getTopicName(), topic -> topic.getSpec().setReplicas(startingTopicReplicationFactor)
        );

        // then KafkaTopic replicaChange status should disappear after task is completed by CC
        // replicationChange not present because there is no need to call POST request via UTO to CC because we already have KT 3 replicas
        KafkaTopicUtils.waitForReplicaChangeStatusNotPresent(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        verifyKafkaTopicAfterReplicationChange(testStorage, topicObservationGeneration, startingTopicReplicationFactor);

        // exchange messages within such Topic to prove that its operative
        sendAndRecvMessages(testStorage);
    }

    @TestDoc(
        description = @Desc("Verifies replication factor change recovery after Cruise Control crash."),
        steps = {
            @Step(value = "Initiate replication factor change.", expected = "Replica change is ongoing."),
            @Step(value = "Crash the Cruise Control pod.", expected = "Cruise Control is terminated and recovers."),
            @Step(value = "Wait for change to complete.", expected = "Replica change status clears and KafkaTopic is Ready.")
        },
        labels = {
            @Label(TestDocsLabels.TOPIC_OPERATOR)
        }
    )
    @IsolatedTest
    void testRecoveryOfReplicationChangeDuringCcCrash() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final int topicPartitions = 3;
        final int startingTopicReplicationFactor = 2;
        final int increasedTopicReplicationFactor = 3;
        final String ccPodName = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(sharedTestStorage.getNamespaceName(), CruiseControlResources.componentName(sharedTestStorage.getClusterName())).get(0).getMetadata().getName();
        final KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), sharedTestStorage.getClusterName(), topicPartitions, startingTopicReplicationFactor, 1).build();

        // -- 1st stage (start with 2 replicas)
        KubeResourceManager.get().createResourceWithWait(kafkaTopic);

        // --- 2nd stage (increasing to 3 replicas)
        KafkaTopicUtils.waitUntilTopicObservationGenerationIsPresent(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        long topicObservationGeneration = KafkaTopicUtils.topicObservationGeneration(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        KafkaTopicUtils.replace(
            testStorage.getNamespaceName(), testStorage.getTopicName(), topic -> topic.getSpec().setReplicas(increasedTopicReplicationFactor)
        );

        KafkaTopicUtils.waitUntilReplicaChangeResolved(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        LOGGER.info("ReplicaChange appeared for KafkaTopic: {}/{}. Proceeding to the next stage.", sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // we should see that KafkaTopic is ongoing
        KafkaTopicUtils.waitUntilReplicaChangeOngoing(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        final Map<String, String> ccPod = DeploymentUtils.depSnapshot(sharedTestStorage.getNamespaceName(), CruiseControlResources.componentName(sharedTestStorage.getClusterName()));

        // --- 3rd stage (during ongoing replicaChange task we induce chaos and delete CruiseControl Pod)
        KubeResourceManager.get().deleteResourceWithoutWait(KubeResourceManager.get().kubeClient().getClient().pods().inNamespace(sharedTestStorage.getNamespaceName()).withName(ccPodName).get());

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

    @TestDoc(
        description = @Desc("Verifies KafkaTopic replication change proceeds after Entity Operator crash."),
        steps = {
            @Step(value = "Initiate replication factor change.", expected = "Replica change is ongoing."),
            @Step(value = "Crash the Entity Operator pod.", expected = "Entity Operator is terminated and recovers."),
            @Step(value = "Wait for change to complete.", expected = "KafkaTopic is updated and Ready with correct replication factor.")
        },
        labels = {
            @Label(TestDocsLabels.TOPIC_OPERATOR)
        }
    )
    @IsolatedTest
    void testRecoveryOfReplicationChangeDuringEoCrash() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final int topicPartitions = 3;
        final int startingTopicReplicationFactor = 3;
        final String eoPodName = KubeResourceManager.get().kubeClient().listPods(sharedTestStorage.getNamespaceName(),
                        LabelSelectors.entityOperatorLabelSelector(sharedTestStorage.getClusterName())).stream()
                .findFirst()
                .orElseThrow()
                .getMetadata().getName();

        final KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), sharedTestStorage.getClusterName(), topicPartitions, startingTopicReplicationFactor, 1).build();

        KubeResourceManager.get().createResourceWithWait(kafkaTopic);

        final int decreasedTopicReplicationFactor = 2;

        KafkaTopicUtils.waitUntilTopicObservationGenerationIsPresent(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        long topicObservationGeneration = KafkaTopicUtils.topicObservationGeneration(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        KafkaTopicUtils.replace(
            testStorage.getNamespaceName(), testStorage.getTopicName(), topic -> topic.getSpec().setReplicas(decreasedTopicReplicationFactor)
        );

        KafkaTopicUtils.waitUntilReplicaChangeResolved(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        LOGGER.info("ReplicaChange appeared for KafkaTopic: {}/{}. Proceeding to the next stage.", sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // we should see that KafkaTopic is ongoing
        KafkaTopicUtils.waitUntilReplicaChangeOngoing(sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // make a snapshot before deleting a Pod, inducing Chaos
        Map<String, String> eoPods = DeploymentUtils.depSnapshot(sharedTestStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(sharedTestStorage.getClusterName()));

        // delete EntityOperator Pod during replicaChange process
        KubeResourceManager.get().deleteResourceWithoutWait(KubeResourceManager.get().kubeClient().getClient().pods().inNamespace(sharedTestStorage.getNamespaceName()).withName(eoPodName).get());

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

        KubeResourceManager.get().createResourceWithWait(
                kafkaClients.producerStrimzi(),
                kafkaClients.consumerStrimzi()
        );
        ClientUtils.waitForInstantClientSuccess(testStorage);
    }

    @BeforeAll
    void setup() {
        sharedTestStorage = new TestStorage(KubeResourceManager.get().getTestContext(), Environment.TEST_SUITE_NAMESPACE);

        SetupClusterOperator
            .getInstance()
            .withDefaultConfiguration()
            .install();

        LOGGER.info("Deploying shared Kafka: {}/{} across all test cases", sharedTestStorage.getNamespaceName(), sharedTestStorage.getClusterName());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(sharedTestStorage.getNamespaceName(), sharedTestStorage.getBrokerPoolName(), sharedTestStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(sharedTestStorage.getNamespaceName(), sharedTestStorage.getControllerPoolName(), sharedTestStorage.getClusterName(), 1).build()
        );
        
        // we need to deploy Kafka with CC enabled (to have RF feature)
        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafkaWithCruiseControlTunedForFastModelGeneration(sharedTestStorage.getNamespaceName(), sharedTestStorage.getClusterName(), 3)
                .editSpec()
                    .editEntityOperator()
                        .editOrNewTopicOperator()
                            .withReconciliationIntervalMs(10_000L)
                        .endTopicOperator()
                    .endEntityOperator()
                    .editCruiseControl()
                        // reserve some resources at startup for faster cluster model generation
                        .withResources(new ResourceRequirementsBuilder()
                            .addToLimits("memory", new Quantity("1Gi"))
                            .addToRequests("memory", new Quantity("1Gi"))
                            .build())
                    .endCruiseControl()
                    .endSpec()
                .build(),
            ScraperTemplates.scraperPod(sharedTestStorage.getNamespaceName(), sharedTestStorage.getScraperName()).build()
        );

        scraperPodName = ScraperUtils.getScraperPod(sharedTestStorage.getNamespaceName()).getMetadata().getName();
    }
}
