/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators.topic;

import io.fabric8.kubernetes.api.model.Pod;
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
public class TopicReplicationFactorChangeST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(TopicST.class);

    private TestStorage sharedTestStorage;
    private String scraperPodName;

    /**
     * Tests the behavior of creating a KafkaTopic with a higher replication factor than available brokers.
     * This test simulates a scenario where, for a freshly created KafkaTopic, the ReplicationChange
     * status will be empty because the update to Kafka will fail on reconciliation due to an insufficient number of brokers.
     *
     * Note:
     * For an already created KafkaTopic, attempting to increase the replication factor beyond available
     * brokers will append a ReplicationChange error message inside the status, which is not part of this test case.
     *
     * Steps:
     *  1. Create a KafkaTopic with more replicas than available brokers to induce a configuration error.
     *  2. Validate that the topic is recognized in the Kubernetes CRD but not actually created in Kafka.
     *  3. Check for specific error messages indicating the cause of the failure.
     *  4. Attempt to correct the issue by setting a valid replication factor and ensure that the changes are applied successfully.
     */
    @ParallelTest
    void testMoreReplicasThanAvailableBrokersWithFreshKafkaTopic() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final int topicPartitions = 5;
        final int topicReplicationFactor = 5; // Intentionally set higher than available brokers to induce failure

        // Create a KafkaTopic with more replicas than available brokers
        KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(this.sharedTestStorage.getClusterName(), testStorage.getTopicName(), topicPartitions, topicReplicationFactor, 1, this.sharedTestStorage.getNamespaceName()).build();
        resourceManager.createResourceWithoutWait(kafkaTopic);

        // Validate topic creation in Kubernetes and its absence in Kafka due to invalid configuration
        assertThat("Topic exists in Kafka CR (Kubernetes)", KafkaTopicUtils.hasTopicInCRK8s(kafkaTopic, testStorage.getTopicName()));
        assertThat("Topic doesn't exists in Kafka itself", !KafkaTopicUtils.hasTopicInKafka(testStorage.getTopicName(), this.sharedTestStorage.getClusterName(), this.scraperPodName));

        // Wait for the KafkaTopic to reflect the invalid configuration
        KafkaTopicUtils.waitForKafkaTopicNotReady(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        KafkaTopicStatus kafkaTopicStatus = KafkaTopicResource.kafkaTopicClient().inNamespace(this.sharedTestStorage.getNamespaceName()).withName(testStorage.getTopicName()).get().getStatus();

        String errorMessage = Environment.isUnidirectionalTopicOperatorEnabled() && Environment.isKRaftModeEnabled() ?
                "org.apache.kafka.common.errors.InvalidReplicationFactorException: Unable to replicate the partition 5 time(s): The target replication factor of 5 cannot be reached because only 3 broker(s) are registered." :
                "org.apache.kafka.common.errors.InvalidReplicationFactorException: Replication factor: 5 larger than available brokers: 3";

        assertThat(kafkaTopicStatus.getConditions().get(0).getMessage(), containsString(errorMessage));
        assertThat(kafkaTopicStatus.getConditions().get(0).getReason(), containsString(Environment.isUnidirectionalTopicOperatorEnabled() ? "KafkaError" : "CompletionException"));

        // also we will not see any replicationChange status here because UTO failed on reconciliation
        // to catch such issue, and it does not create a POST request to CC
        KafkaTopicUtils.waitForReplicaChangeStatusNotPresent(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        final int newTopicReplicationFactor = 3;
        long topicObservationGeneration = KafkaTopicUtils.topicObservationGeneration(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(
                testStorage.getTopicName(),
                topic -> topic.getSpec().setReplicas(newTopicReplicationFactor), this.sharedTestStorage.getNamespaceName());

        // then KafkaTopic replicaChange status should disappear
        KafkaTopicUtils.waitForReplicaChangeStatusNotPresent(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        KafkaTopicUtils.waitTopicHasRolled(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), topicObservationGeneration);
        KafkaTopicUtils.waitForKafkaTopicReady(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        KafkaTopicUtils.waitForKafkaTopicReplicasChange(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), newTopicReplicationFactor);

        // exchange messages within such Topic to prove that its operative
        sendAndRecvMessages(testStorage);
    }

    @ParallelTest
    void testKafkaTopicReplicaChangePositiveRoundTrip() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final int topicPartitions = 3;
        final int startingTopicReplicationFactor = 2;

        KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(this.sharedTestStorage.getClusterName(), testStorage.getTopicName(), topicPartitions, startingTopicReplicationFactor, 1, this.sharedTestStorage.getNamespaceName()).build();

        // --- 1st stage (creating a new with 2 replicas)
        resourceManager.createResourceWithWait(kafkaTopic);

        final int increasedTopicReplicationFactor = 3;

        // --- 2nd stage (increasing to 3 replicas)

        KafkaTopicUtils.waitUntilTopicObservationGenerationIsPresent(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        long topicObservationGeneration = KafkaTopicUtils.topicObservationGeneration(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(
                testStorage.getTopicName(),
                topic -> topic.getSpec().setReplicas(increasedTopicReplicationFactor), testStorage.getNamespaceName());

        KafkaTopicUtils.waitUntilReplicaChangeResolved(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // then KafkaTopic replicaChange status should disappear
        KafkaTopicUtils.waitForReplicaChangeStatusNotPresent(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // topic has rolled because of change in configuration
        KafkaTopicUtils.waitTopicHasRolled(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), topicObservationGeneration);
        KafkaTopicUtils.waitForKafkaTopicReady(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        KafkaTopicUtils.waitForKafkaTopicReplicasChange(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), increasedTopicReplicationFactor);

        // ----- 3rd stage (decreasing to 1 replicas)
        final int decreasedTopicReplicationFactor = 1;

        topicObservationGeneration = KafkaTopicUtils.topicObservationGeneration(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(
                testStorage.getTopicName(),
                topic -> topic.getSpec().setReplicas(decreasedTopicReplicationFactor), testStorage.getNamespaceName());

        // ongoing
        KafkaTopicUtils.waitUntilReplicaChangeOngoing(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        // then KafkaTopic replicaChange status should disappear
        KafkaTopicUtils.waitForReplicaChangeStatusNotPresent(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // topic has rolled because of change in configuration
        KafkaTopicUtils.waitTopicHasRolled(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), topicObservationGeneration);
        KafkaTopicUtils.waitForKafkaTopicReady(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        KafkaTopicUtils.waitForKafkaTopicReplicasChange(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), decreasedTopicReplicationFactor);

        // --- 4th stage (go back to 2 replicas)
        topicObservationGeneration = KafkaTopicUtils.topicObservationGeneration(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(
                testStorage.getTopicName(),
                topic -> topic.getSpec().setReplicas(startingTopicReplicationFactor), testStorage.getNamespaceName());

        // ongoing
        KafkaTopicUtils.waitUntilReplicaChangeOngoing(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        // then KafkaTopic replicaChange status should disappear
        KafkaTopicUtils.waitForReplicaChangeStatusNotPresent(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // topic has rolled because of change in configuration
        KafkaTopicUtils.waitTopicHasRolled(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), topicObservationGeneration);
        KafkaTopicUtils.waitForKafkaTopicReady(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        KafkaTopicUtils.waitForKafkaTopicReplicasChange(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), startingTopicReplicationFactor);

        // exchange messages within such Topic to prove that its operative
        sendAndRecvMessages(testStorage);
    }

    @ParallelTest
    void testKafkaTopicReplicaChangeNegativeRoundTrip() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final int topicPartitions = 3;
        final int startingTopicReplicationFactor = 3;

        KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(this.sharedTestStorage.getClusterName(), testStorage.getTopicName(), topicPartitions, startingTopicReplicationFactor, 1, this.sharedTestStorage.getNamespaceName()).build();

        // --- 1st stage (creating a new with 3 replicas)
        resourceManager.createResourceWithWait(kafkaTopic);

        final int wrongTopicReplicationFactor = 5;

        // --- 2nd stage (increasing to 5 replicas)

        KafkaTopicUtils.waitUntilTopicObservationGenerationIsPresent(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        long topicObservationGeneration = KafkaTopicUtils.topicObservationGeneration(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(
                testStorage.getTopicName(),
                topic -> topic.getSpec().setReplicas(wrongTopicReplicationFactor), testStorage.getNamespaceName());

        KafkaTopicUtils.waitTopicHasRolled(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), topicObservationGeneration);

        //  message: 'Replicas change failed (500), Error processing POST request ''/topic_configuration''
        //        due to: ''com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException:
        //        java.lang.RuntimeException: Unable to change replication factor (RF) of topics
        //        [my-topic-551958661-64449813] to 5 since there are only 3 alive brokers in
        //        the cluster. Requested RF cannot be more than number of alive brokers.''.'
        KafkaTopicUtils.waitForReplicaChangeFailureDueToInsufficientBrokers(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), wrongTopicReplicationFactor);

        // ----- 3rd stage (back to correct 3 replicas)
        topicObservationGeneration = KafkaTopicUtils.topicObservationGeneration(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(
                testStorage.getTopicName(),
                topic -> topic.getSpec().setReplicas(startingTopicReplicationFactor), testStorage.getNamespaceName());

        // then KafkaTopic replicaChange status should disappear because
        // replicationChange with ongoing state? (or I would simply have replicationChange not present because there is no need to call POST request to CC because we already have KT 3 replicas?
        KafkaTopicUtils.waitForReplicaChangeStatusNotPresent(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // topic has rolled because of change in configuration
        KafkaTopicUtils.waitTopicHasRolled(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), topicObservationGeneration);
        KafkaTopicUtils.waitForKafkaTopicReady(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        KafkaTopicUtils.waitForKafkaTopicReplicasChange(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), startingTopicReplicationFactor);

        // exchange messages within such Topic to prove that its operative
        sendAndRecvMessages(testStorage);
    }

    @IsolatedTest
    void testRecoveryOfReplicationChangeDuringCcCrash() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final int topicPartitions = 3;
        final int startingTopicReplicationFactor = 2;
        final int increasedTopicReplicationFactor = 3;
        final String ccPodName = kubeClient().listPodsByPrefixInName(this.sharedTestStorage.getNamespaceName(), CruiseControlResources.componentName(this.sharedTestStorage.getClusterName())).get(0).getMetadata().getName();
        final KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(this.sharedTestStorage.getClusterName(), testStorage.getTopicName(), topicPartitions, startingTopicReplicationFactor, 1, this.sharedTestStorage.getNamespaceName()).build();

        resourceManager.createResourceWithWait(kafkaTopic);

        // --- 2nd stage (increasing to 3 replicas)
        KafkaTopicUtils.waitUntilTopicObservationGenerationIsPresent(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        long topicObservationGeneration = KafkaTopicUtils.topicObservationGeneration(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(
                testStorage.getTopicName(),
                topic -> topic.getSpec().setReplicas(increasedTopicReplicationFactor), testStorage.getNamespaceName());

        KafkaTopicUtils.waitUntilReplicaChangeResolved(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        LOGGER.info("ReplicaChange appeared for KafkaTopic: {}/{}. Proceeding to the next stage.", this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // we should see that KafkaTopic is ongoing
        KafkaTopicUtils.waitUntilReplicaChangeOngoing(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        final Map<String, String> ccPod = DeploymentUtils.depSnapshot(this.sharedTestStorage.getNamespaceName(), CruiseControlResources.componentName(this.sharedTestStorage.getClusterName()));

        // here we should delete CruiseControl
        kubeClient().deletePodWithName(this.sharedTestStorage.getNamespaceName(), ccPodName);

        // we should see that KafkaTopic is ongoing
        KafkaTopicUtils.waitUntilReplicaChangeOngoing(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // then KafkaTopic replicaChange status should disappear
        KafkaTopicUtils.waitForReplicaChangeStatusNotPresent(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // cc should roll
        DeploymentUtils.waitTillDepHasRolled(this.sharedTestStorage.getNamespaceName(), CruiseControlResources.componentName(this.sharedTestStorage.getClusterName()), 1, ccPod);

        KafkaTopicUtils.waitTopicHasRolled(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), topicObservationGeneration);
        KafkaTopicUtils.waitForKafkaTopicReady(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        KafkaTopicUtils.waitForKafkaTopicReplicasChange(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), increasedTopicReplicationFactor);

        // exchange messages within such Topic to prove that its operative
        sendAndRecvMessages(testStorage);
    }

    @IsolatedTest
    void testRecoveryOfReplicationChangeDuringEoCrash() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final int topicPartitions = 3;
        final int startingTopicReplicationFactor = 3;
        final Pod eoPod = kubeClient().listPods(this.sharedTestStorage.getNamespaceName(),
                        KafkaResource.getEntityOperatorLabelSelector(this.sharedTestStorage.getClusterName())).stream()
                .findFirst()
                .orElseThrow();

        final KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(this.sharedTestStorage.getClusterName(), testStorage.getTopicName(), topicPartitions, startingTopicReplicationFactor, 1, this.sharedTestStorage.getNamespaceName()).build();

        resourceManager.createResourceWithWait(kafkaTopic);

        // --- 2nd stage (increasing to 3 replicas)
        final int decreasedTopicReplicationFactor = 2;

        KafkaTopicUtils.waitUntilTopicObservationGenerationIsPresent(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        long topicObservationGeneration = KafkaTopicUtils.topicObservationGeneration(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(
                testStorage.getTopicName(),
                topic -> topic.getSpec().setReplicas(decreasedTopicReplicationFactor), testStorage.getNamespaceName());

        KafkaTopicUtils.waitUntilReplicaChangeResolved(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        LOGGER.info("ReplicaChange appeared for KafkaTopic: {}/{}. Proceeding to the next stage.", this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // we should see that KafkaTopic is ongoing
        KafkaTopicUtils.waitUntilReplicaChangeOngoing(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // make a snapshot before deleting a Pod, inducing Chaos
        Map<String, String> eoPods = DeploymentUtils.depSnapshot(this.sharedTestStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(this.sharedTestStorage.getClusterName()));

        // delete EntityOperator Pod during replicaChange process
        kubeClient().deletePodWithName(this.sharedTestStorage.getNamespaceName(), eoPod.getMetadata().getName());

        // we should see that KafkaTopic is ongoing
        KafkaTopicUtils.waitUntilReplicaChangeOngoing(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        KafkaTopicUtils.waitForReplicaChangeStatusNotPresent(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // eo should roll
        DeploymentUtils.waitTillDepHasRolled(this.sharedTestStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(this.sharedTestStorage.getClusterName()), 1, eoPods);

        KafkaTopicUtils.waitTopicHasRolled(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), topicObservationGeneration);
        KafkaTopicUtils.waitForKafkaTopicReady(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        KafkaTopicUtils.waitForKafkaTopicReplicasChange(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), decreasedTopicReplicationFactor);

        // exchange messages within such Topic to prove that its operative
        sendAndRecvMessages(testStorage);
    }

    private void sendAndRecvMessages(final TestStorage testStorage) {
        KafkaClients kafkaClients = new KafkaClientsBuilder()
                .withTopicName(testStorage.getTopicName())
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(this.sharedTestStorage.getClusterName()))
                .withNamespaceName(this.sharedTestStorage.getNamespaceName())
                .withProducerName(testStorage.getProducerName())
                .withConsumerName(testStorage.getConsumerName())
                .withMessageCount(testStorage.getMessageCount())
                .build();

        resourceManager.createResourceWithWait(
                kafkaClients.producerStrimzi(),
                kafkaClients.consumerStrimzi()
        );
        ClientUtils.waitForClientsSuccess(testStorage);
    }

    @BeforeAll
    void setup() {
        this.sharedTestStorage = new TestStorage(ResourceManager.getTestContext(), Environment.TEST_SUITE_NAMESPACE);

        this.clusterOperator = this.clusterOperator
                .defaultInstallation()
                .createInstallation()
                .runInstallation();

        LOGGER.info("Deploying shared Kafka: {}/{} across all test cases", this.sharedTestStorage.getNamespaceName(), this.sharedTestStorage.getClusterName());

        resourceManager.createResourceWithWait(
                NodePoolsConverter.convertNodePoolsIfNeeded(
                        KafkaNodePoolTemplates.brokerPool(this.sharedTestStorage.getNamespaceName(), this.sharedTestStorage.getBrokerPoolName(), this.sharedTestStorage.getClusterName(), 3).build(),
                        KafkaNodePoolTemplates.controllerPool(this.sharedTestStorage.getNamespaceName(), this.sharedTestStorage.getControllerPoolName(), this.sharedTestStorage.getClusterName(), 1).build()
                )
        );
        // we need to deploy Kafka with CC enabled (to have RF feature)
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(this.sharedTestStorage.getClusterName(), 3, 3)
                    .editMetadata()
                        .withNamespace(this.sharedTestStorage.getNamespaceName())
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
                ScraperTemplates.scraperPod(this.sharedTestStorage.getNamespaceName(), this.sharedTestStorage.getScraperName()).build()
        );

        this.scraperPodName = ScraperUtils.getScraperPod(this.sharedTestStorage.getNamespaceName()).getMetadata().getName();
    }
}
