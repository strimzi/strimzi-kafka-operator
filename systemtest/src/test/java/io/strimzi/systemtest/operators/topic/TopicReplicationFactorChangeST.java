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
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
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

    // FreshTopic vs Already created KafkaTopic
    //  1. for a fresh created KafkaTopic the ReplicationChange status will be empty because UTO will fail on reconciliation
    //  2. for already created it will append a ReplicationChange inside status with error message, which is tested in `testKafkaTopicReplicaChangeNegativeRoundTrip` test case
    @ParallelTest
    void testMoreReplicasThanAvailableBrokersWithFreshKafkaTopic() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final int topicPartitions = 5;
        final int topicReplicationFactor = 5;

        KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(this.sharedTestStorage.getClusterName(), testStorage.getTopicName(), topicPartitions, topicReplicationFactor, 1, this.sharedTestStorage.getNamespaceName()).build();
        resourceManager.createResourceWithoutWait(kafkaTopic);

        assertThat("Topic exists in Kafka CR (Kubernetes)", KafkaTopicUtils.hasTopicInCRK8s(kafkaTopic, testStorage.getTopicName()));
        assertThat("Topic doesn't exists in Kafka itself", !KafkaTopicUtils.hasTopicInKafka(testStorage.getTopicName(), this.sharedTestStorage.getClusterName(), this.scraperPodName));

        // one could see the appended ReplicationChange status only if there is correctly created KafkaTopic and one set
        // replication factor to incorrect number
        String errorMessage = Environment.isUnidirectionalTopicOperatorEnabled() && Environment.isKRaftModeEnabled() ?
                "org.apache.kafka.common.errors.InvalidReplicationFactorException: Unable to replicate the partition 5 time(s): The target replication factor of 5 cannot be reached because only 3 broker(s) are registered." :
                "org.apache.kafka.common.errors.InvalidReplicationFactorException: Replication factor: 5 larger than available brokers: 3";

        KafkaTopicUtils.waitForKafkaTopicNotReady(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        KafkaTopicStatus kafkaTopicStatus = KafkaTopicResource.kafkaTopicClient().inNamespace(this.sharedTestStorage.getNamespaceName()).withName(testStorage.getTopicName()).get().getStatus();

        assertThat(kafkaTopicStatus.getConditions().get(0).getMessage(), containsString(errorMessage));
        assertThat(kafkaTopicStatus.getConditions().get(0).getReason(), containsString(Environment.isUnidirectionalTopicOperatorEnabled() ? "KafkaError" : "CompletionException"));

        // also we will not see any replicationChange status here because UTO failed on reconciliation to catch such issue and it does not creating a POST request to CC
        KafkaTopicUtils.waitForReplicaChangeStatusNotPresent(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // new replication factor
        final int newTopicReplicationFactor = 3;

        long topicObservationGeneration = KafkaTopicUtils.topicObservationGeneration(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(
                testStorage.getTopicName(),
                topic -> topic.getSpec().setReplicas(newTopicReplicationFactor), this.sharedTestStorage.getNamespaceName());

        // ongoing?
//        KafkaTopicUtils.waitForReplicaChangeOngoing(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        // then KafkaTopic replicaChange status should disappear
        KafkaTopicUtils.waitForReplicaChangeStatusNotPresent(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        KafkaTopicUtils.waitTopicHasRolled(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), topicObservationGeneration);
        KafkaTopicUtils.waitForKafkaTopicReady(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        KafkaTopicUtils.waitForKafkaTopicReplicasChange(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), newTopicReplicationFactor);

        // TODO: exchange messages within such Topic to prove that its operative
    }

    // TODO: should we add for each change a sending and receiving messages and for prove that KafkaTopic is operative?
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
        // TOOD: I am not sure we want to monitor or check this transition? because It could lead in lot of race conditions I think
        // I would be fine to just check change of replicationFactor and topicUid change
        // replicaChangeState in ongoing
        // replicaChangeState in running
        // replicaChange is not present (task is already done)

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
        KafkaTopicUtils.waitForReplicaChangeOngoing(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
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
        KafkaTopicUtils.waitForReplicaChangeOngoing(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        // then KafkaTopic replicaChange status should disappear
        KafkaTopicUtils.waitForReplicaChangeStatusNotPresent(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // topic has rolled because of change in configuration
        KafkaTopicUtils.waitTopicHasRolled(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), topicObservationGeneration);
        KafkaTopicUtils.waitForKafkaTopicReady(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        KafkaTopicUtils.waitForKafkaTopicReplicasChange(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), startingTopicReplicationFactor);

        // TODO: exchange messages within such Topic to prove that its operative
    }

    // TODO: should we add for each change a sending and receiving messages and for prove that KafkaTopic is operative (here only at start and at the end)
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

        // TODO: exchange messages within such Topic to prove that its operative
    }

    @IsolatedTest
    void testRecoveryOfReplicationChangeDuringCcCrash() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final int topicPartitions = 3;
        final int startingTopicReplicationFactor = 2;
        final String ccPodName = kubeClient().listPodsByPrefixInName(this.sharedTestStorage.getNamespaceName(), CruiseControlResources.componentName(this.sharedTestStorage.getClusterName())).get(0).getMetadata().getName();
        final KafkaTopic kafkaTopic = KafkaTopicTemplates.topic(this.sharedTestStorage.getClusterName(), testStorage.getTopicName(), topicPartitions, startingTopicReplicationFactor, 1, this.sharedTestStorage.getNamespaceName()).build();

        resourceManager.createResourceWithWait(kafkaTopic);

        // --- 2nd stage (increasing to 3 replicas)
        final int increasedTopicReplicationFactor = 3;

        KafkaTopicUtils.waitUntilTopicObservationGenerationIsPresent(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        long topicObservationGeneration = KafkaTopicUtils.topicObservationGeneration(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        KafkaTopicResource.replaceTopicResourceInSpecificNamespace(
                testStorage.getTopicName(),
                topic -> topic.getSpec().setReplicas(increasedTopicReplicationFactor), testStorage.getNamespaceName());

        // It may happen that if we replace to good or bad
        // `message: Replicas change failed (500), Cluster model not ready but we wait here for 15 minutes`
        KafkaTopicUtils.waitUntilReplicaChangeResolved(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        LOGGER.info("ReplicaChange appeared for KafkaTopic: {}/{}. Proceeding to the next stage.", this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // we should see that KafkaTopic is ongoing
        KafkaTopicUtils.waitForReplicaChangeOngoing(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        final Map<String, String> ccPod = DeploymentUtils.depSnapshot(this.sharedTestStorage.getNamespaceName(), CruiseControlResources.componentName(this.sharedTestStorage.getClusterName()));

        // here we should delete CruiseControl
        kubeClient().deletePodWithName(this.sharedTestStorage.getNamespaceName(), ccPodName);

        // we should see that KafkaTopic is ongoing
        KafkaTopicUtils.waitForReplicaChangeOngoing(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // then KafkaTopic replicaChange status should disappear
        KafkaTopicUtils.waitForReplicaChangeStatusNotPresent(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // cc should roll
        DeploymentUtils.waitTillDepHasRolled(this.sharedTestStorage.getNamespaceName(), CruiseControlResources.componentName(this.sharedTestStorage.getClusterName()), 1, ccPod);

        KafkaTopicUtils.waitTopicHasRolled(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), topicObservationGeneration);
        KafkaTopicUtils.waitForKafkaTopicReady(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        KafkaTopicUtils.waitForKafkaTopicReplicasChange(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), increasedTopicReplicationFactor);
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

        // It may happen that if we replace to good or bad
        // `message: Replicas change failed (500), Cluster model not ready but we wait here for 15 minutes`
        KafkaTopicUtils.waitUntilReplicaChangeResolved(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        LOGGER.info("ReplicaChange appeared for KafkaTopic: {}/{}. Proceeding to the next stage.", this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // we should see that KafkaTopic is ongoing
        KafkaTopicUtils.waitForReplicaChangeOngoing(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        Map<String, String> eoPods = DeploymentUtils.depSnapshot(this.sharedTestStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(this.sharedTestStorage.getClusterName()));

        // delete Pod during replicaChange
        kubeClient().deletePodWithName(this.sharedTestStorage.getNamespaceName(), eoPod.getMetadata().getName());

        // we should see that KafkaTopic is ongoing
        KafkaTopicUtils.waitForReplicaChangeOngoing(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // then KafkaTopic replicaChange status should disappear
        KafkaTopicUtils.waitForReplicaChangeStatusNotPresent(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());

        // eo should roll
        DeploymentUtils.waitTillDepHasRolled(this.sharedTestStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(this.sharedTestStorage.getClusterName()), 1, eoPods);

        KafkaTopicUtils.waitTopicHasRolled(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), topicObservationGeneration);
        KafkaTopicUtils.waitForKafkaTopicReady(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName());
        KafkaTopicUtils.waitForKafkaTopicReplicasChange(this.sharedTestStorage.getNamespaceName(), testStorage.getTopicName(), decreasedTopicReplicationFactor);
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
//                            .addToConfig("num.broker.metrics.windows", "1")
//                            .addToConfig("min.samples.per.broker.metrics.window", "1")
//                            .addToConfig("num.partition.metrics.windows", "1")
//                            .addToConfig("min.samples.per.partition.metrics.window", "1")
                        .endCruiseControl()
                        .endSpec()
                    .build(),
                ScraperTemplates.scraperPod(this.sharedTestStorage.getNamespaceName(), this.sharedTestStorage.getScraperName()).build()
        );

        this.scraperPodName = ScraperUtils.getScraperPod(this.sharedTestStorage.getNamespaceName()).getMetadata().getName();
    }

}
