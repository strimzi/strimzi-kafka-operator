/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.cruisecontrol;

import io.strimzi.api.kafka.model.KafkaTopicSpec;
import io.strimzi.api.kafka.model.status.KafkaRebalanceStatus;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.api.kafka.operator.assembly.KafkaRebalanceAnnotation;
import io.strimzi.api.kafka.operator.assembly.KafkaRebalanceState;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaRebalanceResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaRebalanceUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.CRUISE_CONTROL;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@Tag(REGRESSION)
@Tag(CRUISE_CONTROL)
public class CruiseControlIsolatedST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(CruiseControlIsolatedST.class);
    private static final String NAMESPACE = "cruise-control-isolated-test";

    private static final String CRUISE_CONTROL_METRICS_TOPIC = "strimzi.cruisecontrol.metrics"; // partitions 1 , rf - 1
    private static final String CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC = "strimzi.cruisecontrol.modeltrainingsamples"; // partitions 32 , rf - 2
    private static final String CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC = "strimzi.cruisecontrol.partitionmetricsamples"; // partitions 32 , rf - 2

    @Test
    void testAutoCreationOfCruiseControlTopics() {
        KafkaResource.kafkaWithCruiseControl(CLUSTER_NAME, 3, 3)
            .editOrNewSpec()
                .editKafka()
                    .addToConfig("auto.create.topics.enable", "false")
                .endKafka()
            .endSpec()
            .done();

        KafkaTopicSpec metricsTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE)
            .withName(CRUISE_CONTROL_METRICS_TOPIC).get().getSpec();
        KafkaTopicSpec modelTrainingTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE)
            .withName(CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC).get().getSpec();
        KafkaTopicSpec partitionMetricsTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE)
            .withName(CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC).get().getSpec();

        LOGGER.info("Checking partitions and replicas for {}", CRUISE_CONTROL_METRICS_TOPIC);
        assertThat(metricsTopic.getPartitions(), is(1));
        assertThat(metricsTopic.getReplicas(), is(1));

        LOGGER.info("Checking partitions and replicas for {}", CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC);
        assertThat(modelTrainingTopic.getPartitions(), is(32));
        assertThat(modelTrainingTopic.getReplicas(), is(2));

        LOGGER.info("Checking partitions and replicas for {}", CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC);
        assertThat(partitionMetricsTopic.getPartitions(), is(32));
        assertThat(partitionMetricsTopic.getReplicas(), is(2));
    }

    @Test
    @Tag(ACCEPTANCE)
    void testCruiseControlWithRebalanceResource() {
        KafkaResource.kafkaWithCruiseControl(CLUSTER_NAME, 3, 3).done();
        KafkaRebalanceResource.kafkaRebalance(CLUSTER_NAME).done();

        LOGGER.info("Verifying that KafkaRebalance resource is in {} state", KafkaRebalanceState.PendingProposal);

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(CLUSTER_NAME, KafkaRebalanceState.PendingProposal);

        LOGGER.info("Verifying that KafkaRebalance resource is in {} state", KafkaRebalanceState.ProposalReady);

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(CLUSTER_NAME, KafkaRebalanceState.ProposalReady);

        LOGGER.info("Triggering the rebalance with annotation {} of KafkaRebalance resource", "strimzi.io/rebalance=approve");

        String response = KafkaRebalanceUtils.annotateKafkaRebalanceResource(CLUSTER_NAME, KafkaRebalanceAnnotation.approve);

        LOGGER.info("Response from the annotation process {}", response);

        LOGGER.info("Verifying that annotation triggers the {} state", KafkaRebalanceState.Rebalancing);

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(CLUSTER_NAME, KafkaRebalanceState.Rebalancing);

        LOGGER.info("Verifying that KafkaRebalance is in the {} state", KafkaRebalanceState.Ready);

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(CLUSTER_NAME, KafkaRebalanceState.Ready);
    }

    @Test
    void testCruiseControlWithSingleNodeKafka() {
        String errMessage =  "Kafka " + NAMESPACE + "/" + CLUSTER_NAME + " has invalid configuration." +
            " Cruise Control cannot be deployed with a single-node Kafka cluster. It requires " +
            "at least two Kafka nodes.";

        LOGGER.info("Deploying single node Kafka with CruiseControl");
        KafkaResource.kafkaWithCruiseControlWithoutWait(CLUSTER_NAME, 1, 1);
        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(CLUSTER_NAME, NAMESPACE, errMessage, Duration.ofMinutes(6).toMillis());

        KafkaStatus kafkaStatus = KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus();

        assertThat(kafkaStatus.getConditions().get(0).getReason(), is("InvalidResourceException"));

        LOGGER.info("Increasing Kafka nodes to 3");
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> kafka.getSpec().getKafka().setReplicas(3));
        KafkaUtils.waitForKafkaReady(CLUSTER_NAME);

        kafkaStatus = KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus();
        assertThat(kafkaStatus.getConditions().get(0).getMessage(), is(not(errMessage)));
    }

    @Test
    void testCruiseControlTopicExclusion() {
        String excludedTopic1 = "excluded-topic-1";
        String excludedTopic2 = "excluded-topic-2";
        String includedTopic = "included-topic";

        KafkaResource.kafkaWithCruiseControl(CLUSTER_NAME, 3, 3).done();
        KafkaTopicResource.topic(CLUSTER_NAME, excludedTopic1).done();
        KafkaTopicResource.topic(CLUSTER_NAME, excludedTopic2).done();
        KafkaTopicResource.topic(CLUSTER_NAME, includedTopic).done();

        KafkaRebalanceResource.kafkaRebalance(CLUSTER_NAME)
            .editOrNewSpec()
                .withExcludedTopics("excluded-.*")
            .endSpec()
            .done();

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(CLUSTER_NAME, KafkaRebalanceState.ProposalReady);

        LOGGER.info("Checking status of KafkaRebalance");
        KafkaRebalanceStatus kafkaRebalanceStatus = KafkaRebalanceResource.kafkaRebalanceClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus();
        assertThat(kafkaRebalanceStatus.getOptimizationResult().get("excludedTopics").toString(), containsString(excludedTopic1));
        assertThat(kafkaRebalanceStatus.getOptimizationResult().get("excludedTopics").toString(), containsString(excludedTopic2));
        assertThat(kafkaRebalanceStatus.getOptimizationResult().get("excludedTopics").toString(), not(containsString(includedTopic)));

        KafkaRebalanceUtils.annotateKafkaRebalanceResource(CLUSTER_NAME, KafkaRebalanceAnnotation.approve);
        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(CLUSTER_NAME, KafkaRebalanceState.Ready);
    }

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE);
    }
}
