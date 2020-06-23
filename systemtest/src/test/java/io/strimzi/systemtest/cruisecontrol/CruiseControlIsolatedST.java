/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.cruisecontrol;

import io.strimzi.api.kafka.model.KafkaTopicSpec;
import io.strimzi.systemtest.BaseST;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaRebalanceResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaRebalanceUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.CRUISE_CONTROL;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.resources.ResourceManager.cmdKubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@Tag(REGRESSION)
@Tag(CRUISE_CONTROL)
public class CruiseControlIsolatedST extends BaseST {

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

        LOGGER.info("Verifying that KafkaRebalance resource is in {} state", KafkaRebalanceUtils.KafkaRebalanceState.PendingProposal);

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(CLUSTER_NAME, KafkaRebalanceUtils.KafkaRebalanceState.PendingProposal);

        LOGGER.info("Verifying that KafkaRebalance resource is in {} state", KafkaRebalanceUtils.KafkaRebalanceState.ProposalReady);

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(CLUSTER_NAME, KafkaRebalanceUtils.KafkaRebalanceState.ProposalReady);

        LOGGER.info("Triggering the rebalance with annotation {} of KafkaRebalance resource", "strimzi.io/rebalance=approve");

        // attach the approve annotation -> RS
        LOGGER.info("Executing command in the namespace {}", cmdKubeClient().namespace(NAMESPACE).namespace());
        String response = ResourceManager.cmdKubeClient().namespace(NAMESPACE).execInCurrentNamespace("annotate", "kafkarebalance", CLUSTER_NAME, "strimzi.io/rebalance=approve").out();

        LOGGER.info("Response from the annotation process {}", response);

        LOGGER.info("Verifying that annotation triggers the {} state", KafkaRebalanceUtils.KafkaRebalanceState.Rebalancing);

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(CLUSTER_NAME, KafkaRebalanceUtils.KafkaRebalanceState.Rebalancing);

        LOGGER.info("Verifying that KafkaRebalance is in the {} state", KafkaRebalanceUtils.KafkaRebalanceState.Ready);

        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(CLUSTER_NAME, KafkaRebalanceUtils.KafkaRebalanceState.Ready);
    }

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
//        prepareEnvForOperator(NAMESPACE);
//
//        applyRoleBindings(NAMESPACE);
//        // 050-Deployment
//        KubernetesResource.clusterOperator(NAMESPACE).done();
        installClusterOperator(NAMESPACE);
    }
}
