/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.api.kafka.model.CruiseControlSpec;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.balancing.BrokerCapacity;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;

@Tag(REGRESSION)
public class CruiseControlST extends BaseST {

    private static final Logger LOGGER = LogManager.getLogger(CruiseControlST.class);
    private static final String NAMESPACE = "cruise-control-test";

    private static final String CRUISE_CONTROL_NAME = "Cruise Control";
    private static final int CRUISE_CONTROL_DEFAULT_PORT = 9090;
    private static final String CRUISE_CONTROL_ENDPOINT = "/kafkacruisecontrol/state";

    private static final String CRUISE_CONTROL_METRICS_TOPIC = "strimzi.cruisecontrol.metrics"; // partitions 1 , rf - 1
    private static final String CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC = "strimzi.cruisecontrol.modeltrainingsamples"; // partitions 32 , rf - 2
    private static final String CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC = "strimzi.cruisecontrol.partitionmetricsamples"; // partitions 32 , rf - 2

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();
        prepareEnvForOperator(NAMESPACE);

        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        KubernetesResource.clusterOperator(NAMESPACE).done();

        KafkaResource.kafkaWithCruiseControl(CLUSTER_NAME, 3, 3).done();
    }

    @Test
    void testCruiseControlDeployment()  {
        String ccStatusCommand = "curl -X GET localhost:" + CRUISE_CONTROL_DEFAULT_PORT + CRUISE_CONTROL_ENDPOINT;

        String ccPodName = PodUtils.getFirstPodNameContaining("cruise-control");

        String result = cmdKubeClient().execInPodContainer(ccPodName, "cruise-control",
            "/bin/bash", "-c", ccStatusCommand).out();

        LOGGER.info("Verifying that {} is running inside the Pod {} and REST API is available", CRUISE_CONTROL_NAME, ccPodName);

        assertThat(result, not(containsString("404")));
        assertThat(result, containsString("RUNNING"));

        verifyThatCruiseControlTopicsArePresent();
    }

    @Test
    void testInstallationAndCreatedTopics() {

        String clusterName = "second" + CLUSTER_NAME;

        KafkaResource.kafkaEphemeral(clusterName + "1", 3, 1).done();

        KafkaResource.replaceKafkaResource(clusterName, kafka -> {
            LOGGER.info("Adding Cruise Control to the classic Kafka.");
            kafka.getSpec().setCruiseControl(new CruiseControlSpec());
        });

        PodUtils.waitUntilPodsStability(kubeClient().listPodsByPrefixInName(clusterName + "-kafka"));

        LOGGER.info("Verifying that {} topics are created after CC is instantiated.", CRUISE_CONTROL_NAME);

        KafkaTopicUtils.waitForKafkaTopicReady(CRUISE_CONTROL_METRICS_TOPIC);
        KafkaTopicUtils.waitForKafkaTopicReady(CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC);
        KafkaTopicUtils.waitForKafkaTopicReady(CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC);

        verifyThatCruiseControlTopicsArePresent();
    }


    @Test
    void testConfigurationChangeTriggersRollingUpdate() {
        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            LOGGER.info("Adding Cruise Control to the classic Kafka.");
            BrokerCapacity brokerCapacity = kafka.getSpec().getCruiseControl().getBrokerCapacity();

            brokerCapacity.setDisk("20Gi");
        });

        PodUtils.waitUntilPodsStability(kubeClient().listPodsByPrefixInName(CLUSTER_NAME + "-kafka"));

        KafkaTopicUtils.waitForKafkaTopicReady(CRUISE_CONTROL_METRICS_TOPIC);
        KafkaTopicUtils.waitForKafkaTopicReady(CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC);
        KafkaTopicUtils.waitForKafkaTopicReady(CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC);

        verifyThatCruiseControlTopicsArePresent();
       }

   // TODO: this tests will be next...

    @Test
    void testConfigurationFileIsCreated() { }

    @Test
    void testJvmOptions() {}

    @Test
    void testLogging() {}

    @Test
    void testReadinessProbe() {}

    @Test
    void testLivenessProbe() {}

    @Test
    void testTlsSideCar() {}

    @Test
    void testConfigurationReflectio() {
        // TODO:   /tmp/cruiseontrol.properties
    }

    @Test
    void testCapacityFile() {
        // TODO: ‘/tmp/capacity’ is created inside CC pod
    }

    private void verifyThatCruiseControlTopicsArePresent() {
        KafkaTopic metrics = KafkaTopicResource.kafkaTopicClient().inNamespace(kubeClient().getNamespace()).withName(CRUISE_CONTROL_METRICS_TOPIC).get();
        KafkaTopic modelTrainingSamples = KafkaTopicResource.kafkaTopicClient().inNamespace(kubeClient().getNamespace()).withName(CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC).get();
        KafkaTopic partitionsMetricsSamples = KafkaTopicResource.kafkaTopicClient().inNamespace(kubeClient().getNamespace()).withName(CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC).get();

        assertThat(metrics.getSpec().getPartitions(), is(1));
        assertThat(modelTrainingSamples.getSpec().getPartitions(), is(32));
        assertThat(partitionsMetricsSamples.getSpec().getPartitions(), is(32));

        assertThat(metrics.getSpec().getReplicas(), is(1));
        assertThat(modelTrainingSamples.getSpec().getReplicas(), is(32));
        assertThat(partitionsMetricsSamples.getSpec().getReplicas(), is(32));

    }
}
