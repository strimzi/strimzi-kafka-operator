/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.cruisecontrol;

import io.strimzi.api.kafka.model.CruiseControlResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.BaseST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.specific.CruiseControlUtils;
import io.strimzi.test.WaitException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static io.strimzi.systemtest.Constants.CRUISE_CONTROL;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(REGRESSION)
@Tag(CRUISE_CONTROL)
public class CruiseControlIsolatedST extends BaseST {

    private static final Logger LOGGER = LogManager.getLogger(CruiseControlIsolatedST.class);
    private static final String NAMESPACE = "cruise-control-isolated-test";

    private static final String CRUISE_CONTROL_METRICS_TOPIC = "strimzi.cruisecontrol.metrics"; // partitions 1 , rf - 1
    private static final String CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC = "strimzi.cruisecontrol.modeltrainingsamples"; // partitions 32 , rf - 2
    private static final String CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC = "strimzi.cruisecontrol.partitionmetricsamples"; // partitions 32 , rf - 2


    @Test
    void testManuallyCreateMetricsReporterTopic() {
        KafkaResource.kafkaWithCruiseControlWithoutWaitAutoCreateTopicsDisable(CLUSTER_NAME, 3, 3);

        PodUtils.verifyThatRunningPodsAreStable(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));
        PodUtils.verifyThatRunningPodsAreStable(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));
        PodUtils.verifyThatRunningPodsAreStable(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME));

        PodUtils.waitUntilPodIsPresent(CruiseControlResources.deploymentName(CLUSTER_NAME));
        PodUtils.waitUntilPodIsInCrashLoopBackOff(kubeClient().listPodsByPrefixInName(CruiseControlResources.deploymentName(CLUSTER_NAME)).get(0).getMetadata().getName());

        LOGGER.info("Verifying that samples topics are not present because of " +
            "'Cruise Control cannot find partitions for the metrics reporter that topic matches strimzi.cruisecontrol.metrics in the target cluster'");

        assertThrows(WaitException.class, () -> CruiseControlUtils.verifyThatCruiseControlSamplesTopicsArePresent(Constants.GLOBAL_CRUISE_CONTROL_EXCEPT_FAIL_TIMEOUT));

        LOGGER.info("Verifying that metrics reporter topic is not present because of selected config 'auto.create.topics.enable=false'");

        assertThrows(WaitException.class, () -> CruiseControlUtils.verifyThatKafkaCruiseControlMetricReporterTopicIsPresent(Constants.GLOBAL_CRUISE_CONTROL_EXCEPT_FAIL_TIMEOUT));

        // Since log compaction may remove records needed by Cruise Control, all topics created by Cruise Control must
        // be configured with cleanup.policy=delete to disable log compaction.
        // More in docs 8.5.2. Topic creation and configuration
        KafkaTopicResource.topic(CLUSTER_NAME, CRUISE_CONTROL_METRICS_TOPIC)
            .editSpec()
                .addToConfig("cleanup.policy", "delete")
            .endSpec()
            .done();

        CruiseControlUtils.verifyThatKafkaCruiseControlMetricReporterTopicIsPresent(Constants.GLOBAL_CRUISE_CONTROL_TIMEOUT);

        LOGGER.info("Implicitly creating samples topics {},{} because of explicitly created {} topic",
            CRUISE_CONTROL_MODEL_TRAINING_SAMPLES_TOPIC, CRUISE_CONTROL_PARTITION_METRICS_SAMPLES_TOPIC, CRUISE_CONTROL_METRICS_TOPIC);

        CruiseControlUtils.verifyThatCruiseControlSamplesTopicsArePresent(Constants.GLOBAL_CRUISE_CONTROL_TIMEOUT);

        LOGGER.info("Verifying that Cruise control pod is running and ready (stable)");

        PodUtils.verifyThatRunningPodsAreStable(CruiseControlResources.deploymentName(CLUSTER_NAME));
    }

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();
        prepareEnvForOperator(NAMESPACE);

        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        KubernetesResource.clusterOperator(NAMESPACE).done();
    }
}
