/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.cruisecontrol;

import io.strimzi.systemtest.BaseST;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.utils.specific.CruiseControlUtils;
import io.strimzi.test.WaitException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class CruiseControlIsolatedST extends BaseST {

    private static final Logger LOGGER = LogManager.getLogger(CruiseControlConfigurationST.class);
    private static final String NAMESPACE = "cruise-control-isolated-test";

    private static final String CRUISE_CONTROL_METRICS_TOPIC = "strimzi.cruisecontrol.metrics"; // partitions 1 , rf - 1

    @Test
    void testManuallyCreateMetricsReporterTopic() {
        KafkaResource.kafkaWithCruiseControl(CLUSTER_NAME, 3, 3)
            .editSpec()
                .editKafka()
                    .addToConfig("auto.create.topics.enable", "false")
                .endKafka()
            .endSpec()
            .done();

        LOGGER.info("Verifying that samples topics are present");

        CruiseControlUtils.verifyThatCruiseControlSamplesTopicsArePresent();

        LOGGER.info("Verifying that metrics reporter topic is not present because of selected config 'auto.create.topics.enable=false'");

        assertThrows(WaitException.class, CruiseControlUtils::verifyThatKafkaCruiseControlMetricReporterTopicIsPresent);

        KafkaTopicResource.topic(CLUSTER_NAME, CRUISE_CONTROL_METRICS_TOPIC).done();

        CruiseControlUtils.verifyThatKafkaCruiseControlMetricReporterTopicIsPresent();
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
