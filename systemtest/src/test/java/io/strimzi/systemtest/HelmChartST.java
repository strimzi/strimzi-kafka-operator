/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.test.ClusterOperator;
import io.strimzi.test.Namespace;
import io.strimzi.test.StrimziExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(StrimziExtension.class)
@Namespace(HelmChartST.NAMESPACE)
@ClusterOperator(useHelmChart = true)
class HelmChartST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(HelmChartST.class);

    static final String NAMESPACE = "helm-chart-cluster-test";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final String TOPIC_NAME = "test-topic";

    @Test
    @Tag("regression")
    void testDeployKafkaClusterViaHelmChart() {
        resources().kafkaEphemeral(CLUSTER_NAME, 3).done();
        resources().topic(CLUSTER_NAME, TOPIC_NAME).done();
        LOGGER.info("Running testDeployKafkaClusterViaHelmChart {}", CLUSTER_NAME);
        kubeClient.waitForStatefulSet(zookeeperClusterName(CLUSTER_NAME), 3);
        kubeClient.waitForStatefulSet(kafkaClusterName(CLUSTER_NAME), 3);
    }
}
