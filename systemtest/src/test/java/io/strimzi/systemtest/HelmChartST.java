/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.test.ClusterOperator;
import io.strimzi.test.JUnitGroup;
import io.strimzi.test.Namespace;
import io.strimzi.test.StrimziRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(StrimziRunner.class)
@Namespace(HelmChartST.NAMESPACE)
@ClusterOperator(useHelmChart = true)
public class HelmChartST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(HelmChartST.class);

    static final String NAMESPACE = "helm-chart-cluster-test";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final String TOPIC_NAME = "test-topic";

    @Test
    @JUnitGroup(name = "regression")
    public void testDeployKafkaClusterViaHelmChart() {
        resources().kafkaEphemeral(CLUSTER_NAME, 3).done();
        resources().topic(CLUSTER_NAME, TOPIC_NAME).done();
        LOGGER.info("Running testDeployKafkaClusterViaHelmChart {}", CLUSTER_NAME);
        this.kubeClient.waitForStatefulSet(zookeeperClusterName(CLUSTER_NAME), 3);
        this.kubeClient.waitForStatefulSet(kafkaClusterName(CLUSTER_NAME), 3);
    }
}
