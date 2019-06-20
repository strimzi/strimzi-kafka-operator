/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.systemtest.utils.StUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.strimzi.systemtest.Constants.REGRESSION;

@Tag(REGRESSION)
class HelmChartST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(HelmChartST.class);

    static final String NAMESPACE = "helm-chart-cluster-test";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final String TOPIC_NAME = "test-topic";

    @Test
    void testDeployKafkaClusterViaHelmChart() {
        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3).done();
        testMethodResources().topic(CLUSTER_NAME, TOPIC_NAME).done();
        StUtils.waitForAllStatefulSetPodsReady(zookeeperClusterName(CLUSTER_NAME), 3);
        StUtils.waitForAllStatefulSetPodsReady(kafkaClusterName(CLUSTER_NAME), 3);
    }

    @BeforeEach
    void createTestResources() {
        createTestMethodResources();
    }

    @BeforeAll
    void setupEnvironment() {
        LOGGER.info("Creating resources before the test class");
        createNamespace(NAMESPACE);
        deployClusterOperatorViaHelmChart();
    }

    @Override
    void tearDownEnvironmentAfterEach() throws Exception {
        deleteTestMethodResources();
        waitForDeletion(Constants.TIMEOUT_TEARDOWN);
    }

    @Override
    void tearDownEnvironmentAfterAll() {
        deleteClusterOperatorViaHelmChart();
        deleteNamespaces();
    }

    @Override
    void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        deleteClusterOperatorViaHelmChart();
        deleteNamespaces();
        createNamespace(NAMESPACE);
    }
}
