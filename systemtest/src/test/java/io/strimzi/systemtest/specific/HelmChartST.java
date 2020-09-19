/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.specific;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.resources.operator.HelmResource;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;

import static io.strimzi.systemtest.Constants.HELM;

@Tag(HELM)
class HelmChartST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(HelmChartST.class);

    static final String NAMESPACE = "helm-chart-cluster-test";
    private static final String CLUSTER_NAME = "my-cluster";

    @Test
    void testDeployKafkaClusterViaHelmChart() {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();
        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME).done();
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), 3);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);
    }

    @BeforeAll
    void setup() {
        LOGGER.info("Creating resources before the test class");
        cluster.createNamespace(NAMESPACE);
        HelmResource.clusterOperator();
    }

    @Override
    protected void tearDownEnvironmentAfterAll() {
        cluster.deleteNamespaces();
    }
}
