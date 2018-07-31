/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.test.ClusterOperator;
import io.strimzi.test.JUnitGroup;
import io.strimzi.test.KafkaFromClasspathYaml;
import io.strimzi.test.Namespace;
import io.strimzi.test.StrimziRunner;
import io.strimzi.test.Topic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(StrimziRunner.class)
@Namespace(HelmChartClusterIT.NAMESPACE)
@ClusterOperator(useHelmChart = true)
public class HelmChartClusterIT extends KafkaClusterIT {

    private static final Logger LOGGER = LogManager.getLogger(HelmChartClusterIT.class);

    static final String NAMESPACE = "helm-chart-cluster-test";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final String TOPIC_NAME = "test-topic";

    @Override
    protected String testNamespace() {
        return NAMESPACE;
    }

    @Test
    @JUnitGroup(name = "regression")
    @KafkaFromClasspathYaml()
    @Topic(name = TOPIC_NAME, clusterName = "my-cluster")
    public void testDeployKafkaClusterViaHelmChart() {
        this.kubeClient.waitForStatefulSet(zookeeperClusterName(CLUSTER_NAME), 3);
        this.kubeClient.waitForStatefulSet(kafkaClusterName(CLUSTER_NAME), 3);
    }
}
