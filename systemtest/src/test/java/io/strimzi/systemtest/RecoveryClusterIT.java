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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;

import static io.strimzi.test.k8s.BaseKubeClient.CM;
import static io.strimzi.test.k8s.BaseKubeClient.DEPLOYMENT;
import static io.strimzi.test.k8s.BaseKubeClient.SERVICE;
import static io.strimzi.test.k8s.BaseKubeClient.STATEFUL_SET;
import static java.util.concurrent.TimeUnit.SECONDS;

@RunWith(StrimziRunner.class)
@JUnitGroup(name = "regression")
@Namespace(RecoveryClusterIT.NAMESPACE)
@ClusterOperator
@KafkaFromClasspathYaml
public class RecoveryClusterIT extends AbstractClusterIT {

    static final String NAMESPACE = "recovery-cluster-test";
    static final String CLUSTER_NAME = "recovery-cluster";

    private static final Logger LOGGER = LogManager.getLogger(RecoveryClusterIT.class);

    @Test
    public void testRecoveryFromTopicOperatorDeletion() {
        // kafka cluster already deployed via annotation
        String topicOperatorDeploymentName = topicOperatorDeploymentName(CLUSTER_NAME);
        LOGGER.info("Running deleteTopicOperatorDeployment with cluster {}", CLUSTER_NAME);

        kubeClient.deleteByName(DEPLOYMENT, topicOperatorDeploymentName);
        kubeClient.waitForResourceDeletion(DEPLOYMENT, topicOperatorDeploymentName);

        LOGGER.info("Waiting for recovery {}", topicOperatorDeploymentName);
        kubeClient.waitForDeployment(topicOperatorDeploymentName);

        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(stopwatch.runtime(SECONDS));
    }

    @Test
    public void testRecoveryFromKafkaStatefulSetDeletion() {
        // kafka cluster already deployed via annotation
        String kafkaStatefulSetName = kafkaClusterName(CLUSTER_NAME);
        LOGGER.info("Running deleteKafkaStatefulSet with cluster {}", CLUSTER_NAME);

        kubeClient.deleteByName(STATEFUL_SET, kafkaStatefulSetName);
        kubeClient.waitForResourceDeletion(STATEFUL_SET, kafkaStatefulSetName);

        LOGGER.info("Waiting for recovery {}", kafkaStatefulSetName);
        kubeClient.waitForStatefulSet(kafkaStatefulSetName, 1);

        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(stopwatch.runtime(SECONDS));
    }

    @Test
    public void testRecoveryFromZookeeperStatefulSetDeletion() {
        // kafka cluster already deployed via annotation
        String zookeeperStatefulSetName = zookeeperClusterName(CLUSTER_NAME);
        LOGGER.info("Running deleteZookeeperStatefulSet with cluster {}", CLUSTER_NAME);

        kubeClient.deleteByName(STATEFUL_SET, zookeeperStatefulSetName);
        kubeClient.waitForResourceDeletion(STATEFUL_SET, zookeeperStatefulSetName);

        LOGGER.info("Waiting for recovery {}", zookeeperStatefulSetName);
        kubeClient.waitForStatefulSet(zookeeperStatefulSetName, 1);

        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(stopwatch.runtime(SECONDS));
    }

    @Test
    public void testRecoveryFromKafkaServiceDeletion() {
        // kafka cluster already deployed via annotation
        String kafkaServiceName = kafkaClusterName(CLUSTER_NAME);
        LOGGER.info("Running deleteKafkaService with cluster {}", CLUSTER_NAME);

        kubeClient.deleteByName(SERVICE, kafkaServiceName);

        LOGGER.info("Waiting for creation {}", kafkaServiceName);
        kubeClient.waitForResourceCreation(SERVICE, kafkaServiceName);

        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(stopwatch.runtime(SECONDS));
    }

    @Test
    public void testRecoveryFromZookeeperServiceDeletion() {
        // kafka cluster already deployed via annotation
        String zookeeperServiceName = zookeeperClusterName(CLUSTER_NAME);

        LOGGER.info("Running deleteKafkaService with cluster {}", CLUSTER_NAME);

        kubeClient.deleteByName(SERVICE, zookeeperServiceName);

        LOGGER.info("Waiting for creation {}", zookeeperServiceName);
        kubeClient.waitForResourceCreation(SERVICE, zookeeperServiceName);

        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(stopwatch.runtime(SECONDS));
    }

    @Test
    public void testRecoveryFromKafkaHeadlessServiceDeletion() {
        // kafka cluster already deployed via annotation
        String kafkaHeadlessServiceName = kafkaHeadlessServiceName(CLUSTER_NAME);
        LOGGER.info("Running deleteKafkaHeadlessService with cluster {}", CLUSTER_NAME);

        kubeClient.deleteByName(SERVICE, kafkaHeadlessServiceName);

        LOGGER.info("Waiting for creation {}", kafkaHeadlessServiceName);
        kubeClient.waitForResourceCreation(SERVICE, kafkaHeadlessServiceName);

        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(stopwatch.runtime(SECONDS));
    }

    @Test
    public void testRecoveryFromZookeeperHeadlessServiceDeletion() {
        // kafka cluster already deployed via annotation
        String zookeeperHeadlessServiceName = zookeeperHeadlessServiceName(CLUSTER_NAME);
        LOGGER.info("Running deleteKafkaHeadlessService with cluster {}", CLUSTER_NAME);

        kubeClient.deleteByName(SERVICE, zookeeperHeadlessServiceName);

        LOGGER.info("Waiting for creation {}", zookeeperHeadlessServiceName);
        kubeClient.waitForResourceCreation(SERVICE, zookeeperHeadlessServiceName);

        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(stopwatch.runtime(SECONDS));
    }

    @Test
    public void testRecoveryFromKafkaMetricsConfigDeletion() {
        // kafka cluster already deployed via annotation
        String kafkaMetricsConfigName = kafkaMetricsConfigName(CLUSTER_NAME);
        LOGGER.info("Running deleteKafkaMetricsConfig with cluster {}", CLUSTER_NAME);

        kubeClient.deleteByName(CM, kafkaMetricsConfigName);
        kubeClient.waitForResourceDeletion(CM, kafkaMetricsConfigName);

        LOGGER.info("Waiting for creation {}", kafkaMetricsConfigName);
        kubeClient.waitForResourceCreation(CM, kafkaMetricsConfigName);

        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(stopwatch.runtime(SECONDS));
    }

    @Test
    public void testRecoveryFromZookeeperMetricsConfigDeletion() {
        // kafka cluster already deployed via annotation
        String zookeeperMetricsConfigName = zookeeperMetricsConfigName(CLUSTER_NAME);
        LOGGER.info("Running deleteZookeeperMetricsConfig with cluster {}", CLUSTER_NAME);

        kubeClient.deleteByName(CM, zookeeperMetricsConfigName);
        kubeClient.waitForResourceDeletion(CM, zookeeperMetricsConfigName);

        LOGGER.info("Waiting for creation {}", zookeeperMetricsConfigName);
        kubeClient.waitForResourceCreation(CM, zookeeperMetricsConfigName);

        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(stopwatch.runtime(SECONDS));
    }
}
