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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static io.strimzi.test.k8s.BaseKubeClient.CM;
import static io.strimzi.test.k8s.BaseKubeClient.DEPLOYMENT;
import static io.strimzi.test.k8s.BaseKubeClient.SERVICE;
import static io.strimzi.test.k8s.BaseKubeClient.STATEFUL_SET;
import static java.util.concurrent.TimeUnit.SECONDS;

@RunWith(StrimziRunner.class)
@JUnitGroup(name = "regression")
@Namespace(RecoveryST.NAMESPACE)
@ClusterOperator
public class RecoveryST extends AbstractST {

    static final String NAMESPACE = "recovery-cluster-test";
    static final String CLUSTER_NAME = "recovery-cluster";

    private static final Logger LOGGER = LogManager.getLogger(RecoveryST.class);
    private static Resources classResources;

    @Test
    public void testRecoveryFromEntityOperatorDeletion() {
        // kafka cluster already deployed
        String entityOperatorDeploymentName = entityOperatorDeploymentName(CLUSTER_NAME);
        LOGGER.info("Running testRecoveryFromEntityOperatorDeletion with cluster {}", CLUSTER_NAME);

        kubeClient.deleteByName(DEPLOYMENT, entityOperatorDeploymentName);
        kubeClient.waitForResourceDeletion(DEPLOYMENT, entityOperatorDeploymentName);

        LOGGER.info("Waiting for recovery {}", entityOperatorDeploymentName);
        kubeClient.waitForDeployment(entityOperatorDeploymentName, 1);

        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(stopwatch.runtime(SECONDS));
    }

    @Test
    public void testRecoveryFromKafkaStatefulSetDeletion() {
        // kafka cluster already deployed
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
        // kafka cluster already deployed
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
        // kafka cluster already deployed
        String kafkaServiceName = kafkaServiceName(CLUSTER_NAME);
        LOGGER.info("Running deleteKafkaService with cluster {}", CLUSTER_NAME);

        kubeClient.deleteByName(SERVICE, kafkaServiceName);

        LOGGER.info("Waiting for creation {}", kafkaServiceName);
        kubeClient.waitForResourceCreation(SERVICE, kafkaServiceName);

        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(stopwatch.runtime(SECONDS));
    }

    @Test
    public void testRecoveryFromZookeeperServiceDeletion() {
        // kafka cluster already deployed
        String zookeeperServiceName = zookeeperServiceName(CLUSTER_NAME);

        LOGGER.info("Running deleteKafkaService with cluster {}", CLUSTER_NAME);

        kubeClient.deleteByName(SERVICE, zookeeperServiceName);

        LOGGER.info("Waiting for creation {}", zookeeperServiceName);
        kubeClient.waitForResourceCreation(SERVICE, zookeeperServiceName);

        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(stopwatch.runtime(SECONDS));
    }

    @Test
    public void testRecoveryFromKafkaHeadlessServiceDeletion() {
        // kafka cluster already deployed
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
        // kafka cluster already deployed
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
        // kafka cluster already deployed
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
        // kafka cluster already deployed
        String zookeeperMetricsConfigName = zookeeperMetricsConfigName(CLUSTER_NAME);
        LOGGER.info("Running deleteZookeeperMetricsConfig with cluster {}", CLUSTER_NAME);

        kubeClient.deleteByName(CM, zookeeperMetricsConfigName);
        kubeClient.waitForResourceDeletion(CM, zookeeperMetricsConfigName);

        LOGGER.info("Waiting for creation {}", zookeeperMetricsConfigName);
        kubeClient.waitForResourceCreation(CM, zookeeperMetricsConfigName);

        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(stopwatch.runtime(SECONDS));
    }

    @BeforeClass
    public static void createClassResources() {
        classResources = new Resources(namespacedClient());
        classResources().kafkaEphemeral(CLUSTER_NAME, 1).done();
    }

    @AfterClass
    public static void deleteClassResources() {
        LOGGER.info("Deleting resources after the test class");
        classResources.deleteResources();
        classResources = null;
    }

    static Resources classResources() {
        return classResources;
    }
}
