/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.test.timemeasuring.Operation;
import io.strimzi.test.timemeasuring.TimeMeasuringSystem;
import io.strimzi.test.extensions.StrimziExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static io.strimzi.test.extensions.StrimziExtension.REGRESSION;
import static io.strimzi.test.k8s.BaseKubeClient.CM;
import static io.strimzi.test.k8s.BaseKubeClient.DEPLOYMENT;
import static io.strimzi.test.k8s.BaseKubeClient.SERVICE;
import static io.strimzi.test.k8s.BaseKubeClient.STATEFUL_SET;

@ExtendWith(StrimziExtension.class)
@Tag(REGRESSION)
class RecoveryST extends AbstractST {

    static final String NAMESPACE = "recovery-cluster-test";
    static final String CLUSTER_NAME = "recovery-cluster";

    private static final Logger LOGGER = LogManager.getLogger(RecoveryST.class);

    @Test
    void testRecoveryFromEntityOperatorDeletion() {
        operationID = startTimeMeasuring(Operation.CLUSTER_RECOVERY);
        // kafka cluster already deployed
        String entityOperatorDeploymentName = entityOperatorDeploymentName(CLUSTER_NAME);
        LOGGER.info("Running testRecoveryFromEntityOperatorDeletion with cluster {}", CLUSTER_NAME);

        KUBE_CLIENT.deleteByName(DEPLOYMENT, entityOperatorDeploymentName);
        KUBE_CLIENT.waitForResourceDeletion(DEPLOYMENT, entityOperatorDeploymentName);

        LOGGER.info("Waiting for recovery {}", entityOperatorDeploymentName);
        KUBE_CLIENT.waitForDeployment(entityOperatorDeploymentName, 1);

        TimeMeasuringSystem.stopOperation(operationID);
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, operationID));
    }

    @Test
    void testRecoveryFromKafkaStatefulSetDeletion() {
        operationID = startTimeMeasuring(Operation.CLUSTER_RECOVERY);
        // kafka cluster already deployed
        String kafkaStatefulSetName = kafkaClusterName(CLUSTER_NAME);
        LOGGER.info("Running deleteKafkaStatefulSet with cluster {}", CLUSTER_NAME);

        KUBE_CLIENT.deleteByName(STATEFUL_SET, kafkaStatefulSetName);
        KUBE_CLIENT.waitForResourceDeletion(STATEFUL_SET, kafkaStatefulSetName);

        LOGGER.info("Waiting for recovery {}", kafkaStatefulSetName);
        KUBE_CLIENT.waitForStatefulSet(kafkaStatefulSetName, 1);

        TimeMeasuringSystem.stopOperation(operationID);
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, operationID));
    }

    @Test
    void testRecoveryFromZookeeperStatefulSetDeletion() {
        operationID = startTimeMeasuring(Operation.CLUSTER_RECOVERY);
        // kafka cluster already deployed
        String zookeeperStatefulSetName = zookeeperClusterName(CLUSTER_NAME);
        LOGGER.info("Running deleteZookeeperStatefulSet with cluster {}", CLUSTER_NAME);

        KUBE_CLIENT.deleteByName(STATEFUL_SET, zookeeperStatefulSetName);
        KUBE_CLIENT.waitForResourceDeletion(STATEFUL_SET, zookeeperStatefulSetName);

        LOGGER.info("Waiting for recovery {}", zookeeperStatefulSetName);
        KUBE_CLIENT.waitForStatefulSet(zookeeperStatefulSetName, 3);

        TimeMeasuringSystem.stopOperation(operationID);
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, operationID));
    }

    @Test
    void testRecoveryFromKafkaServiceDeletion() {
        operationID = startTimeMeasuring(Operation.CLUSTER_RECOVERY);
        // kafka cluster already deployed
        String kafkaServiceName = kafkaServiceName(CLUSTER_NAME);
        LOGGER.info("Running deleteKafkaService with cluster {}", CLUSTER_NAME);

        KUBE_CLIENT.deleteByName(SERVICE, kafkaServiceName);

        LOGGER.info("Waiting for creation {}", kafkaServiceName);
        KUBE_CLIENT.waitForResourceCreation(SERVICE, kafkaServiceName);

        TimeMeasuringSystem.stopOperation(operationID);
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, operationID));
    }

    @Test
    void testRecoveryFromZookeeperServiceDeletion() {
        operationID = startTimeMeasuring(Operation.CLUSTER_RECOVERY);
        // kafka cluster already deployed
        String zookeeperServiceName = zookeeperServiceName(CLUSTER_NAME);

        LOGGER.info("Running deleteKafkaService with cluster {}", CLUSTER_NAME);

        KUBE_CLIENT.deleteByName(SERVICE, zookeeperServiceName);

        LOGGER.info("Waiting for creation {}", zookeeperServiceName);
        KUBE_CLIENT.waitForResourceCreation(SERVICE, zookeeperServiceName);

        TimeMeasuringSystem.stopOperation(operationID);
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, operationID));
    }

    @Test
    void testRecoveryFromKafkaHeadlessServiceDeletion() {
        operationID = startTimeMeasuring(Operation.CLUSTER_RECOVERY);
        // kafka cluster already deployed
        String kafkaHeadlessServiceName = kafkaHeadlessServiceName(CLUSTER_NAME);
        LOGGER.info("Running deleteKafkaHeadlessService with cluster {}", CLUSTER_NAME);

        KUBE_CLIENT.deleteByName(SERVICE, kafkaHeadlessServiceName);

        LOGGER.info("Waiting for creation {}", kafkaHeadlessServiceName);
        KUBE_CLIENT.waitForResourceCreation(SERVICE, kafkaHeadlessServiceName);

        TimeMeasuringSystem.stopOperation(operationID);
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, operationID));
    }

    @Test
    void testRecoveryFromZookeeperHeadlessServiceDeletion() {
        operationID = startTimeMeasuring(Operation.CLUSTER_RECOVERY);
        // kafka cluster already deployed
        String zookeeperHeadlessServiceName = zookeeperHeadlessServiceName(CLUSTER_NAME);
        LOGGER.info("Running deleteKafkaHeadlessService with cluster {}", CLUSTER_NAME);

        KUBE_CLIENT.deleteByName(SERVICE, zookeeperHeadlessServiceName);

        LOGGER.info("Waiting for creation {}", zookeeperHeadlessServiceName);
        KUBE_CLIENT.waitForResourceCreation(SERVICE, zookeeperHeadlessServiceName);

        TimeMeasuringSystem.stopOperation(operationID);
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, operationID));
    }

    @Test
    void testRecoveryFromKafkaMetricsConfigDeletion() {
        operationID = startTimeMeasuring(Operation.CLUSTER_RECOVERY);
        // kafka cluster already deployed
        String kafkaMetricsConfigName = kafkaMetricsConfigName(CLUSTER_NAME);
        LOGGER.info("Running deleteKafkaMetricsConfig with cluster {}", CLUSTER_NAME);

        KUBE_CLIENT.deleteByName(CM, kafkaMetricsConfigName);
        KUBE_CLIENT.waitForResourceDeletion(CM, kafkaMetricsConfigName);

        LOGGER.info("Waiting for creation {}", kafkaMetricsConfigName);
        KUBE_CLIENT.waitForResourceCreation(CM, kafkaMetricsConfigName);

        TimeMeasuringSystem.stopOperation(operationID);
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, operationID));
    }

    @Test
    void testRecoveryFromZookeeperMetricsConfigDeletion() {
        operationID = startTimeMeasuring(Operation.TEST_EXECUTION);
        // kafka cluster already deployed
        String zookeeperMetricsConfigName = zookeeperMetricsConfigName(CLUSTER_NAME);
        LOGGER.info("Running deleteZookeeperMetricsConfig with cluster {}", CLUSTER_NAME);

        KUBE_CLIENT.deleteByName(CM, zookeeperMetricsConfigName);
        KUBE_CLIENT.waitForResourceDeletion(CM, zookeeperMetricsConfigName);

        LOGGER.info("Waiting for creation {}", zookeeperMetricsConfigName);
        KUBE_CLIENT.waitForResourceCreation(CM, zookeeperMetricsConfigName);

        TimeMeasuringSystem.stopOperation(operationID);
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, operationID));
    }

    @BeforeAll
    void setupEnvironment() {
        LOGGER.info("Creating resources before the test class");
        prepareEnvForOperator(NAMESPACE);

        createTestClassResources();
        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        testClassResources.clusterOperator(NAMESPACE).done();

        testClassResources.kafkaEphemeral(CLUSTER_NAME, 1).done();
    }
}
