/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.systemtest.timemeasuring.Operation;
import io.strimzi.systemtest.timemeasuring.TimeMeasuringSystem;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.annotations.ClusterOperator;
import io.strimzi.test.annotations.Namespace;
import io.strimzi.test.extensions.StrimziExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import static io.strimzi.test.extensions.StrimziExtension.REGRESSION;

@ExtendWith(StrimziExtension.class)
@Namespace(RecoveryST.NAMESPACE)
@ClusterOperator
@Tag(REGRESSION)
class RecoveryST extends AbstractST {

    static final String NAMESPACE = "recovery-cluster-test";
    static final String CLUSTER_NAME = "recovery-cluster";

    private static final Logger LOGGER = LogManager.getLogger(RecoveryST.class);
    private static Resources classResources;

    @Test
    void testRecoveryFromEntityOperatorDeletion() {
        operationID = startTimeMeasuring(Operation.TEST_EXECUTION);
        // kafka cluster already deployed
        String entityOperatorDeploymentName = entityOperatorDeploymentName(CLUSTER_NAME);
        LOGGER.info("Running testRecoveryFromEntityOperatorDeletion with cluster {}", CLUSTER_NAME);

        kubernetes.deleteDeployment(entityOperatorDeploymentName);
        StUtils.waitForDeploymentDeletion(entityOperatorDeploymentName);

        LOGGER.info("Waiting for recovery {}", entityOperatorDeploymentName);
        StUtils.waitForDeploymentReady(entityOperatorDeploymentName);

        //Test that CO doesn't have any exceptions in log
        assertNoErrorLogged();
    }

    @Test
    void testRecoveryFromKafkaStatefulSetDeletion() {
        operationID = startTimeMeasuring(Operation.TEST_EXECUTION);
        // kafka cluster already deployed
        String kafkaStatefulSetName = kafkaClusterName(CLUSTER_NAME);
        LOGGER.info("Running deleteKafkaStatefulSet with cluster {}", CLUSTER_NAME);

        kubernetes.deleteStatefulSet(kafkaStatefulSetName);
        StUtils.waitForStatefulSetDeletion(kafkaStatefulSetName);

        LOGGER.info("Waiting for recovery {}", kafkaStatefulSetName);
        StUtils.waitForStatefulSetReady(kafkaStatefulSetName);

        //Test that CO doesn't have any exceptions in log
        assertNoErrorLogged();
    }

    @Test
    void testRecoveryFromZookeeperStatefulSetDeletion() {
        operationID = startTimeMeasuring(Operation.TEST_EXECUTION);
        // kafka cluster already deployed
        String zookeeperStatefulSetName = zookeeperClusterName(CLUSTER_NAME);
        LOGGER.info("Running deleteZookeeperStatefulSet with cluster {}", CLUSTER_NAME);

        kubernetes.deleteStatefulSet(zookeeperStatefulSetName);
        StUtils.waitForStatefulSetDeletion(zookeeperStatefulSetName);

        LOGGER.info("Waiting for recovery {}", zookeeperStatefulSetName);
        StUtils.waitForStatefulSetReady(zookeeperStatefulSetName);

        //Test that CO doesn't have any exceptions in log
        assertNoErrorLogged();
    }

    @Test
    void testRecoveryFromKafkaServiceDeletion() {
        operationID = startTimeMeasuring(Operation.TEST_EXECUTION);
        // kafka cluster already deployed
        String kafkaServiceName = kafkaServiceName(CLUSTER_NAME);
        LOGGER.info("Running deleteKafkaService with cluster {}", CLUSTER_NAME);

        kubernetes.deleteService(kafkaServiceName);

        LOGGER.info("Waiting for creation {}", kafkaServiceName);
        StUtils.waitForServiceReady(kafkaServiceName);

        //Test that CO doesn't have any exceptions in log
        assertNoErrorLogged();
    }

    @Test
    void testRecoveryFromZookeeperServiceDeletion() {
        operationID = startTimeMeasuring(Operation.TEST_EXECUTION);
        // kafka cluster already deployed
        String zookeeperServiceName = zookeeperServiceName(CLUSTER_NAME);

        LOGGER.info("Running deleteKafkaService with cluster {}", CLUSTER_NAME);

        kubernetes.deleteService(zookeeperServiceName);

        LOGGER.info("Waiting for creation {}", zookeeperServiceName);
        StUtils.waitForServiceReady(zookeeperServiceName);

        //Test that CO doesn't have any exceptions in log
        assertNoErrorLogged();
    }

    @Test
    void testRecoveryFromKafkaHeadlessServiceDeletion() {
        operationID = startTimeMeasuring(Operation.TEST_EXECUTION);
        // kafka cluster already deployed
        String kafkaHeadlessServiceName = kafkaHeadlessServiceName(CLUSTER_NAME);
        LOGGER.info("Running deleteKafkaHeadlessService with cluster {}", CLUSTER_NAME);

        kubernetes.deleteService(kafkaHeadlessServiceName);

        LOGGER.info("Waiting for creation {}", kafkaHeadlessServiceName);
        StUtils.waitForServiceReady(kafkaHeadlessServiceName);

        //Test that CO doesn't have any exceptions in log
        assertNoErrorLogged();
    }

    @Test
    void testRecoveryFromZookeeperHeadlessServiceDeletion() {
        operationID = startTimeMeasuring(Operation.TEST_EXECUTION);
        // kafka cluster already deployed
        String zookeeperHeadlessServiceName = zookeeperHeadlessServiceName(CLUSTER_NAME);
        LOGGER.info("Running deleteKafkaHeadlessService with cluster {}", CLUSTER_NAME);

        kubernetes.deleteService(zookeeperHeadlessServiceName);

        LOGGER.info("Waiting for creation {}", zookeeperHeadlessServiceName);
        StUtils.waitForServiceReady(zookeeperHeadlessServiceName);

        //Test that CO doesn't have any exceptions in log
        assertNoErrorLogged();
    }

    @Test
    void testRecoveryFromKafkaMetricsConfigDeletion() {
        operationID = startTimeMeasuring(Operation.TEST_EXECUTION);
        // kafka cluster already deployed
        String kafkaMetricsConfigName = kafkaMetricsConfigName(CLUSTER_NAME);
        LOGGER.info("Running deleteKafkaMetricsConfig with cluster {}", CLUSTER_NAME);

        kubernetes.deleteConfigMap(kafkaMetricsConfigName);
        StUtils.waitForConfigMapDeletion(kafkaMetricsConfigName);

        LOGGER.info("Waiting for creation {}", kafkaMetricsConfigName);
        StUtils.waitForConfigMapReady(kafkaMetricsConfigName);

        //Test that CO doesn't have any exceptions in log
        assertNoErrorLogged();
    }

    @Test
    void testRecoveryFromZookeeperMetricsConfigDeletion() {
        operationID = startTimeMeasuring(Operation.TEST_EXECUTION);
        // kafka cluster already deployed
        String zookeeperMetricsConfigName = zookeeperMetricsConfigName(CLUSTER_NAME);
        LOGGER.info("Running deleteZookeeperMetricsConfig with cluster {}", CLUSTER_NAME);

        kubernetes.deleteConfigMap(zookeeperMetricsConfigName);
        StUtils.waitForConfigMapDeletion(zookeeperMetricsConfigName);

        LOGGER.info("Waiting for creation {}", zookeeperMetricsConfigName);
        StUtils.waitForConfigMapReady(zookeeperMetricsConfigName);

        //Test that CO doesn't have any exceptions in log
        assertNoErrorLogged();
    }

    @BeforeAll
    static void createClassResources(TestInfo testInfo) {
        LOGGER.info("Creating resources before the test class");
        applyRoleBindings(NAMESPACE, NAMESPACE);
        // 050-Deployment
        testClassResources.clusterOperator(NAMESPACE).done();

        classResources = new Resources();
        classResources().kafkaEphemeral(CLUSTER_NAME, 1).done();
        testClass = testInfo.getTestClass().get().getSimpleName();
    }

    private static Resources classResources() {
        return classResources;
    }

    private void assertNoErrorLogged() {
        TimeMeasuringSystem.stopOperation(operationID);
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, operationID));
    }
}
