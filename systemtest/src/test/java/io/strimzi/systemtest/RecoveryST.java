/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.timemeasuring.Operation;
import io.strimzi.test.timemeasuring.TimeMeasuringSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.REGRESSION;

@Tag(REGRESSION)
class RecoveryST extends AbstractST {

    static final String NAMESPACE = "recovery-cluster-test";
    static final String CLUSTER_NAME = "recovery-cluster";

    private static final Logger LOGGER = LogManager.getLogger(RecoveryST.class);

    @Test
    void testRecoveryFromEntityOperatorDeletion() {
        setOperationID(startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        // kafka cluster already deployed
        LOGGER.info("Running testRecoveryFromEntityOperatorDeletion with cluster {}", CLUSTER_NAME);
        String entityOperatorDeploymentName = KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME);
        String entityOperatorDeploymentUid = kubeClient().getDeploymentUid(entityOperatorDeploymentName);
        kubeClient().deleteDeployment(entityOperatorDeploymentName);

        LOGGER.info("Waiting for recovery {}", entityOperatorDeploymentName);
        StUtils.waitForDeploymentRecovery(entityOperatorDeploymentName, entityOperatorDeploymentUid);
        StUtils.waitForDeploymentReady(entityOperatorDeploymentName, 1);

        TimeMeasuringSystem.stopOperation(getOperationID());
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, getOperationID()));
    }

    @Test
    @Tag(ACCEPTANCE)
    void testRecoveryFromKafkaStatefulSetDeletion() {
        setOperationID(startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaStatefulSet with cluster {}", CLUSTER_NAME);
        String kafkaStatefulSetName = KafkaResources.kafkaStatefulSetName(CLUSTER_NAME);
        String kafkaStatefulSetUid = kubeClient().getStatefulSetUid(kafkaStatefulSetName);
        kubeClient().deleteStatefulSet(kafkaStatefulSetName);

        LOGGER.info("Waiting for recovery {}", kafkaStatefulSetName);
        StUtils.waitForStatefulSetRecovery(kafkaStatefulSetName, kafkaStatefulSetUid);
        StUtils.waitForAllStatefulSetPodsReady(kafkaStatefulSetName, 3);

        TimeMeasuringSystem.stopOperation(getOperationID());
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, getOperationID()));
    }

    @Test
    @Tag(ACCEPTANCE)
    void testRecoveryFromZookeeperStatefulSetDeletion() {
        setOperationID(startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        // kafka cluster already deployed
        LOGGER.info("Running deleteZookeeperStatefulSet with cluster {}", CLUSTER_NAME);
        String zookeeperStatefulSetName = KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME);
        String zookeeperStatefulSetUid = kubeClient().getStatefulSetUid(zookeeperStatefulSetName);
        kubeClient().deleteStatefulSet(zookeeperStatefulSetName);

        LOGGER.info("Waiting for recovery {}", zookeeperStatefulSetName);
        StUtils.waitForStatefulSetRecovery(zookeeperStatefulSetName, zookeeperStatefulSetUid);
        StUtils.waitForAllStatefulSetPodsReady(zookeeperStatefulSetName, 1);

        TimeMeasuringSystem.stopOperation(getOperationID());
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, getOperationID()));
    }

    @Test
    void testRecoveryFromKafkaServiceDeletion() {
        setOperationID(startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaService with cluster {}", CLUSTER_NAME);
        String kafkaServiceName = KafkaResources.bootstrapServiceName(CLUSTER_NAME);
        String kafkaServiceUid = kubeClient().getServiceUid(kafkaServiceName);
        kubeClient().deleteService(kafkaServiceName);

        LOGGER.info("Waiting for creation {}", kafkaServiceName);
        StUtils.waitForServiceRecovery(kafkaServiceName, kafkaServiceUid);

        TimeMeasuringSystem.stopOperation(getOperationID());
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, getOperationID()));
    }

    @Test
    void testRecoveryFromZookeeperServiceDeletion() {
        setOperationID(startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaService with cluster {}", CLUSTER_NAME);
        String zookeeperServiceName = zookeeperServiceName(CLUSTER_NAME);
        String zookeeperServiceUid = kubeClient().getServiceUid(zookeeperServiceName);
        kubeClient().deleteService(zookeeperServiceName);

        LOGGER.info("Waiting for creation {}", zookeeperServiceName);
        StUtils.waitForServiceRecovery(zookeeperServiceName, zookeeperServiceUid);

        TimeMeasuringSystem.stopOperation(getOperationID());
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, getOperationID()));
    }

    @Test
    void testRecoveryFromKafkaHeadlessServiceDeletion() {
        setOperationID(startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaHeadlessService with cluster {}", CLUSTER_NAME);
        String kafkaHeadlessServiceName = KafkaResources.brokersServiceName(CLUSTER_NAME);
        String kafkaHeadlessServiceUid = kubeClient().getServiceUid(kafkaHeadlessServiceName);
        kubeClient().deleteService(kafkaHeadlessServiceName);

        LOGGER.info("Waiting for creation {}", kafkaHeadlessServiceName);
        StUtils.waitForServiceRecovery(kafkaHeadlessServiceName, kafkaHeadlessServiceUid);

        TimeMeasuringSystem.stopOperation(getOperationID());
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, getOperationID()));
    }

    @Test
    void testRecoveryFromZookeeperHeadlessServiceDeletion() {
        setOperationID(startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaHeadlessService with cluster {}", CLUSTER_NAME);
        String zookeeperHeadlessServiceName = zookeeperHeadlessServiceName(CLUSTER_NAME);
        String zookeeperHeadlessServiceUid = kubeClient().getServiceUid(zookeeperHeadlessServiceName);
        kubeClient().deleteService(zookeeperHeadlessServiceName);

        LOGGER.info("Waiting for creation {}", zookeeperHeadlessServiceName);
        StUtils.waitForServiceRecovery(zookeeperHeadlessServiceName, zookeeperHeadlessServiceUid);

        TimeMeasuringSystem.stopOperation(getOperationID());
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, getOperationID()));
    }

    @Test
    void testRecoveryFromKafkaMetricsConfigDeletion() {
        setOperationID(startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        // kafka cluster already deployed
        LOGGER.info("Running deleteKafkaMetricsConfig with cluster {}", CLUSTER_NAME);
        String kafkaMetricsConfigName = KafkaResources.kafkaMetricsAndLogConfigMapName(CLUSTER_NAME);
        String kafkaMetricsConfigUid = kubeClient().getConfigMapUid(kafkaMetricsConfigName);
        kubeClient().deleteConfigMap(kafkaMetricsConfigName);

        LOGGER.info("Waiting for creation {}", kafkaMetricsConfigName);
        StUtils.waitForConfigMapRecovery(kafkaMetricsConfigName, kafkaMetricsConfigUid);

        TimeMeasuringSystem.stopOperation(getOperationID());
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, getOperationID()));
    }

    @Test
    void testRecoveryFromZookeeperMetricsConfigDeletion() {
        setOperationID(startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        LOGGER.info("Running deleteZookeeperMetricsConfig with cluster {}", CLUSTER_NAME);
        // kafka cluster already deployed
        String zookeeperMetricsConfigName = KafkaResources.zookeeperMetricsAndLogConfigMapName(CLUSTER_NAME);
        String zookeeperMetricsConfigUid = kubeClient().getConfigMapUid(zookeeperMetricsConfigName);
        kubeClient().deleteConfigMap(zookeeperMetricsConfigName);

        LOGGER.info("Waiting for creation {}", zookeeperMetricsConfigName);
        StUtils.waitForConfigMapRecovery(zookeeperMetricsConfigName, zookeeperMetricsConfigUid);

        TimeMeasuringSystem.stopOperation(getOperationID());
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, getOperationID()));
    }

    @Test
    @Tag(BRIDGE)
    void testRecoveryFromKafkaBridgeDeploymentDeletion() {
        setOperationID(startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        LOGGER.info("Running deleteKafkaBridgeDeployment with cluster {}", CLUSTER_NAME);
        // kafka cluster already deployed
        String kafkaBridgeDeploymentName = KafkaBridgeResources.deploymentName(CLUSTER_NAME);
        String kafkaBridgeDeploymentUid = kubeClient().getDeploymentUid(kafkaBridgeDeploymentName);
        kubeClient().deleteDeployment(kafkaBridgeDeploymentName);

        LOGGER.info("Waiting for deployment {} recovery", kafkaBridgeDeploymentName);
        StUtils.waitForDeploymentRecovery(kafkaBridgeDeploymentName, kafkaBridgeDeploymentUid);

        TimeMeasuringSystem.stopOperation(getOperationID());
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, getOperationID()));
    }

    @Test
    @Tag(BRIDGE)
    void testRecoveryFromKafkaBridgeServiceDeletion() {
        setOperationID(startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        LOGGER.info("Running deleteKafkaBridgeService with cluster {}", CLUSTER_NAME);
        String kafkaBridgeServiceName = KafkaBridgeResources.serviceName(CLUSTER_NAME);
        String kafkaBridgeServiceUid = kubeClient().namespace(NAMESPACE).getServiceUid(kafkaBridgeServiceName);
        kubeClient().deleteService(kafkaBridgeServiceName);

        LOGGER.info("Waiting for service {} recovery", kafkaBridgeServiceName);
        StUtils.waitForServiceRecovery(kafkaBridgeServiceName, kafkaBridgeServiceUid);

        TimeMeasuringSystem.stopOperation(getOperationID());
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, getOperationID()));
    }

    @Test
    @Tag(BRIDGE)
    void testRecoveryFromKafkaBridgeMetricsConfigDeletion() {
        setOperationID(startTimeMeasuring(Operation.CLUSTER_RECOVERY));
        LOGGER.info("Running deleteKafkaBridgeMetricsConfig with cluster {}", CLUSTER_NAME);
        String kafkaBridgeMetricsConfigName = KafkaBridgeResources.metricsAndLogConfigMapName(CLUSTER_NAME);
        String kafkaBridgeMetricsConfigUid = kubeClient().getConfigMapUid(kafkaBridgeMetricsConfigName);
        kubeClient().deleteConfigMap(kafkaBridgeMetricsConfigName);

        LOGGER.info("Waiting for metric config {} re-creation", kafkaBridgeMetricsConfigName);
        StUtils.waitForConfigMapRecovery(kafkaBridgeMetricsConfigName, kafkaBridgeMetricsConfigUid);

        TimeMeasuringSystem.stopOperation(getOperationID());
        //Test that CO doesn't have any exceptions in log
        assertNoCoErrorsLogged(TimeMeasuringSystem.getDurationInSecconds(testClass, testName, getOperationID()));
    }

    @BeforeAll
    void setupEnvironment() {
        LOGGER.info("Creating resources before the test class");
        prepareEnvForOperator(NAMESPACE);

        createTestClassResources();
        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        testClassResources().clusterOperator(NAMESPACE).done();

        deployTestSpecificResources();
    }

    void deployTestSpecificResources() {
        testClassResources().kafkaEphemeral(CLUSTER_NAME, 3, 1).done();
        testClassResources().kafkaBridge(CLUSTER_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME), 1, Constants.HTTP_BRIDGE_DEFAULT_PORT).done();
    }

    @Override
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        super.recreateTestEnv(coNamespace, bindingsNamespaces);
        deployTestSpecificResources();
    }
}
