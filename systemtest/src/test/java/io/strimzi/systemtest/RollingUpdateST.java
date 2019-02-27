/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.test.TestUtils;
import io.strimzi.test.extensions.StrimziExtension;
import io.strimzi.test.timemeasuring.Operation;
import io.strimzi.test.timemeasuring.TimeMeasuringSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.hamcrest.MatcherAssert.assertThat;
import static io.strimzi.test.extensions.StrimziExtension.REGRESSION;

@ExtendWith(StrimziExtension.class)
@Tag(REGRESSION)
class RollingUpdateST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(RecoveryST.class);

    static final String NAMESPACE = "rolling-update-cluster-test";
    static final String CLUSTER_NAME = "my-cluster";
    private static final int CO_OPERATION_TIMEOUT = 60000;

    @Test
    void testRecoveryDuringZookeeperRollingUpdate() {
        // @TODO add send-recv messages during this test
        operationID = startTimeMeasuring(Operation.CLUSTER_RECOVERY);

        String firstZkPodName = KafkaResources.zookeeperPodName(CLUSTER_NAME, 0);
        String logZkPattern = "'Exceeded timeout of .* while waiting for Pods resource " + firstZkPodName + "'";

        resources().kafkaEphemeral(CLUSTER_NAME, 3).done();

        LOGGER.info("Update resources for pods");

        resources().kafkaEphemeral(CLUSTER_NAME, 3)
                .editSpec()
                .editZookeeper()
                .withNewResources()
                .withNewRequests()
                .withMilliCpu("100000m")
                .endRequests()
                .endResources()
                .endZookeeper()
                .endSpec()
                .done();

        TestUtils.waitFor("Wait till rolling update of pods start", 2000, CO_OPERATION_TIMEOUT,
            () -> !CLIENT.pods().withName(firstZkPodName).isReady());

        TestUtils.waitFor("Wait till rolling update timedout", 2000, CO_OPERATION_TIMEOUT + 20000,
            () -> !KUBE_CLIENT.searchInLog("deploy", "strimzi-cluster-operator", TimeMeasuringSystem.getCurrentDuration(testClass, testName, operationID), logZkPattern).isEmpty());

        assertThatRollingUpdatedFinished(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        LOGGER.info("Wait till another rolling update starts");
        // Second part
        String rollingUpdateOperation = TimeMeasuringSystem.startOperation(Operation.ROLLING_UPDATE);

        LOGGER.info(TimeMeasuringSystem.getCurrentDuration(testClass, testName, rollingUpdateOperation));

        TestUtils.waitFor("Wait till rolling update timedout", 2000, 180000,
            () -> !KUBE_CLIENT.searchInLog("deploy", "strimzi-cluster-operator", TimeMeasuringSystem.getCurrentDuration(testClass, testName, rollingUpdateOperation), logZkPattern).isEmpty());

        assertThatRollingUpdatedFinished(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        TimeMeasuringSystem.stopOperation(rollingUpdateOperation);
        TimeMeasuringSystem.stopOperation(operationID);
    }

    @Test
    void testRecoveryDuringKafkaRollingUpdate() {
        // @TODO add send-recv messages during this test
        operationID = startTimeMeasuring(Operation.CLUSTER_RECOVERY);

        String firstKafkaPodName = KafkaResources.kafkaPodName(CLUSTER_NAME, 0);
        String logKafkaPattern = "'Exceeded timeout of .* while waiting for Pods resource " + firstKafkaPodName + "'";

        resources().kafkaEphemeral(CLUSTER_NAME, 3).done();

        LOGGER.info("Update resources for pods");

        resources().kafkaEphemeral(CLUSTER_NAME, 3)
                .editSpec()
                .editKafka()
                .withNewResources()
                .withNewRequests()
                .withMilliCpu("100000m")
                .endRequests()
                .endResources()
                .endKafka()
                .endSpec()
                .done();

        TestUtils.waitFor("Wait till rolling update of pods start", 2000, CO_OPERATION_TIMEOUT,
            () -> !CLIENT.pods().withName(firstKafkaPodName).isReady());

        TestUtils.waitFor("Wait till rolling update timeouted", 2000, CO_OPERATION_TIMEOUT + 20000,
            () -> !KUBE_CLIENT.searchInLog("deploy", "strimzi-cluster-operator", TimeMeasuringSystem.getCurrentDuration(testClass, testName, operationID), logKafkaPattern).isEmpty());

        assertThatRollingUpdatedFinished(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));

        LOGGER.info("Wait till another rolling update starts");
        // Second part
        String rollingUpdateOperation = TimeMeasuringSystem.startOperation(Operation.ROLLING_UPDATE);

        TestUtils.waitFor("Wait till rolling update timeouted", 2000, 180000,
            () -> !KUBE_CLIENT.searchInLog("deploy", "strimzi-cluster-operator", TimeMeasuringSystem.getCurrentDuration(testClass, testName, rollingUpdateOperation), logKafkaPattern).isEmpty());

        assertThatRollingUpdatedFinished(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));

        TimeMeasuringSystem.stopOperation(rollingUpdateOperation);
        TimeMeasuringSystem.stopOperation(operationID);
    }

    void assertThatRollingUpdatedFinished(String rolledComponent, String stableComponent) {
        assertThat(rolledComponent + "-1 is not in ready state", CLIENT.pods().withName(rolledComponent + "-1").isReady());
        assertThat(rolledComponent + "-2 is not in ready state", CLIENT.pods().withName(rolledComponent + "-2").isReady());

        assertThat(stableComponent + "-0 is is not in ready state", CLIENT.pods().withName(stableComponent + "-0").isReady());
        assertThat(stableComponent + "-1 is is not in ready state", CLIENT.pods().withName(stableComponent + "-1").isReady());
        assertThat(stableComponent + "-2 is is not in ready state", CLIENT.pods().withName(stableComponent + "-2").isReady());
    }

    @BeforeEach
    void createTestResources() {
        createResources();
    }

    @AfterEach
    void deleteTestResources() throws Exception {
        deleteResources();
        waitForDeletion(TEARDOWN_GLOBAL_WAIT, NAMESPACE);
    }

    @BeforeAll
    void setupEnvironment() {
        LOGGER.info("Creating resources before the test class");
        prepareEnvForOperator(NAMESPACE);

        createTestClassResources();
        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        testClassResources.clusterOperator(NAMESPACE, Integer.toString(CO_OPERATION_TIMEOUT)).done();
    }

    @AfterAll
    void teardownEnvironment() {
        testClassResources.deleteResources();
        teardownEnvForOperator();
    }
}
