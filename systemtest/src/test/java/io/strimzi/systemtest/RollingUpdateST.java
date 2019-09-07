/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.timemeasuring.Operation;
import io.strimzi.test.timemeasuring.TimeMeasuringSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
class RollingUpdateST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(RecoveryST.class);

    static final String NAMESPACE = "rolling-update-cluster-test";
    private static final String RECONCILIATION_PATTERN = "'Triggering periodic reconciliation for namespace " + NAMESPACE + "'";

    @Test
    void testRecoveryDuringZookeeperRollingUpdate() {
        // @TODO add send-recv messages during this test
        setOperationID(startTimeMeasuring(Operation.CLUSTER_RECOVERY));

        String firstZkPodName = KafkaResources.zookeeperPodName(CLUSTER_NAME, 0);
        String logZkPattern = "'Exceeded timeout of .* while waiting for Pods resource " + firstZkPodName + "'";

        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3).done();

        LOGGER.info("Update resources for pods");

        replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec()
                .getZookeeper()
                .setResources(new ResourceRequirementsBuilder()
                        .addToRequests("cpu", new Quantity("100000m"))
                        .build());
        });

        TestUtils.waitFor("Wait till rolling update timeout", Constants.CO_OPERATION_TIMEOUT_POLL, Constants.CO_OPERATION_TIMEOUT_WAIT,
            () -> !cmdKubeClient().searchInLog("deploy", "strimzi-cluster-operator", TimeMeasuringSystem.getCurrentDuration(testClass, testName, getOperationID()), logZkPattern).isEmpty());

        assertThatRollingUpdatedFinished(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        StUtils.waitForReconciliation(testClass, testName, NAMESPACE);

        // Second part
        String rollingUpdateOperation = TimeMeasuringSystem.startOperation(Operation.ROLLING_UPDATE);

        TestUtils.waitFor("Wait till rolling update timeout", Constants.CO_OPERATION_TIMEOUT_POLL, Constants.CO_OPERATION_TIMEOUT_WAIT,
            () -> !cmdKubeClient().searchInLog("deploy", "strimzi-cluster-operator", TimeMeasuringSystem.getCurrentDuration(testClass, testName, rollingUpdateOperation), logZkPattern).isEmpty());

        assertThatRollingUpdatedFinished(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        TimeMeasuringSystem.stopOperation(rollingUpdateOperation);
        TimeMeasuringSystem.stopOperation(getOperationID());
    }

    @Test
    void testRecoveryDuringKafkaRollingUpdate() {
        // @TODO add send-recv messages during this test
        setOperationID(startTimeMeasuring(Operation.CLUSTER_RECOVERY));

        String firstKafkaPodName = KafkaResources.kafkaPodName(CLUSTER_NAME, 0);
        String logKafkaPattern = "'Exceeded timeout of .* while waiting for Pods resource " + firstKafkaPodName + "'";

        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 3).done();

        LOGGER.info("Update resources for pods");

        replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec()
                .getKafka()
                .setResources(new ResourceRequirementsBuilder()
                        .addToRequests("cpu", new Quantity("100000m"))
                        .build());
        });

        TestUtils.waitFor("Wait till rolling update timeouted", Constants.CO_OPERATION_TIMEOUT_POLL, Constants.CO_OPERATION_TIMEOUT_WAIT,
            () -> !cmdKubeClient().searchInLog("deploy", "strimzi-cluster-operator", TimeMeasuringSystem.getCurrentDuration(testClass, testName, getOperationID()), logKafkaPattern).isEmpty());

        assertThatRollingUpdatedFinished(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));

        StUtils.waitForReconciliation(testClass, testName, NAMESPACE);

        // Second part
        String rollingUpdateOperation = TimeMeasuringSystem.startOperation(Operation.ROLLING_UPDATE);

        TestUtils.waitFor("Wait till rolling update timedout", Constants.CO_OPERATION_TIMEOUT_POLL, Constants.CO_OPERATION_TIMEOUT_WAIT,
            () -> !cmdKubeClient().searchInLog("deploy", "strimzi-cluster-operator", TimeMeasuringSystem.getCurrentDuration(testClass, testName, rollingUpdateOperation), logKafkaPattern).isEmpty());

        assertThatRollingUpdatedFinished(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));

        TimeMeasuringSystem.stopOperation(rollingUpdateOperation);
        TimeMeasuringSystem.stopOperation(getOperationID());
    }

    void assertThatRollingUpdatedFinished(String rolledComponent, String stableComponent) {
        List<String> podStatuses = kubeClient().listPods().stream()
                .filter(p -> p.getMetadata().getName().startsWith(rolledComponent))
                .map(p -> p.getStatus().getPhase()).sorted().collect(Collectors.toList());

        assertThat(rolledComponent + "is fine", podStatuses.contains("Pending"));

        Map<String, Long> statusCount = podStatuses.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        LOGGER.info("{} pods statutes: {}", rolledComponent, statusCount);

        assertThat("", statusCount.get("Pending"), is(1L));
        assertThat("", statusCount.get("Running"), is(Integer.toUnsignedLong(podStatuses.size() - 1)));

        podStatuses = kubeClient().listPods().stream()
                .filter(p -> p.getMetadata().getName().startsWith(stableComponent))
                .map(p -> p.getStatus().getPhase()).sorted().collect(Collectors.toList());

        statusCount = podStatuses.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        LOGGER.info("{} pods statutes: {}", stableComponent, statusCount);

        assertThat("", statusCount.get("Running"), is(Integer.toUnsignedLong(podStatuses.size())));
    }

    @BeforeEach
    void createTestResources() {
        createTestMethodResources();
    }

    @BeforeAll
    void setupEnvironment() {
        LOGGER.info("Creating resources before the test class");
        prepareEnvForOperator(NAMESPACE);

        createTestClassResources();
        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        testClassResources().clusterOperator(NAMESPACE, Constants.CO_OPERATION_TIMEOUT_SHORT).done();
    }

    @Override
    protected void tearDownEnvironmentAfterEach() throws Exception {
        deleteTestMethodResources();
        waitForDeletion(Constants.TIMEOUT_TEARDOWN);
    }
}
