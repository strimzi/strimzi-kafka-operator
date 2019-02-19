package io.strimzi.systemtest;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.utils.StUtils;
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

import java.util.Map;

import static io.strimzi.test.extensions.StrimziExtension.REGRESSION;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(StrimziExtension.class)
@Tag(REGRESSION)
public class RollingUpdateST extends AbstractST {

    static final String NAMESPACE = "rolling-update-cluster-test";
    static final String CLUSTER_NAME = "my-cluster";

    private static final Logger LOGGER = LogManager.getLogger(RecoveryST.class);


    @Test
    void testRecoveryDuringRollingUpdate() {
        operationID = startTimeMeasuring(Operation.CLUSTER_RECOVERY);

        String firstZkPodName = KafkaResources.zookeeperPodName(CLUSTER_NAME, 0);
        String logPattern = "Exceeded timeout of .* while waiting for Pods resource " + firstZkPodName;

        resources().kafkaEphemeral(CLUSTER_NAME, 3).done();

//        String zkSsName = KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME);
//        Map<String, String> zkPods = StUtils.ssSnapshot(CLIENT, NAMESPACE, zkSsName);

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

        TestUtils.waitFor("Wait till rolling update of pods start", 2000, 60000, () -> !CLIENT.pods().withName(firstZkPodName).isReady());

        TestUtils.waitFor("Wait till rolling update timeouted", 2000, 70000,
                () -> !KUBE_CLIENT.searchInLog("deploy", "strimzi-cluster-operator", TimeMeasuringSystem.getCurrentDuration(testClass, testName, operationID), logPattern).isEmpty());

//
//        try {
//            Thread.sleep(70000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//
//        String zkPodName = KafkaResources.zookeeperStatefulSetName("my-cluster") + "-1";
//
//        String zkPodResources = CLIENT.pods().withName(zkPodName).get().getMetadata().getResourceVersion();
//
//        assertEquals(zkPods.get(zkPodName), zkPodResources);
//
        TimeMeasuringSystem.stopOperation(operationID);
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
        testClassResources.clusterOperator(NAMESPACE, "60000").done();
    }

    @AfterAll
    void teardownEnvironment() {
        testClassResources.deleteResources();
        teardownEnvForOperator();
    }
}
