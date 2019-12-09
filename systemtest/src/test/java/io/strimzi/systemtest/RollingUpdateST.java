/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.timemeasuring.Operation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
class RollingUpdateST extends BaseST {

    private static final Logger LOGGER = LogManager.getLogger(RecoveryST.class);

    static final String NAMESPACE = "rolling-update-cluster-test";

    @Test
    void testRecoveryDuringZookeeperRollingUpdate() {
        // @TODO add send-recv messages during this test
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY));

        String firstZkPodName = KafkaResources.zookeeperPodName(CLUSTER_NAME, 0);
        String logZkPattern = "'Exceeded timeout of .* while waiting for Pods resource " + firstZkPodName + "'";

        // TODO persistent kafka
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();

        // TODO send messages

        LOGGER.info("Update resources for pods");

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec()
                .getZookeeper()
                .setResources(new ResourceRequirementsBuilder()
                        .addToRequests("cpu", new Quantity("100000m"))
                        .build());
        });

        StUtils.waitForRollingUpdateTimeout(testClass, testName, logZkPattern, timeMeasuringSystem.getOperationID());

        assertThatRollingUpdatedFinished(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        StUtils.waitForReconciliation(testClass, testName, NAMESPACE);

        // Second part
        String rollingUpdateOperation = timeMeasuringSystem.startOperation(Operation.ROLLING_UPDATE);

        StUtils.waitForRollingUpdateTimeout(testClass, testName, logZkPattern, rollingUpdateOperation);

        assertThatRollingUpdatedFinished(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        timeMeasuringSystem.stopOperation(rollingUpdateOperation);
        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
        // TODO receive messages
    }

    @Test
    void testRecoveryDuringKafkaRollingUpdate() {
        // @TODO add send-recv messages during this test
        timeMeasuringSystem.setOperationID(timeMeasuringSystem.startTimeMeasuring(Operation.CLUSTER_RECOVERY));

        String firstKafkaPodName = KafkaResources.kafkaPodName(CLUSTER_NAME, 0);
        String logKafkaPattern = "'Exceeded timeout of .* while waiting for Pods resource " + firstKafkaPodName + "'";

        // TODO persistent kafka
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();

        // TODO send messages

        LOGGER.info("Update resources for pods");

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec()
                .getKafka()
                .setResources(new ResourceRequirementsBuilder()
                        .addToRequests("cpu", new Quantity("100000m"))
                        .build());
        });

        StUtils.waitForRollingUpdateTimeout(testClass, testName, logKafkaPattern, timeMeasuringSystem.getOperationID());

        assertThatRollingUpdatedFinished(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));

        StUtils.waitForReconciliation(testClass, testName, NAMESPACE);

        // Second part
        String rollingUpdateOperation = timeMeasuringSystem.startOperation(Operation.ROLLING_UPDATE);

        StUtils.waitForRollingUpdateTimeout(testClass, testName, logKafkaPattern, rollingUpdateOperation);

        assertThatRollingUpdatedFinished(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));

        timeMeasuringSystem.stopOperation(rollingUpdateOperation);
        timeMeasuringSystem.stopOperation(timeMeasuringSystem.getOperationID());
    }

    void assertThatRollingUpdatedFinished(String rolledComponent, String stableComponent) {
        List<String> podStatuses = kubeClient().listPods().stream()
                .filter(p -> p.getMetadata().getName().startsWith(rolledComponent))
                .map(p -> p.getStatus().getPhase()).sorted().collect(Collectors.toList());

        // TODO update reason
        assertThat(rolledComponent + " is fine", podStatuses.contains("Pending"));

        Map<String, Long> statusCount = podStatuses.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        LOGGER.info("{} pods statutes: {}", rolledComponent, statusCount);

        // TODO remove reason
        assertThat("", statusCount.get("Pending"), is(1L));
        assertThat("", statusCount.get("Running"), is(Integer.toUnsignedLong(podStatuses.size() - 1)));

        podStatuses = kubeClient().listPods().stream()
                .filter(p -> p.getMetadata().getName().startsWith(stableComponent))
                .map(p -> p.getStatus().getPhase()).sorted().collect(Collectors.toList());

        statusCount = podStatuses.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        LOGGER.info("{} pods statutes: {}", stableComponent, statusCount);

        assertThat("", statusCount.get("Running"), is(Integer.toUnsignedLong(podStatuses.size())));
    }

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();
        prepareEnvForOperator(NAMESPACE);

        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        KubernetesResource.clusterOperator(NAMESPACE, Constants.CO_OPERATION_TIMEOUT_SHORT).done();
    }
}
