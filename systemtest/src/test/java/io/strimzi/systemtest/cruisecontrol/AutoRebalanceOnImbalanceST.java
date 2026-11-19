/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.cruisecontrol;

import io.fabric8.kubernetes.api.model.Pod;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceConfigurationBuilder;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceMode;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceState;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.resources.CrdClients;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaRebalanceTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaNodePoolUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaRebalanceUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.ConfigMapUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.CRUISE_CONTROL;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

@Tag(REGRESSION)
@Tag(CRUISE_CONTROL)
@SuiteDoc(
    description = @Desc("This test suite validates the auto-rebalance on imbalance feature, which triggers an automatic " +
        "rebalancing when Cruise Control detects fixable goal violations in the Kafka cluster."),
    beforeTestSteps = {
        @Step(value = "Deploy Cluster Operator with default installation", expected = "Cluster Operator is deployed and running")
    },
    labels = {
        @Label(value = TestDocsLabels.CRUISE_CONTROL),
    }
)
public class AutoRebalanceOnImbalanceST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(AutoRebalanceOnImbalanceST.class);

    // ReplicaDistributionGoal fires when replicas are unevenly distributed — reliably violated after scale-up
    // when new brokers have zero replicas while old brokers carry all the load.
    private static final String ANOMALY_DETECTION_GOALS =
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal";

    // Short names used in KafkaRebalance spec.goals — must be a superset of anomaly detection goals
    private static final String[] TEMPLATE_GOALS = {
        "ReplicaDistributionGoal"
    };

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test the full lifecycle of auto-rebalance on imbalance: Cruise Control detects a goal violation " +
            "after scaling up the cluster, an auto-rebalancing KafkaRebalance CR is automatically created, " +
            "rebalancing completes, and the auto-rebalance state returns to Idle."),
        steps = {
            @Step(value = "Create broker and controller KafkaNodePools with 3 brokers.", expected = "Both KafkaNodePools are successfully created."),
            @Step(value = "Create KafkaRebalance template for imbalance mode with anomaly detection goals.", expected = "KafkaRebalance template is created."),
            @Step(value = "Deploy Kafka cluster with Cruise Control configured for imbalance auto-rebalance and fast anomaly detection.", expected = "Kafka cluster with Cruise Control is deployed."),
            @Step(value = "Ensure the auto-rebalance state is Idle.", expected = "Auto-rebalance state is Idle."),
            @Step(value = "Scale brokers to 5 without add-brokers auto-rebalance — new brokers get no replicas, creating an imbalance.", expected = "Cluster scaled to 5 brokers; brokers 3 and 4 have no replicas."),
            @Step(value = "Cruise Control detects the goal violation and the operator creates the imbalance auto-rebalancing KafkaRebalance CR.", expected = "Auto-rebalance state transitions to RebalanceOnImbalance."),
            @Step(value = "Wait for rebalance to complete.", expected = "Auto-rebalance state returns to Idle and KafkaRebalance CR is deleted.")
        },
        labels = {
            @Label(value = TestDocsLabels.CRUISE_CONTROL),
        }
    )
    void testAutoRebalanceOnImbalanceSuccessful() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String imbalanceTemplateName = testStorage.getClusterName() + "-imbalance-template";
        final int initialReplicas = 3;
        final int scaledReplicas = 5;

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), initialReplicas).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), initialReplicas).build()
        );

        // Template goals must be a superset of the anomaly detection goals configured in CC
        KubeResourceManager.get().createResourceWithoutWait(
            KafkaRebalanceTemplates.kafkaRebalance(testStorage.getNamespaceName(), testStorage.getClusterName())
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_REBALANCE_TEMPLATE, "true")
                    .withName(imbalanceTemplateName)
                .endMetadata()
                .editSpec()
                    .withGoals(TEMPLATE_GOALS)
                .endSpec()
                .build()
        );

        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafkaWithCruiseControlTunedForFastModelGeneration(testStorage.getNamespaceName(), testStorage.getClusterName(), initialReplicas)
                .editSpec()
                    .editCruiseControl()
                        // Short anomaly detection interval for fast test execution
                        .addToConfig("anomaly.detection.goals", ANOMALY_DETECTION_GOALS)
                        .addToConfig("anomaly.detection.interval.ms", "30000")
                        .withAutoRebalance(
                            new KafkaAutoRebalanceConfigurationBuilder()
                                .withMode(KafkaAutoRebalanceMode.IMBALANCE)
                                .withNewTemplate(imbalanceTemplateName)
                                .build())
                    .endCruiseControl()
                .endSpec()
                .build(),
            KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName(), 12, 3).build()
        );

        KafkaUtils.waitUntilKafkaHasAutoRebalanceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaAutoRebalanceState.Idle);

        // Scale up to 5 brokers without ADD_BROKERS mode — new brokers get no replicas,
        // creating an imbalance that CC will detect via MinTopicLeadersPerBrokerGoal
        LOGGER.info("Scaling Kafka brokers from {} to {} to create a replica imbalance", initialReplicas, scaledReplicas);
        KafkaNodePoolUtils.replace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), knp ->
            knp.getSpec().setReplicas(scaledReplicas));

        // CC detects the imbalance — operator creates the auto-rebalancing KR and transitions state
        KafkaUtils.waitUntilKafkaHasExpectedAutoRebalanceMode(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaAutoRebalanceMode.IMBALANCE);
        KafkaUtils.waitUntilKafkaHasAutoRebalanceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaAutoRebalanceState.RebalanceOnImbalance);

        final String imbalanceKrName = KafkaResources.autoRebalancingKafkaRebalanceResourceName(testStorage.getClusterName(), KafkaAutoRebalanceMode.IMBALANCE);
        KafkaRebalanceUtils.waitForKafkaRebalanceIsPresent(testStorage.getNamespaceName(), imbalanceKrName);

        // Wait for rebalance to complete
        KafkaUtils.waitUntilKafkaHasAutoRebalanceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaAutoRebalanceState.Idle);

        KafkaRebalanceUtils.waitForKafkaRebalanceIsDeleted(testStorage.getNamespaceName(), imbalanceKrName);
        ConfigMapUtils.waitForConfigMapIsDeleted(testStorage.getNamespaceName(), imbalanceKrName);

        assertThat(CrdClients.kafkaClient().inNamespace(testStorage.getNamespaceName())
            .withName(testStorage.getClusterName()).get().getStatus().getAutoRebalance().getModes(), is(nullValue()));
    }

    @IsolatedTest
    @TestDoc(
        description = @Desc("Test that auto-rebalance on imbalance does not trigger when the operator is outside the maintenance window, " +
            "and does trigger once the cluster enters the maintenance window."),
        steps = {
            @Step(value = "Create broker and controller KafkaNodePools.", expected = "Both KafkaNodePools are successfully created."),
            @Step(value = "Create KafkaRebalance template for imbalance mode.", expected = "KafkaRebalance template is created."),
            @Step(value = "Deploy Kafka cluster with Cruise Control and imbalance auto-rebalance configured with a future maintenance window.", expected = "Kafka cluster is deployed with maintenance window set to the future."),
            @Step(value = "Scale brokers to 5 to create an imbalance.", expected = "Cluster scaled to 5 brokers; imbalance created."),
            @Step(value = "Verify auto-rebalance does NOT trigger outside the maintenance window.", expected = "State stays Idle, no imbalance KafkaRebalance is created."),
            @Step(value = "Update the maintenance window to the current time.", expected = "Maintenance window is now active."),
            @Step(value = "Verify auto-rebalance triggers within the maintenance window.", expected = "State transitions to RebalanceOnImbalance and then Idle.")
        },
        labels = {
            @Label(value = TestDocsLabels.CRUISE_CONTROL),
        }
    )
    void testAutoRebalanceOnImbalanceMaintenanceWindow() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String imbalanceTemplateName = testStorage.getClusterName() + "-imbalance-template";
        final int initialReplicas = 3;
        final int scaledReplicas = 5;

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), initialReplicas).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), initialReplicas).build()
        );

        KubeResourceManager.get().createResourceWithoutWait(
            KafkaRebalanceTemplates.kafkaRebalance(testStorage.getNamespaceName(), testStorage.getClusterName())
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_REBALANCE_TEMPLATE, "true")
                    .withName(imbalanceTemplateName)
                .endMetadata()
                .editSpec()
                    .withGoals(TEMPLATE_GOALS)
                .endSpec()
                .build()
        );

        // Set maintenance window 2 hours in the future so it will not trigger initially
        LocalDateTime futureWindow = LocalDateTime.now(ZoneOffset.UTC).plusHours(2);
        String futureCron = "* " + futureWindow.getMinute() + " " + futureWindow.getHour() + " * * ? *";

        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafkaWithCruiseControlTunedForFastModelGeneration(testStorage.getNamespaceName(), testStorage.getClusterName(), initialReplicas)
                .editSpec()
                    .addToMaintenanceTimeWindows(futureCron)
                    .editCruiseControl()
                        .addToConfig("anomaly.detection.goals", ANOMALY_DETECTION_GOALS)
                        .addToConfig("anomaly.detection.interval.ms", "30000")
                        .withAutoRebalance(
                            new KafkaAutoRebalanceConfigurationBuilder()
                                .withMode(KafkaAutoRebalanceMode.IMBALANCE)
                                .withNewTemplate(imbalanceTemplateName)
                                .build())
                    .endCruiseControl()
                .endSpec()
                .build(),
            KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName(), 12, 3).build()
        );

        KafkaUtils.waitUntilKafkaHasAutoRebalanceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaAutoRebalanceState.Idle);

        LOGGER.info("Scaling Kafka brokers from {} to {} to create a replica imbalance", initialReplicas, scaledReplicas);
        KafkaNodePoolUtils.replace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), knp ->
            knp.getSpec().setReplicas(scaledReplicas));

        final String imbalanceKrName = KafkaResources.autoRebalancingKafkaRebalanceResourceName(testStorage.getClusterName(), KafkaAutoRebalanceMode.IMBALANCE);

        // Verify auto-rebalance does NOT trigger outside maintenance window (wait 2 detection cycles = 60s)
        LOGGER.info("Verifying auto-rebalance does not trigger outside maintenance window");
        TestUtils.waitFor("state to stay Idle outside maintenance window", TestConstants.GLOBAL_POLL_INTERVAL, 60_000,
            () -> KafkaAutoRebalanceState.Idle.equals(CrdClients.kafkaClient()
                .inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName())
                .get().getStatus().getAutoRebalance().getState()));
        assertThat("No imbalance KR should be created outside maintenance window",
            CrdClients.kafkaRebalanceClient().inNamespace(testStorage.getNamespaceName()).withName(imbalanceKrName).get(),
            is(nullValue()));

        // Update maintenance window to now so auto-rebalance can trigger
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
        String activeCron = "* " + now.getMinute() + "-59 " + now.getHour() + " * * ? *";
        LOGGER.info("Updating maintenance window to current time: {}", activeCron);
        CrdClients.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName())
            .edit(k -> {
                k.getSpec().setMaintenanceTimeWindows(List.of(activeCron));
                return k;
            });

        KafkaUtils.waitUntilKafkaHasAutoRebalanceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaAutoRebalanceState.RebalanceOnImbalance);
        KafkaUtils.waitUntilKafkaHasAutoRebalanceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaAutoRebalanceState.Idle);
        KafkaRebalanceUtils.waitForKafkaRebalanceIsDeleted(testStorage.getNamespaceName(), imbalanceKrName);
    }

    @IsolatedTest
    @TestDoc(
        description = @Desc("Test crash recovery: when the operator is restarted while auto-rebalance on imbalance is in progress " +
            "(RebalanceOnImbalance state), the operator recovers and continues the rebalance to completion."),
        steps = {
            @Step(value = "Create broker and controller KafkaNodePools.", expected = "Both KafkaNodePools are successfully created."),
            @Step(value = "Create KafkaRebalance template for imbalance mode.", expected = "KafkaRebalance template is created."),
            @Step(value = "Deploy Kafka cluster with Cruise Control and imbalance auto-rebalance configured.", expected = "Kafka cluster is deployed."),
            @Step(value = "Scale brokers to 5 to create an imbalance — operator transitions to RebalanceOnImbalance.", expected = "Auto-rebalance state is RebalanceOnImbalance and imbalance KR is created."),
            @Step(value = "Simulate operator crash by deleting the operator pod.", expected = "Operator pod is deleted and restarted by the Deployment."),
            @Step(value = "Wait for the operator to restart.", expected = "Operator pod is running again."),
            @Step(value = "Verify operator recovers and auto-rebalance state is still RebalanceOnImbalance.", expected = "State remains RebalanceOnImbalance after restart."),
            @Step(value = "Wait for the rebalance to complete.", expected = "Auto-rebalance state returns to Idle.")
        },
        labels = {
            @Label(value = TestDocsLabels.CRUISE_CONTROL),
        }
    )
    void testAutoRebalanceOnImbalanceCrashRecovery() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String imbalanceTemplateName = testStorage.getClusterName() + "-imbalance-template";
        final int initialReplicas = 3;
        final int scaledReplicas = 5;

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), initialReplicas).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), initialReplicas).build()
        );

        KubeResourceManager.get().createResourceWithoutWait(
            KafkaRebalanceTemplates.kafkaRebalance(testStorage.getNamespaceName(), testStorage.getClusterName())
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_REBALANCE_TEMPLATE, "true")
                    .withName(imbalanceTemplateName)
                .endMetadata()
                .editSpec()
                    .withGoals(TEMPLATE_GOALS)
                .endSpec()
                .build()
        );

        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafkaWithCruiseControlTunedForFastModelGeneration(testStorage.getNamespaceName(), testStorage.getClusterName(), initialReplicas)
                .editSpec()
                    .editCruiseControl()
                        .addToConfig("anomaly.detection.goals", ANOMALY_DETECTION_GOALS)
                        .addToConfig("anomaly.detection.interval.ms", "30000")
                        .withAutoRebalance(
                            new KafkaAutoRebalanceConfigurationBuilder()
                                .withMode(KafkaAutoRebalanceMode.IMBALANCE)
                                .withNewTemplate(imbalanceTemplateName)
                                .build())
                    .endCruiseControl()
                .endSpec()
                .build(),
            KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName(), 12, 3).build()
        );

        KafkaUtils.waitUntilKafkaHasAutoRebalanceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaAutoRebalanceState.Idle);

        LOGGER.info("Scaling Kafka brokers from {} to {} to create a replica imbalance", initialReplicas, scaledReplicas);
        KafkaNodePoolUtils.replace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), knp ->
            knp.getSpec().setReplicas(scaledReplicas));

        // Wait until imbalance is detected and KR is created
        KafkaUtils.waitUntilKafkaHasAutoRebalanceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaAutoRebalanceState.RebalanceOnImbalance);
        KafkaRebalanceUtils.waitForKafkaRebalanceIsPresent(testStorage.getNamespaceName(),
            KafkaResources.autoRebalancingKafkaRebalanceResourceName(testStorage.getClusterName(), KafkaAutoRebalanceMode.IMBALANCE));

        // Simulate operator crash by deleting the operator pod
        final String operatorNamespace = SetupClusterOperator.getInstance().getOperatorNamespace();
        final String operatorDeploymentName = SetupClusterOperator.getInstance().getOperatorDeploymentName();
        final Map<String, String> operatorSnapshot = DeploymentUtils.depSnapshot(operatorNamespace, operatorDeploymentName);

        LOGGER.info("Simulating operator crash by deleting operator pod");
        List<Pod> operatorPods = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(operatorNamespace, operatorDeploymentName);
        operatorPods.forEach(pod -> KubeResourceManager.get().deleteResourceWithoutWait(pod));

        DeploymentUtils.waitTillDepHasRolled(operatorNamespace, operatorDeploymentName, 1, operatorSnapshot);
        LOGGER.info("Operator restarted successfully");

        // After restart, state should still be RebalanceOnImbalance (crash recovery)
        assertThat("Auto-rebalance state should still be RebalanceOnImbalance after operator restart",
            CrdClients.kafkaClient().inNamespace(testStorage.getNamespaceName())
                .withName(testStorage.getClusterName()).get().getStatus().getAutoRebalance().getState(),
            is(KafkaAutoRebalanceState.RebalanceOnImbalance));

        // Rebalance should complete after recovery
        KafkaUtils.waitUntilKafkaHasAutoRebalanceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaAutoRebalanceState.Idle);
    }

    @ParallelNamespaceTest
    @TestDoc(
        description = @Desc("Test that auto-rebalance on imbalance is not triggered when a scale operation (add-brokers) is already " +
            "in progress. The operator prioritizes the scale-up rebalance over imbalance detection."),
        steps = {
            @Step(value = "Create broker and controller KafkaNodePools with 3 brokers.", expected = "Both KafkaNodePools are successfully created."),
            @Step(value = "Create KafkaRebalance templates for imbalance and add-brokers modes.", expected = "KafkaRebalance templates are created."),
            @Step(value = "Deploy Kafka cluster with Cruise Control configured for both imbalance and add-brokers auto-rebalance.", expected = "Kafka cluster is deployed."),
            @Step(value = "Scale up brokers to 5 to trigger the add-brokers auto-rebalance.", expected = "Auto-rebalance state transitions to RebalanceOnScaleUp."),
            @Step(value = "Verify no imbalance KafkaRebalance CR is created while the scale-up rebalance is in progress.", expected = "No imbalance KR exists; operator ignores CC violation detection during scale operations."),
            @Step(value = "Wait for the scale-up rebalance to complete.", expected = "Auto-rebalance state returns to Idle.")
        },
        labels = {
            @Label(value = TestDocsLabels.CRUISE_CONTROL),
        }
    )
    void testAutoRebalanceOnImbalanceIgnoredDuringScaleOperation() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final String imbalanceTemplateName = testStorage.getClusterName() + "-imbalance-template";
        final String scaleUpTemplateName = testStorage.getClusterName() + "-scale-up-template";
        final int initialReplicas = 3;
        final int scaledReplicas = 5;

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), initialReplicas).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), initialReplicas)
                .editMetadata()
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[100-102]"))
                .endMetadata()
                .build()
        );

        KubeResourceManager.get().createResourceWithoutWait(
            KafkaRebalanceTemplates.kafkaRebalance(testStorage.getNamespaceName(), testStorage.getClusterName())
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_REBALANCE_TEMPLATE, "true")
                    .withName(imbalanceTemplateName)
                .endMetadata()
                .editSpec()
                    .withGoals(TEMPLATE_GOALS)
                .endSpec()
                .build(),
            KafkaRebalanceTemplates.kafkaRebalance(testStorage.getNamespaceName(), testStorage.getClusterName())
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_REBALANCE_TEMPLATE, "true")
                    .withName(scaleUpTemplateName)
                .endMetadata()
                .build()
        );

        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafkaWithCruiseControlTunedForFastModelGeneration(testStorage.getNamespaceName(), testStorage.getClusterName(), initialReplicas)
                .editSpec()
                    .editCruiseControl()
                        .addToConfig("anomaly.detection.goals", ANOMALY_DETECTION_GOALS)
                        .addToConfig("anomaly.detection.interval.ms", "30000")
                        .withAutoRebalance(
                            new KafkaAutoRebalanceConfigurationBuilder()
                                .withMode(KafkaAutoRebalanceMode.IMBALANCE)
                                .withNewTemplate(imbalanceTemplateName)
                                .build(),
                            new KafkaAutoRebalanceConfigurationBuilder()
                                .withMode(KafkaAutoRebalanceMode.ADD_BROKERS)
                                .withNewTemplate(scaleUpTemplateName)
                                .build())
                    .endCruiseControl()
                .endSpec()
                .build(),
            KafkaTopicTemplates.topic(testStorage.getNamespaceName(), testStorage.getTopicName(), testStorage.getClusterName(), 12, 3).build()
        );

        KafkaUtils.waitUntilKafkaHasAutoRebalanceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaAutoRebalanceState.Idle);

        // Scale up — ADD_BROKERS auto-rebalance takes priority; imbalance detection should not trigger
        LOGGER.info("Scaling Kafka brokers from {} to {} to trigger add-brokers auto-rebalance", initialReplicas, scaledReplicas);
        KafkaNodePoolUtils.replace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), knp ->
            knp.getSpec().setReplicas(scaledReplicas));

        KafkaUtils.waitUntilKafkaHasAutoRebalanceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaAutoRebalanceState.RebalanceOnScaleUp);

        final String imbalanceKrName = KafkaResources.autoRebalancingKafkaRebalanceResourceName(testStorage.getClusterName(), KafkaAutoRebalanceMode.IMBALANCE);
        assertThat("Imbalance KafkaRebalance should not exist while scale-up auto-rebalance is in progress",
            CrdClients.kafkaRebalanceClient().inNamespace(testStorage.getNamespaceName()).withName(imbalanceKrName).get(),
            is(nullValue()));

        KafkaUtils.waitUntilKafkaHasAutoRebalanceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaAutoRebalanceState.Idle);
    }

    @BeforeAll
    void setup() {
        SetupClusterOperator.getInstance().withDefaultConfiguration().install();
    }
}
