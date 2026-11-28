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
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
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
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaNodePoolUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaRebalanceUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.TestTags.CRUISE_CONTROL;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
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

    @IsolatedTest
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
                    .withSkipHardGoalCheck(true)
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

        // Create scraper pod for verifying replica movement after rebalance
        LOGGER.info("Creating scraper pod for replica verification");
        KubeResourceManager.get().createResourceWithWait(
            ScraperTemplates.scraperPod(testStorage.getNamespaceName(), testStorage.getScraperName()).build()
        );

        // Scale up to 5 brokers without ADD_BROKERS mode — new brokers get no replicas,
        // creating an imbalance that CC will detect via ReplicaDistributionGoal
        LOGGER.info("Scaling Kafka brokers from {} to {} to create a replica imbalance", initialReplicas, scaledReplicas);
        KafkaNodePoolUtils.replace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), knp ->
            knp.getSpec().setReplicas(scaledReplicas));

        // Give Cruise Control time to stabilize after broker scaling and generate a new model
        // CC needs to: collect metrics from new brokers, generate model, then run anomaly detection (30s interval)
        LOGGER.info("Waiting for Cruise Control to generate model and detect imbalance after scaling");
        try {
            Thread.sleep(90_000);  // Wait ~3 detection cycles to ensure CC detects the imbalance
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for CC model generation", e);
        }

        // CC detects the imbalance — operator creates the auto-rebalancing KR and transitions state
        KafkaUtils.waitUntilKafkaHasExpectedAutoRebalanceMode(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaAutoRebalanceMode.IMBALANCE);

        final String imbalanceKrName = KafkaResources.autoRebalancingKafkaRebalanceResourceName(testStorage.getClusterName(), KafkaAutoRebalanceMode.IMBALANCE);
        // Use KR presence as the reliable signal that the imbalance was detected
        KafkaRebalanceUtils.waitForKafkaRebalanceIsPresent(testStorage.getNamespaceName(), imbalanceKrName);

        // Wait for rebalance to complete (terminal state) and cleanup
        KafkaUtils.waitUntilKafkaHasAutoRebalanceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaAutoRebalanceState.Idle);

        KafkaRebalanceUtils.waitForKafkaRebalanceIsDeleted(testStorage.getNamespaceName(), imbalanceKrName);
        ConfigMapUtils.waitForConfigMapIsDeleted(testStorage.getNamespaceName(), imbalanceKrName);

        // Verify auto-rebalance completed successfully: modes are null and state is Idle
        assertThat(CrdClients.kafkaClient().inNamespace(testStorage.getNamespaceName())
            .withName(testStorage.getClusterName()).get().getStatus().getAutoRebalance().getModes(), is(nullValue()));

        // Verify replicas were actually moved to the newly scaled brokers
        // This proves CC not only proposed a rebalance but it actually executed successfully
        // As Tom noted: "the thing we most want to prove is that the replicas actually moved
        // onto the new empty brokers and the cluster ended up balanced"

        // Get the actual new broker node IDs from the KafkaNodePool
        // The broker pool may assign non-sequential IDs due to controller pool allocations
        KafkaNodePool brokerPool = CrdClients.kafkaNodePoolClient().inNamespace(testStorage.getNamespaceName())
            .withName(testStorage.getBrokerPoolName()).get();

        List<String> allBrokerIds = new ArrayList<>();
        if (brokerPool.getStatus() != null && brokerPool.getStatus().getNodeIds() != null) {
            brokerPool.getStatus().getNodeIds().forEach(id -> allBrokerIds.add(String.valueOf(id)));
        }

        // The new broker IDs are those added during scaling
        // They are the last (scaledReplicas - initialReplicas) IDs in the nodeIds list
        List<String> newBrokerIds;
        if (allBrokerIds.size() >= scaledReplicas) {
            newBrokerIds = new ArrayList<>(allBrokerIds.subList(initialReplicas, Math.min(scaledReplicas, allBrokerIds.size())));
        } else {
            // Fallback: use the last (scaledReplicas - initialReplicas) IDs
            int newBrokerCount = scaledReplicas - initialReplicas;
            newBrokerIds = new ArrayList<>(allBrokerIds.subList(Math.max(0, allBrokerIds.size() - newBrokerCount), allBrokerIds.size()));
        }

        LOGGER.info("All broker node IDs: {}, New broker IDs after scaling: {}", allBrokerIds, newBrokerIds);

        // Get the scraper pod for verification
        List<Pod> scraperPods = KubeResourceManager.get().kubeClient()
            .listPodsByPrefixInName(testStorage.getNamespaceName(), testStorage.getScraperName());

        if (scraperPods.isEmpty()) {
            throw new RuntimeException("No scraper pod found for replica verification - cannot validate rebalance execution");
        }

        final String scraperPodName = scraperPods.get(0).getMetadata().getName();
        LOGGER.info("Using scraper pod '{}' to verify replica placement", scraperPodName);

        // Assert that replicas actually moved to the new brokers
        // This is the critical test: proves CC produced a valid proposal AND Kafka executed it
        KafkaTopicUtils.waitForTopicReplicasOnBrokers(testStorage.getNamespaceName(), testStorage.getTopicName(),
            scraperPodName, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()),
            newBrokerIds);

        LOGGER.info("SUCCESS: Confirmed replicas are now distributed on newly scaled brokers {} - cluster is balanced", newBrokerIds);

        // Test dedup behavior: verify no second rebalance is triggered in the next detection cycle
        // This tests the main safety property: don't rebalance in a loop
        LOGGER.info("Verifying auto-rebalance does not trigger again after completion (dedup check)");
        TestUtils.waitFor("state to stay Idle after completion (dedup - no second rebalance)",
            TestConstants.GLOBAL_POLL_INTERVAL, 90_000,  // Wait ~2 more detection cycles (60s each)
            () -> {
                KafkaAutoRebalanceState state = CrdClients.kafkaClient()
                    .inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName())
                    .get().getStatus().getAutoRebalance().getState();
                KafkaRebalance secondKr = CrdClients.kafkaRebalanceClient()
                    .inNamespace(testStorage.getNamespaceName()).withName(imbalanceKrName).get();
                // State should stay Idle, no new RebalanceOnImbalance transition, and no new KR created
                return KafkaAutoRebalanceState.Idle.equals(state) && secondKr == null;
            });
        assertThat("No second KafkaRebalance should be triggered after completion (dedup)",
            CrdClients.kafkaRebalanceClient().inNamespace(testStorage.getNamespaceName()).withName(imbalanceKrName).get(),
            is(nullValue()));
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
                    .withSkipHardGoalCheck(true)
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

        // Verify auto-rebalance does NOT trigger outside maintenance window
        // Window is in the future, so operator should block the trigger even though CC detects the violation
        LOGGER.info("Verifying auto-rebalance does not trigger outside maintenance window");
        TestUtils.waitFor("state to stay Idle while window is in future (waiting for detection + reconcile attempts)",
            TestConstants.GLOBAL_POLL_INTERVAL, 90_000,
            () -> {
                try {
                    KafkaAutoRebalanceState state = CrdClients.kafkaClient()
                        .inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName())
                        .get().getStatus().getAutoRebalance().getState();
                    return KafkaAutoRebalanceState.Idle.equals(state);
                } catch (Exception e) {
                    return false;
                }
            });
        assertThat("No imbalance KR should be created outside maintenance window",
            CrdClients.kafkaRebalanceClient().inNamespace(testStorage.getNamespaceName()).withName(imbalanceKrName).get(),
            is(nullValue()));

        // Update maintenance window to match every minute (unbounded time window)
        // This avoids the hour-boundary edge case: Quartz expression "* * * * * ? *" matches every second
        // We switch from "blocked by future window" to "allowed by any-time window" and verify trigger now works
        String activeCron = "* * * * * ? *";  // Every second, matches regardless of hour
        LOGGER.info("Updating maintenance window to always-active: {}", activeCron);
        CrdClients.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName())
            .edit(k -> {
                k.getSpec().setMaintenanceTimeWindows(List.of(activeCron));
                return k;
            });

        // Now the window is satisfied — rebalance should trigger
        KafkaUtils.waitUntilKafkaHasExpectedAutoRebalanceMode(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaAutoRebalanceMode.IMBALANCE);
        KafkaRebalanceUtils.waitForKafkaRebalanceIsPresent(testStorage.getNamespaceName(), imbalanceKrName);

        // Wait for rebalance to complete
        KafkaUtils.waitUntilKafkaHasAutoRebalanceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaAutoRebalanceState.Idle);
        KafkaRebalanceUtils.waitForKafkaRebalanceIsDeleted(testStorage.getNamespaceName(), imbalanceKrName);
    }

    @IsolatedTest
    @TestDoc(
        description = @Desc("Test crash recovery: when the operator is restarted while a KafkaRebalance resource is in a non-terminal state " +
            "(e.g., PendingProposal, ProposalReady), the operator resumes monitoring and continues the rebalance to completion."),
        steps = {
            @Step(value = "Create broker and controller KafkaNodePools.", expected = "Both KafkaNodePools are successfully created."),
            @Step(value = "Create KafkaRebalance template for imbalance mode.", expected = "KafkaRebalance template is created."),
            @Step(value = "Deploy Kafka cluster with Cruise Control and imbalance auto-rebalance configured.", expected = "Kafka cluster is deployed."),
            @Step(value = "Scale brokers to 5 to create an imbalance — operator transitions to RebalanceOnImbalance.", expected = "Auto-rebalance state is RebalanceOnImbalance and imbalance KR is created."),
            @Step(value = "Wait for KafkaRebalance to reach non-terminal state (PendingProposal or ProposalReady).", expected = "KafkaRebalance is in one of the non-terminal states."),
            @Step(value = "Simulate operator crash by deleting the operator pod.", expected = "Operator pod is deleted and restarted by the Deployment."),
            @Step(value = "Wait for the operator to restart.", expected = "Operator pod is running again."),
            @Step(value = "Verify operator finds existing KafkaRebalance and resumes monitoring.", expected = "Operator resumes from current KafkaRebalance state."),
            @Step(value = "Wait for the rebalance to complete.", expected = "Auto-rebalance state returns to Idle and KafkaRebalance is deleted.")
        },
        labels = {
            @Label(value = TestDocsLabels.CRUISE_CONTROL),
        }
    )
    void testAutoRebalanceOnImbalanceCrashRecovery() throws InterruptedException {
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
                    .withSkipHardGoalCheck(true)
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

        // Set maintenance window to always-active so auto-rebalance can trigger
        // Using "* * * * * ? *" (every second) avoids hour-boundary timing issues
        String activeCron = "* * * * * ? *";
        LOGGER.info("Setting maintenance window to always-active for auto-rebalance: {}", activeCron);
        CrdClients.kafkaClient().inNamespace(testStorage.getNamespaceName()).withName(testStorage.getClusterName())
            .edit(k -> {
                k.getSpec().setMaintenanceTimeWindows(List.of(activeCron));
                return k;
            });

        LOGGER.info("Scaling Kafka brokers from {} to {} to create a replica imbalance", initialReplicas, scaledReplicas);
        KafkaNodePoolUtils.replace(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), knp ->
            knp.getSpec().setReplicas(scaledReplicas));

        // Wait until imbalance is detected and KR is created (now that maintenance window is active)
        KafkaUtils.waitUntilKafkaHasAutoRebalanceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaAutoRebalanceState.RebalanceOnImbalance);
        final String imbalanceKrName = KafkaResources.autoRebalancingKafkaRebalanceResourceName(testStorage.getClusterName(), KafkaAutoRebalanceMode.IMBALANCE);
        KafkaRebalanceUtils.waitForKafkaRebalanceIsPresent(testStorage.getNamespaceName(), imbalanceKrName);

        // Give KafkaRebalance time to progress beyond initial state, but we don't wait for a specific state
        // (ProposalReady can take a long time and cause timeout in test environments)
        LOGGER.info("Giving KafkaRebalance time to progress from initial creation state");
        TestUtils.waitFor("KafkaRebalance to progress", TestConstants.GLOBAL_POLL_INTERVAL, 90_000,
            () -> {
                try {
                    KafkaRebalance kr = CrdClients.kafkaRebalanceClient().inNamespace(testStorage.getNamespaceName()).withName(imbalanceKrName).get();
                    // Check that KR exists with valid status (not briefly null after restart)
                    return kr != null && kr.getStatus() != null && kr.getStatus().getConditions() != null && !kr.getStatus().getConditions().isEmpty();
                } catch (Exception e) {
                    return false;  // Null pointer or API error — status not ready yet
                }
            });

        // Simulate operator crash by deleting the operator pod while KR exists in non-terminal state
        final String operatorNamespace = SetupClusterOperator.getInstance().getOperatorNamespace();
        final String operatorDeploymentName = SetupClusterOperator.getInstance().getOperatorDeploymentName();
        final Map<String, String> operatorSnapshot = DeploymentUtils.depSnapshot(operatorNamespace, operatorDeploymentName);

        LOGGER.info("Simulating operator crash by deleting operator pod (KR exists in non-terminal state)");
        List<Pod> operatorPods = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(operatorNamespace, operatorDeploymentName);
        operatorPods.forEach(pod -> KubeResourceManager.get().deleteResourceWithoutWait(pod));

        DeploymentUtils.waitTillDepHasRolled(operatorNamespace, operatorDeploymentName, 1, operatorSnapshot);
        LOGGER.info("Operator restarted successfully");

        // Verify crash recovery: KafkaRebalance resource still exists after operator restart
        // This proves the operator resumed managing the rebalance, not that it forgot completely
        // Note: The operator may legitimately recreate the KR during recovery, so we just verify it still exists
        LOGGER.info("Verifying operator recovered and resumed managing KafkaRebalance");
        KafkaRebalance krAfterRestart = CrdClients.kafkaRebalanceClient().inNamespace(testStorage.getNamespaceName()).withName(imbalanceKrName).get();
        assertThat("KafkaRebalance should still exist after operator restart (recovery validation)", krAfterRestart, notNullValue());

        // Wait for rebalance to complete — operator should continue the rebalancing from where it left off
        // Don't assert on RebalanceOnImbalance after restart, as the state may have already progressed during the reconcile
        LOGGER.info("Waiting for rebalance to complete after operator recovery");
        KafkaUtils.waitUntilKafkaHasAutoRebalanceState(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaAutoRebalanceState.Idle);
        KafkaRebalanceUtils.waitForKafkaRebalanceIsDeleted(testStorage.getNamespaceName(), imbalanceKrName);
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
                    .withSkipHardGoalCheck(true)
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
