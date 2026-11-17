/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.common.ConditionBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceConfiguration;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceConfigurationBuilder;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceMode;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceState;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceStatusBrokersBuilder;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.KafkaAutoRebalanceStatusBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceList;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceMode;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.GoalViolationInfo;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.StatusUtils;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.kubernetes.CrdOperator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KafkaAutoRebalancingReconcilerTest {

    private static final String NAMESPACE = "test-ns";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final Reconciliation RECONCILIATION = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME);
    private static final Instant DETECTION_TIME = Instant.now();
    private static final String IMBALANCE_KR_NAME = KafkaResources.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaAutoRebalanceMode.IMBALANCE);
    private static final GoalViolationInfo FIXABLE_VIOLATION = new GoalViolationInfo(
            DETECTION_TIME, GoalViolationInfo.Fixability.FIXABLE, List.of("DiskUsageDistributionGoal"), List.of());

    private ResourceOperatorSupplier supplier;
    private CrdOperator<KubernetesClient, KafkaRebalance, KafkaRebalanceList> rebalanceOperator;
    private KafkaAssemblyOperatorMetricsHolder metrics;

    @BeforeEach
    public void setup() {
        supplier = ResourceUtils.supplierWithMocks(false);
        rebalanceOperator = supplier.kafkaRebalanceOperator;
        metrics = new KafkaAssemblyOperatorMetricsHolder(Kafka.RESOURCE_KIND, null, ResourceUtils.metricsProvider());
    }

    /**
     * When the auto-rebalance state is Idle and Cruise Control detects fixable goal violations,
     * the reconciler creates the auto-rebalancing KafkaRebalance CR and transitions to RebalanceOnImbalance.
     */
    @Test
    public void testAutoRebalanceOnImbalanceSuccessful() {
        when(rebalanceOperator.getAsync(eq(NAMESPACE), eq(IMBALANCE_KR_NAME)))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(rebalanceOperator.createOrUpdate(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(ReconcileResult.created(buildKafkaRebalance(IMBALANCE_KR_NAME, KafkaRebalanceMode.FULL, KafkaRebalanceState.New))));

        Kafka kafka = buildKafkaWithAutoRebalance(KafkaAutoRebalanceState.Idle, KafkaAutoRebalanceMode.IMBALANCE);
        KafkaStatus kafkaStatus = reconcile(buildReconciler(kafka, mockImbalanceDetector(FIXABLE_VIOLATION), Set.of()));

        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnImbalance));
        assertThat(kafkaStatus.getAutoRebalance().getModes(), notNullValue());
        assertThat(kafkaStatus.getAutoRebalance().getModes().get(0).getMode(), is(KafkaAutoRebalanceMode.IMBALANCE));
        assertThat(metrics.autoRebalanceOnImbalanceTriggeredCounter(NAMESPACE, "DiskUsageDistributionGoal").count(), is(1.0));
    }

    /**
     * When the auto-rebalance state is Idle but the auto-rebalancing KafkaRebalance CR already exists
     * (operator crash recovery), the reconciler skips creation and transitions directly to RebalanceOnImbalance
     * without incrementing metrics.
     */
    @Test
    public void testAutoRebalanceOnImbalanceKrAlreadyExistsCrashRecovery() {
        when(rebalanceOperator.getAsync(eq(NAMESPACE), eq(IMBALANCE_KR_NAME)))
                .thenReturn(CompletableFuture.completedFuture(buildKafkaRebalance(IMBALANCE_KR_NAME, KafkaRebalanceMode.FULL, KafkaRebalanceState.Rebalancing)));

        Kafka kafka = buildKafkaWithAutoRebalance(KafkaAutoRebalanceState.Idle, KafkaAutoRebalanceMode.IMBALANCE);
        KafkaStatus kafkaStatus = reconcile(buildReconciler(kafka, mockImbalanceDetector(FIXABLE_VIOLATION), Set.of()));

        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnImbalance));
        assertThat(kafkaStatus.getAutoRebalance().getModes(), notNullValue());
        assertThat(kafkaStatus.getAutoRebalance().getModes().get(0).getMode(), is(KafkaAutoRebalanceMode.IMBALANCE));
        assertThat(metrics.autoRebalanceOnImbalanceTriggeredCounter(NAMESPACE, "DiskUsageDistributionGoal").count(), is(0.0));
    }

    /**
     * When the auto-rebalance state is RebalanceOnImbalance and the auto-rebalancing KafkaRebalance CR is still
     * Rebalancing, the reconciler keeps the state as RebalanceOnImbalance and waits for the rebalance to complete.
     */
    @Test
    public void testAutoRebalanceOnImbalanceInRebalancingState() {
        KafkaRebalance kr = buildKafkaRebalance(IMBALANCE_KR_NAME, KafkaRebalanceMode.FULL, KafkaRebalanceState.Rebalancing);
        when(rebalanceOperator.getAsync(eq(NAMESPACE), eq(kr.getMetadata().getName())))
                .thenReturn(CompletableFuture.completedFuture(kr));

        // Build Kafka with modes pre-set in status to simulate the state after transitioning to RebalanceOnImbalance
        Kafka kafka = new KafkaBuilder(buildKafkaWithAutoRebalance(KafkaAutoRebalanceState.RebalanceOnImbalance, KafkaAutoRebalanceMode.IMBALANCE))
                .editStatus()
                    .editAutoRebalance()
                        .withModes(new KafkaAutoRebalanceStatusBrokersBuilder().withMode(KafkaAutoRebalanceMode.IMBALANCE).build())
                    .endAutoRebalance()
                .endStatus()
                .build();
        KafkaStatus kafkaStatus = reconcile(buildReconciler(kafka, mockImbalanceDetector(null), Set.of()));

        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnImbalance));
        assertThat(kafkaStatus.getAutoRebalance().getModes().get(0).getMode(), is(KafkaAutoRebalanceMode.IMBALANCE));
    }

    /**
     * When the auto-rebalancing KafkaRebalance CR transitions to Ready,
     * the reconciler deletes it and transitions the auto-rebalance state back to Idle.
     */
    @Test
    public void testAutoRebalanceOnImbalanceReady() {
        KafkaRebalance kr = buildKafkaRebalance(IMBALANCE_KR_NAME, KafkaRebalanceMode.FULL, KafkaRebalanceState.Ready);
        when(rebalanceOperator.getAsync(eq(NAMESPACE), eq(kr.getMetadata().getName())))
                .thenReturn(CompletableFuture.completedFuture(kr));
        when(rebalanceOperator.patchAsync(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(kr));
        when(rebalanceOperator.deleteAsync(any(), eq(NAMESPACE), eq(kr.getMetadata().getName()), eq(true)))
                .thenReturn(CompletableFuture.completedFuture(null));

        Kafka kafka = buildKafkaWithAutoRebalance(KafkaAutoRebalanceState.RebalanceOnImbalance, KafkaAutoRebalanceMode.IMBALANCE);
        KafkaStatus kafkaStatus = reconcile(buildReconciler(kafka, mockImbalanceDetector(null), Set.of()));

        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
        verify(rebalanceOperator).deleteAsync(any(), eq(NAMESPACE), eq(kr.getMetadata().getName()), eq(true));
    }

    /**
     * When the auto-rebalancing KafkaRebalance CR transitions to NotReady (e.g. Cruise Control unavailable),
     * the reconciler deletes it and returns the auto-rebalance state to Idle to allow a retry on the next reconciliation.
     */
    @Test
    public void testAutoRebalanceOnImbalanceNotReady() {
        KafkaRebalance kr = buildKafkaRebalance(IMBALANCE_KR_NAME, KafkaRebalanceMode.FULL, KafkaRebalanceState.NotReady);
        when(rebalanceOperator.getAsync(eq(NAMESPACE), eq(kr.getMetadata().getName())))
                .thenReturn(CompletableFuture.completedFuture(kr));
        when(rebalanceOperator.patchAsync(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(kr));
        when(rebalanceOperator.deleteAsync(any(), eq(NAMESPACE), eq(kr.getMetadata().getName()), eq(true)))
                .thenReturn(CompletableFuture.completedFuture(null));

        Kafka kafka = buildKafkaWithAutoRebalance(KafkaAutoRebalanceState.RebalanceOnImbalance, KafkaAutoRebalanceMode.IMBALANCE);
        KafkaStatus kafkaStatus = reconcile(buildReconciler(kafka, mockImbalanceDetector(null), Set.of()));

        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
        verify(rebalanceOperator).deleteAsync(any(), eq(NAMESPACE), eq(kr.getMetadata().getName()), eq(true));
    }

    /**
     * When the auto-rebalancing KafkaRebalance CR is manually stopped by the user,
     * the reconciler deletes it and transitions the auto-rebalance state back to Idle.
     */
    @Test
    public void testAutoRebalanceOnImbalanceStopped() {
        KafkaRebalance kr = buildKafkaRebalance(IMBALANCE_KR_NAME, KafkaRebalanceMode.FULL, KafkaRebalanceState.Stopped);
        when(rebalanceOperator.getAsync(eq(NAMESPACE), eq(kr.getMetadata().getName())))
                .thenReturn(CompletableFuture.completedFuture(kr));
        when(rebalanceOperator.patchAsync(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(kr));
        when(rebalanceOperator.deleteAsync(any(), eq(NAMESPACE), eq(kr.getMetadata().getName()), eq(true)))
                .thenReturn(CompletableFuture.completedFuture(null));

        Kafka kafka = buildKafkaWithAutoRebalance(KafkaAutoRebalanceState.RebalanceOnImbalance, KafkaAutoRebalanceMode.IMBALANCE);
        KafkaStatus kafkaStatus = reconcile(buildReconciler(kafka, mockImbalanceDetector(null), Set.of()));

        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
        verify(rebalanceOperator).deleteAsync(any(), eq(NAMESPACE), eq(kr.getMetadata().getName()), eq(true));
    }

    /**
     * When the auto-rebalance state is RebalanceOnImbalance but the auto-rebalancing KafkaRebalance CR is missing
     * (e.g. deleted externally), the reconciler transitions the auto-rebalance state back to Idle and waits for
     * Cruise Control to re-detect the violation.
     */
    @Test
    public void testAutoRebalanceOnImbalanceKrMissing() {
        when(rebalanceOperator.getAsync(eq(NAMESPACE), eq(IMBALANCE_KR_NAME)))
                .thenReturn(CompletableFuture.completedFuture(null));

        Kafka kafka = buildKafkaWithAutoRebalance(KafkaAutoRebalanceState.RebalanceOnImbalance, KafkaAutoRebalanceMode.IMBALANCE);
        KafkaStatus kafkaStatus = reconcile(buildReconciler(kafka, mockImbalanceDetector(null), Set.of()));

        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
    }

    /**
     * When Cruise Control detects only unfixable goal violations, the auto-rebalance state stays Idle
     * and the reconciler does not create an auto-rebalancing KafkaRebalance CR.
     */
    @Test
    public void testAutoRebalanceOnImbalanceUnfixableViolations() {
        GoalViolationInfo unfixableViolation = new GoalViolationInfo(
                DETECTION_TIME, GoalViolationInfo.Fixability.UNFIXABLE, List.of(), List.of("RackAwareGoal"));
        Kafka kafka = buildKafkaWithAutoRebalance(KafkaAutoRebalanceState.Idle, KafkaAutoRebalanceMode.IMBALANCE);
        KafkaStatus kafkaStatus = reconcile(buildReconciler(kafka, mockImbalanceDetector(unfixableViolation), Set.of()));

        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
        assertThat(kafkaStatus.getAutoRebalance().getModes(), nullValue());
        verify(rebalanceOperator, never()).createOrUpdate(any(), any());
        assertThat(kafkaStatus.getConditions().stream()
                .anyMatch(c -> "AutoRebalanceOnImbalanceFailure".equals(c.getReason())
                        && c.getMessage().equals("Unfixable goal violations detected: [RackAwareGoal]." +
                        " Auto-rebalance on imbalance is blocked until resolved.")), is(true));
    }

    /**
     * When Cruise Control detects both fixable and unfixable goal violations (MIXED), the auto-rebalance
     * state stays Idle, no KafkaRebalance is created, and a warning condition is set.
     */
    @Test
    public void testAutoRebalanceOnImbalanceMixedViolations() {
        GoalViolationInfo mixedViolation = new GoalViolationInfo(
                DETECTION_TIME, GoalViolationInfo.Fixability.MIXED, List.of("DiskUsageDistributionGoal"), List.of("RackAwareGoal"));
        Kafka kafka = buildKafkaWithAutoRebalance(KafkaAutoRebalanceState.Idle, KafkaAutoRebalanceMode.IMBALANCE);
        KafkaStatus kafkaStatus = reconcile(buildReconciler(kafka, mockImbalanceDetector(mixedViolation), Set.of()));

        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
        assertThat(kafkaStatus.getAutoRebalance().getModes(), nullValue());
        verify(rebalanceOperator, never()).createOrUpdate(any(), any());
        assertThat(kafkaStatus.getConditions().stream()
                .anyMatch(c -> "AutoRebalanceOnImbalanceFailure".equals(c.getReason())
                        && c.getMessage().equals("Both fixable and unfixable goal violations detected. Unfixable goals: [RackAwareGoal]." +
                        " Fixable goals: [DiskUsageDistributionGoal]." +
                        " Auto-rebalance on imbalance is blocked until resolved." +
                        " To address fixable violations, create a manual KafkaRebalance with skipHardGoalCheck: true.")), is(true));
    }

    /**
     * When fixable violations are detected but the operator is outside the maintenance window,
     * no KafkaRebalance is created and the state stays Idle.
     */
    @Test
    public void testAutoRebalanceOnImbalanceOutsideMaintenanceWindow() {
        KafkaAutoRebalanceImbalanceDetector det = mockImbalanceDetector(FIXABLE_VIOLATION);
        when(det.isInMaintenanceWindow()).thenReturn(false);

        Kafka kafka = buildKafkaWithAutoRebalance(KafkaAutoRebalanceState.Idle, KafkaAutoRebalanceMode.IMBALANCE);
        KafkaStatus kafkaStatus = reconcile(buildReconciler(kafka, det, Set.of()));

        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
        verify(rebalanceOperator, never()).createOrUpdate(any(), any());
    }

    /**
     * When fixable violations are detected but shouldTriggerRebalance returns false (violation is older
     * than the last rebalance completion), no KafkaRebalance is created and the state stays Idle.
     */
    @Test
    public void testAutoRebalanceOnImbalanceShouldNotTrigger() {
        KafkaAutoRebalanceImbalanceDetector det = mockImbalanceDetector(FIXABLE_VIOLATION);
        when(det.shouldTriggerRebalance(any())).thenReturn(CompletableFuture.completedFuture(false));

        Kafka kafka = buildKafkaWithAutoRebalance(KafkaAutoRebalanceState.Idle, KafkaAutoRebalanceMode.IMBALANCE);
        KafkaStatus kafkaStatus = reconcile(buildReconciler(kafka, det, Set.of()));

        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
        verify(rebalanceOperator, never()).createOrUpdate(any(), any());
    }

    /**
     * When template goal validation fails (the template is missing anomaly detection goals),
     * no KafkaRebalance is created, the state stays Idle, and an InvalidRebalanceTemplateGoals warning
     * condition is set on the Kafka CR status.
     */
    @Test
    public void testAutoRebalanceOnImbalanceTemplateValidationFailure() {
        KafkaAutoRebalanceImbalanceDetector det = mockImbalanceDetector(FIXABLE_VIOLATION);
        when(det.validateTemplateGoals(any())).thenReturn(CompletableFuture.completedFuture(List.of("RackAwareGoal")));

        Kafka kafka = new KafkaBuilder(buildKafkaWithAutoRebalance(KafkaAutoRebalanceState.Idle, KafkaAutoRebalanceMode.IMBALANCE))
                .editSpec()
                    .editCruiseControl()
                        .withAutoRebalance(new KafkaAutoRebalanceConfigurationBuilder()
                                .withMode(KafkaAutoRebalanceMode.IMBALANCE)
                                .withNewTemplate("my-imbalance-template")
                                .build())
                    .endCruiseControl()
                .endSpec()
                .build();
        KafkaStatus kafkaStatus = reconcile(buildReconciler(kafka, det, Set.of()));

        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
        verify(rebalanceOperator, never()).createOrUpdate(any(), any());
        assertThat(kafkaStatus.getConditions().stream()
                .anyMatch(c -> "InvalidRebalanceTemplateGoals".equals(c.getReason())
                        && c.getMessage().equals("Anomaly detection goals [RackAwareGoal] are missing from template 'my-imbalance-template'." +
                        " Add the missing goals to the template.")), is(true));
    }

    /**
     * When a scale-down is detected while an imbalance rebalance is in progress (Rebalancing),
     * the reconciler stops and deletes the imbalance KafkaRebalance CR, then starts the scale-down rebalance.
     * Scale operations take priority over imbalance rebalancing.
     */
    @Test
    public void testAutoRebalanceOnImbalanceInterruptedByScaleDown() {
        KafkaRebalance kr = buildKafkaRebalance(IMBALANCE_KR_NAME, KafkaRebalanceMode.FULL, KafkaRebalanceState.Rebalancing);
        when(rebalanceOperator.getAsync(eq(NAMESPACE), eq(kr.getMetadata().getName())))
                .thenReturn(CompletableFuture.completedFuture(kr));
        when(rebalanceOperator.patchAsync(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(kr));
        when(rebalanceOperator.deleteAsync(any(), eq(NAMESPACE), eq(kr.getMetadata().getName()), eq(true)))
                .thenReturn(CompletableFuture.completedFuture(null));

        String scaleDownKrName = KafkaResources.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaAutoRebalanceMode.REMOVE_BROKERS);
        when(rebalanceOperator.getAsync(eq(NAMESPACE), eq(scaleDownKrName)))
                .thenReturn(CompletableFuture.completedFuture(null));
        KafkaRebalance scaleDownKr = new KafkaRebalanceBuilder().withNewMetadata().withName(scaleDownKrName).endMetadata().build();
        when(rebalanceOperator.createOrUpdate(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(ReconcileResult.created(scaleDownKr)));

        // Kafka needs REMOVE_BROKERS mode configured so the scale-down KR can be created
        Kafka kafkaWithBothModes = buildKafkaWithAutoRebalance(KafkaAutoRebalanceState.RebalanceOnImbalance, KafkaAutoRebalanceMode.IMBALANCE, KafkaAutoRebalanceMode.REMOVE_BROKERS);
        KafkaStatus kafkaStatus = reconcile(buildReconciler(kafkaWithBothModes, mockImbalanceDetector(null), Set.of(3)));

        verify(rebalanceOperator).deleteAsync(any(), eq(NAMESPACE), eq(kr.getMetadata().getName()), eq(true));
        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnScaleDown));
        assertThat(kafkaStatus.getAutoRebalance().getModes(), notNullValue());
        assertThat(kafkaStatus.getAutoRebalance().getModes().get(0).getMode(), is(KafkaAutoRebalanceMode.REMOVE_BROKERS));
    }

    /**
     * When a scale-up is detected while an imbalance rebalance is in progress (Rebalancing),
     * the reconciler stops and deletes the imbalance KafkaRebalance CR, then starts the scale-up rebalance.
     * Scale operations take priority over imbalance rebalancing.
     */
    @Test
    public void testAutoRebalanceOnImbalanceInterruptedByScaleUp() {
        KafkaRebalance kr = buildKafkaRebalance(IMBALANCE_KR_NAME, KafkaRebalanceMode.FULL, KafkaRebalanceState.Rebalancing);
        when(rebalanceOperator.getAsync(eq(NAMESPACE), eq(kr.getMetadata().getName())))
                .thenReturn(CompletableFuture.completedFuture(kr))
                .thenReturn(CompletableFuture.completedFuture(kr));
        when(rebalanceOperator.patchAsync(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(kr));
        when(rebalanceOperator.deleteAsync(any(), eq(NAMESPACE), eq(kr.getMetadata().getName()), eq(true)))
                .thenReturn(CompletableFuture.completedFuture(null));

        String scaleUpKrName = KafkaResources.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaAutoRebalanceMode.ADD_BROKERS);
        when(rebalanceOperator.getAsync(eq(NAMESPACE), eq(scaleUpKrName)))
                .thenReturn(CompletableFuture.completedFuture(null));
        KafkaRebalance scaleUpKr = new KafkaRebalanceBuilder().withNewMetadata().withName(scaleUpKrName).endMetadata().build();
        when(rebalanceOperator.createOrUpdate(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(ReconcileResult.created(scaleUpKr)));

        // The input kafkaStatus has ADD_BROKERS mode — simulates KafkaReconciler detecting a scale-up
        KafkaStatus kafkaStatus = new KafkaStatus();
        kafkaStatus.setAutoRebalance(new KafkaAutoRebalanceStatusBuilder()
                .withModes(new KafkaAutoRebalanceStatusBrokersBuilder()
                        .withMode(KafkaAutoRebalanceMode.ADD_BROKERS)
                        .withBrokers(List.of(5, 6))
                        .build())
                .build());

        Kafka kafkaWithBothModes = buildKafkaWithAutoRebalance(KafkaAutoRebalanceState.RebalanceOnImbalance, KafkaAutoRebalanceMode.IMBALANCE, KafkaAutoRebalanceMode.ADD_BROKERS);
        buildReconciler(kafkaWithBothModes, mockImbalanceDetector(null), Set.of()).reconcile(kafkaStatus).toCompletableFuture().join();

        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnScaleUp));
        assertThat(kafkaStatus.getAutoRebalance().getModes(), notNullValue());
        assertThat(kafkaStatus.getAutoRebalance().getModes().get(0).getMode(), is(KafkaAutoRebalanceMode.ADD_BROKERS));
    }

    /**
     * When a scale operation is already in progress (auto-rebalance state is RebalanceOnScaleUp), imbalance
     * detection is not invoked and the reconciler stays in RebalanceOnScaleUp regardless of any goal violations.
     */
    @Test
    public void testAutoRebalanceOnImbalanceIgnoredDuringScaleOperation() {
        // A scale-up rebalance is in progress (RebalanceOnScaleUp state)
        // CC violations should not be acted on while a scaling operation is running
        KafkaRebalance scaleUpKr = new KafkaRebalanceBuilder()
                .withNewMetadata()
                    .withName(KafkaResources.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaAutoRebalanceMode.ADD_BROKERS))
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec().withMode(KafkaRebalanceMode.ADD_BROKERS).withBrokers(5, 6).endSpec()
                .withNewStatus()
                    .withConditions(new ConditionBuilder().withType(KafkaRebalanceState.Rebalancing.name()).withStatus("True").build())
                .endStatus()
                .build();
        when(rebalanceOperator.getAsync(eq(NAMESPACE), eq(scaleUpKr.getMetadata().getName())))
                .thenReturn(CompletableFuture.completedFuture(scaleUpKr));

        // The input kafkaStatus has ADD_BROKERS mode — scale-up is in progress
        KafkaStatus kafkaStatus = new KafkaStatus();
        kafkaStatus.setAutoRebalance(new KafkaAutoRebalanceStatusBuilder()
                .withModes(new KafkaAutoRebalanceStatusBrokersBuilder()
                        .withMode(KafkaAutoRebalanceMode.ADD_BROKERS)
                        .withBrokers(List.of(5, 6))
                        .build())
                .build());

        // Pre-set modes in Kafka status to simulate the state after transitioning to RebalanceOnScaleUp
        Kafka kafkaWithBothModes = new KafkaBuilder(buildKafkaWithAutoRebalance(KafkaAutoRebalanceState.RebalanceOnScaleUp, KafkaAutoRebalanceMode.IMBALANCE, KafkaAutoRebalanceMode.ADD_BROKERS))
                .editStatus()
                    .editAutoRebalance()
                        .withModes(new KafkaAutoRebalanceStatusBrokersBuilder()
                                .withMode(KafkaAutoRebalanceMode.ADD_BROKERS)
                                .withBrokers(List.of(5, 6))
                                .build())
                    .endAutoRebalance()
                .endStatus()
                .build();
        KafkaAutoRebalanceImbalanceDetector imbalanceDetector = mockImbalanceDetector(FIXABLE_VIOLATION);
        buildReconciler(kafkaWithBothModes, imbalanceDetector, Set.of()).reconcile(kafkaStatus).toCompletableFuture().join();

        // Should remain in RebalanceOnScaleUp — imbalance detection is not invoked during scale operations
        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnScaleUp));
        assertThat(kafkaStatus.getAutoRebalance().getModes().get(0).getMode(), is(KafkaAutoRebalanceMode.ADD_BROKERS));
        verify(imbalanceDetector, never()).checkForGoalViolations();
    }

    /**
     * When a manual KafkaRebalance is already running (detected by Cruise Control), the reconciler
     * stays in Idle auto-rebalance state and does not trigger an additional auto-rebalance on imbalance.
     */
    @Test
    public void testAutoRebalanceOnImbalanceBlockedByActiveManualRebalance() {
        Kafka kafka = buildKafkaWithAutoRebalance(KafkaAutoRebalanceState.Idle, KafkaAutoRebalanceMode.IMBALANCE);
        KafkaAutoRebalanceImbalanceDetector imbalanceDetector = mockImbalanceDetector(null);
        when(imbalanceDetector.isRebalanceRunning()).thenReturn(CompletableFuture.completedFuture(true));
        KafkaStatus kafkaStatus = reconcile(buildReconciler(kafka, imbalanceDetector, Set.of()));

        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
        assertThat(kafkaStatus.getAutoRebalance().getModes(), nullValue());
        verify(imbalanceDetector, never()).checkForGoalViolations();
        verify(rebalanceOperator, never()).createOrUpdate(any(), any());
    }

    private KafkaStatus reconcile(KafkaAutoRebalancingReconciler reconciler) {
        KafkaStatus kafkaStatus = new KafkaStatus();
        reconciler.reconcile(kafkaStatus).toCompletableFuture().join();
        return kafkaStatus;
    }

    private KafkaAutoRebalancingReconciler buildReconciler(Kafka kafka, KafkaAutoRebalanceImbalanceDetector mockDetector,
                                                           Set<Integer> scalingDownBlockedNodes) {
        return new KafkaAutoRebalancingReconciler(RECONCILIATION, kafka, supplier, scalingDownBlockedNodes, metrics) {
            @Override
            KafkaAutoRebalanceImbalanceDetector createImbalanceDetector(ResourceOperatorSupplier supplier) {
                return mockDetector;
            }
        };
    }

    private KafkaAutoRebalanceImbalanceDetector mockImbalanceDetector(GoalViolationInfo violation) {
        KafkaAutoRebalanceImbalanceDetector imbalanceDetector = mock(KafkaAutoRebalanceImbalanceDetector.class);
        when(imbalanceDetector.isRebalanceRunning()).thenReturn(CompletableFuture.completedFuture(false));
        when(imbalanceDetector.checkForGoalViolations()).thenReturn(CompletableFuture.completedFuture(violation));
        when(imbalanceDetector.shouldTriggerRebalance(any())).thenReturn(CompletableFuture.completedFuture(true));
        when(imbalanceDetector.isInMaintenanceWindow()).thenReturn(true);
        when(imbalanceDetector.validateTemplateGoals(any())).thenReturn(CompletableFuture.completedFuture(List.of()));
        return imbalanceDetector;
    }

    private Kafka buildKafkaWithAutoRebalance(KafkaAutoRebalanceState autoRebalanceState, KafkaAutoRebalanceMode... modes) {
        List<KafkaAutoRebalanceConfiguration> configs = Arrays.stream(modes)
                .map(m -> new KafkaAutoRebalanceConfigurationBuilder().withMode(m).build())
                .toList();
        KafkaBuilder builder = new KafkaBuilder()
                .withNewMetadata().withName(CLUSTER_NAME).withNamespace(NAMESPACE).endMetadata()
                .withNewSpec()
                    .withNewCruiseControl()
                        .withAutoRebalance(configs)
                    .endCruiseControl()
                .endSpec();
        builder.withNewStatus()
                .withAutoRebalance(new KafkaAutoRebalanceStatusBuilder()
                        .withState(autoRebalanceState)
                        .withLastTransitionTime(StatusUtils.iso8601Now())
                        .build())
                .endStatus();
        return builder.build();
    }

    private KafkaRebalance buildKafkaRebalance(String name, KafkaRebalanceMode mode, KafkaRebalanceState state) {
        return new KafkaRebalanceBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withMode(mode)
                .endSpec()
                .withNewStatus()
                    .withConditions(new ConditionBuilder().withType(state.name()).withStatus("True").build())
                .endStatus()
                .build();
    }
}
