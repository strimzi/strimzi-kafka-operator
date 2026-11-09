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
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KafkaAutoRebalancingReconcilerTest {

    private static final String NAMESPACE = "test-ns";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final Reconciliation RECONCILIATION = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME);
    private static final Instant DETECTION_TIME = Instant.now();

    private ResourceOperatorSupplier supplier;
    private CrdOperator<KubernetesClient, KafkaRebalance, KafkaRebalanceList> rebalanceOperator;

    @BeforeEach
    public void setup() {
        supplier = ResourceUtils.supplierWithMocks(false);
        rebalanceOperator = supplier.kafkaRebalanceOperator;
    }

    @Test
    public void testAutoRebalanceOnImbalanceSuccessful() {
        String krName = KafkaResources.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaAutoRebalanceMode.IMBALANCE);
        when(rebalanceOperator.getAsync(eq(NAMESPACE), eq(krName)))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(rebalanceOperator.createOrUpdate(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(ReconcileResult.created(buildAutoImbalanceKr(KafkaRebalanceState.New))));

        Kafka kafka = buildKafkaWithState(KafkaAutoRebalanceState.Idle);
        KafkaStatus kafkaStatus = reconcile(buildReconciler(kafka, fixableViolationsDetector(kafka), Set.of()));

        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnImbalance));
        assertThat(kafkaStatus.getAutoRebalance().getModes(), notNullValue());
        assertThat(kafkaStatus.getAutoRebalance().getModes().get(0).getMode(), is(KafkaAutoRebalanceMode.IMBALANCE));
    }

    @Test
    public void testAutoRebalanceOnImbalanceKrAlreadyExistsCrashRecovery() {
        String krName = KafkaResources.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaAutoRebalanceMode.IMBALANCE);
        when(rebalanceOperator.getAsync(eq(NAMESPACE), eq(krName)))
                .thenReturn(CompletableFuture.completedFuture(buildAutoImbalanceKr(KafkaRebalanceState.Rebalancing)));

        Kafka kafka = buildKafkaWithState(KafkaAutoRebalanceState.Idle);
        KafkaStatus kafkaStatus = reconcile(buildReconciler(kafka, fixableViolationsDetector(kafka), Set.of()));

        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnImbalance));
    }

    @Test
    public void testAutoRebalanceOnImbalanceInRebalancingState() {
        KafkaRebalance kr = buildAutoImbalanceKr(KafkaRebalanceState.Rebalancing);
        when(rebalanceOperator.getAsync(eq(NAMESPACE), eq(kr.getMetadata().getName())))
                .thenReturn(CompletableFuture.completedFuture(kr));

        Kafka kafka = buildKafkaWithState(KafkaAutoRebalanceState.RebalanceOnImbalance);
        KafkaStatus kafkaStatus = reconcile(buildReconciler(kafka, noViolationsDetector(kafka), Set.of()));

        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnImbalance));
    }

    @Test
    public void testAutoRebalanceOnImbalanceReady() {
        KafkaRebalance kr = buildAutoImbalanceKr(KafkaRebalanceState.Ready);
        when(rebalanceOperator.getAsync(eq(NAMESPACE), eq(kr.getMetadata().getName())))
                .thenReturn(CompletableFuture.completedFuture(kr));
        when(rebalanceOperator.patchAsync(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(kr));
        when(rebalanceOperator.deleteAsync(any(), eq(NAMESPACE), eq(kr.getMetadata().getName()), eq(true)))
                .thenReturn(CompletableFuture.completedFuture(null));

        Kafka kafka = buildKafkaWithState(KafkaAutoRebalanceState.RebalanceOnImbalance);
        KafkaStatus kafkaStatus = reconcile(buildReconciler(kafka, noViolationsDetector(kafka), Set.of()));

        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
        verify(rebalanceOperator).deleteAsync(any(), eq(NAMESPACE), eq(kr.getMetadata().getName()), eq(true));
    }

    @Test
    public void testAutoRebalanceOnImbalanceNotReady() {
        KafkaRebalance kr = buildAutoImbalanceKr(KafkaRebalanceState.NotReady);
        when(rebalanceOperator.getAsync(eq(NAMESPACE), eq(kr.getMetadata().getName())))
                .thenReturn(CompletableFuture.completedFuture(kr));
        when(rebalanceOperator.patchAsync(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(kr));
        when(rebalanceOperator.deleteAsync(any(), eq(NAMESPACE), eq(kr.getMetadata().getName()), eq(true)))
                .thenReturn(CompletableFuture.completedFuture(null));

        Kafka kafka = buildKafkaWithState(KafkaAutoRebalanceState.RebalanceOnImbalance);
        KafkaStatus kafkaStatus = reconcile(buildReconciler(kafka, noViolationsDetector(kafka), Set.of()));

        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
    }

    @Test
    public void testAutoRebalanceOnImbalanceStopped() {
        KafkaRebalance kr = buildAutoImbalanceKr(KafkaRebalanceState.Stopped);
        when(rebalanceOperator.getAsync(eq(NAMESPACE), eq(kr.getMetadata().getName())))
                .thenReturn(CompletableFuture.completedFuture(kr));
        when(rebalanceOperator.patchAsync(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(kr));
        when(rebalanceOperator.deleteAsync(any(), eq(NAMESPACE), eq(kr.getMetadata().getName()), eq(true)))
                .thenReturn(CompletableFuture.completedFuture(null));

        Kafka kafka = buildKafkaWithState(KafkaAutoRebalanceState.RebalanceOnImbalance);
        KafkaStatus kafkaStatus = reconcile(buildReconciler(kafka, noViolationsDetector(kafka), Set.of()));

        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
        verify(rebalanceOperator).deleteAsync(any(), eq(NAMESPACE), eq(kr.getMetadata().getName()), eq(true));
    }

    @Test
    public void testAutoRebalanceOnImbalanceKrMissing() {
        String krName = KafkaResources.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaAutoRebalanceMode.IMBALANCE);
        when(rebalanceOperator.getAsync(eq(NAMESPACE), eq(krName)))
                .thenReturn(CompletableFuture.completedFuture(null));

        Kafka kafka = buildKafkaWithState(KafkaAutoRebalanceState.RebalanceOnImbalance);
        KafkaStatus kafkaStatus = reconcile(buildReconciler(kafka, noViolationsDetector(kafka), Set.of()));

        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
    }

    @Test
    public void testAutoRebalanceOnImbalanceUnfixableViolations() {
        String krName = KafkaResources.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaAutoRebalanceMode.IMBALANCE);
        when(rebalanceOperator.getAsync(eq(NAMESPACE), eq(krName)))
                .thenReturn(CompletableFuture.completedFuture(null));

        Kafka kafka = buildKafkaWithState(KafkaAutoRebalanceState.Idle);
        KafkaStatus kafkaStatus = reconcile(buildReconciler(kafka, unfixableViolationsDetector(kafka), Set.of()));

        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
        assertThat(kafkaStatus.getAutoRebalance().getModes(), nullValue());
    }

    @Test
    public void testAutoRebalanceOnImbalanceInterruptedByScaleDown() {
        KafkaRebalance kr = buildAutoImbalanceKr(KafkaRebalanceState.Rebalancing);
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
        Kafka kafkaWithBothModes = new KafkaBuilder(buildKafkaWithState(KafkaAutoRebalanceState.RebalanceOnImbalance))
                .editSpec()
                    .editCruiseControl()
                        .withAutoRebalance(
                                new KafkaAutoRebalanceConfigurationBuilder().withMode(KafkaAutoRebalanceMode.IMBALANCE).build(),
                                new KafkaAutoRebalanceConfigurationBuilder().withMode(KafkaAutoRebalanceMode.REMOVE_BROKERS).build())
                    .endCruiseControl()
                .endSpec()
                .build();

        KafkaStatus kafkaStatus = reconcile(buildReconciler(kafkaWithBothModes, noViolationsDetector(kafkaWithBothModes), Set.of(3)));

        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnScaleDown));
    }

    @Test
    public void testAutoRebalanceOnImbalanceInterruptedByScaleUp() {
        KafkaRebalance kr = buildAutoImbalanceKr(KafkaRebalanceState.Rebalancing);
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

        Kafka kafkaWithBothModes = new KafkaBuilder(buildKafkaWithState(KafkaAutoRebalanceState.RebalanceOnImbalance))
                .editSpec()
                    .editCruiseControl()
                        .withAutoRebalance(
                                new KafkaAutoRebalanceConfigurationBuilder().withMode(KafkaAutoRebalanceMode.IMBALANCE).build(),
                                new KafkaAutoRebalanceConfigurationBuilder().withMode(KafkaAutoRebalanceMode.ADD_BROKERS).build())
                    .endCruiseControl()
                .endSpec()
                .build();

        buildReconciler(kafkaWithBothModes, noViolationsDetector(kafkaWithBothModes), Set.of()).reconcile(kafkaStatus).toCompletableFuture().join();

        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnScaleUp));
    }

    @Test
    public void testAutoRebalanceOnImbalanceReadyThenScaleDown() {
        KafkaRebalance kr = buildAutoImbalanceKr(KafkaRebalanceState.Ready);
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

        Kafka kafkaWithBothModes = new KafkaBuilder(buildKafkaWithState(KafkaAutoRebalanceState.RebalanceOnImbalance))
                .editSpec()
                    .editCruiseControl()
                        .withAutoRebalance(
                                new KafkaAutoRebalanceConfigurationBuilder().withMode(KafkaAutoRebalanceMode.IMBALANCE).build(),
                                new KafkaAutoRebalanceConfigurationBuilder().withMode(KafkaAutoRebalanceMode.REMOVE_BROKERS).build())
                    .endCruiseControl()
                .endSpec()
                .build();

        // scalingDownBlockedNodes = {3, 4} simulates a scale-down happening while imbalance rebalance completed
        KafkaStatus kafkaStatus = reconcile(buildReconciler(kafkaWithBothModes, noViolationsDetector(kafkaWithBothModes), Set.of(3, 4)));

        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnScaleDown));
    }

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

        Kafka kafkaWithBothModes = new KafkaBuilder(buildKafkaWithState(KafkaAutoRebalanceState.RebalanceOnScaleUp))
                .editSpec()
                    .editCruiseControl()
                        .withAutoRebalance(
                                new KafkaAutoRebalanceConfigurationBuilder().withMode(KafkaAutoRebalanceMode.IMBALANCE).build(),
                                new KafkaAutoRebalanceConfigurationBuilder().withMode(KafkaAutoRebalanceMode.ADD_BROKERS).build())
                    .endCruiseControl()
                .endSpec()
                .build();

        buildReconciler(kafkaWithBothModes, fixableViolationsDetector(kafkaWithBothModes), Set.of()).reconcile(kafkaStatus).toCompletableFuture().join();

        // Should remain in RebalanceOnScaleUp — imbalance detection is not invoked during scale operations
        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.RebalanceOnScaleUp));
    }

    @Test
    public void testAutoRebalanceOnImbalanceBlockedByActiveManualRebalance() {
        Kafka kafka = buildKafkaWithState(KafkaAutoRebalanceState.Idle);
        KafkaStatus kafkaStatus = reconcile(buildReconciler(kafka, activeRebalanceDetector(kafka), Set.of()));

        assertThat(kafkaStatus.getAutoRebalance().getState(), is(KafkaAutoRebalanceState.Idle));
    }

    private KafkaStatus reconcile(KafkaAutoRebalancingReconciler reconciler) {
        KafkaStatus kafkaStatus = new KafkaStatus();
        reconciler.reconcile(kafkaStatus).toCompletableFuture().join();
        return kafkaStatus;
    }

    private KafkaAutoRebalancingReconciler buildReconciler(Kafka kafka, KafkaAutoRebalanceImbalanceDetector mockDetector,
                                                           Set<Integer> scalingDownBlockedNodes) {
        KafkaAssemblyOperatorMetricsHolder metrics = new KafkaAssemblyOperatorMetricsHolder(
                Kafka.RESOURCE_KIND, null, ResourceUtils.metricsProvider());
        return new KafkaAutoRebalancingReconciler(RECONCILIATION, kafka, supplier, scalingDownBlockedNodes, metrics) {
            @Override
            KafkaAutoRebalanceImbalanceDetector createImbalanceDetector(ResourceOperatorSupplier supplier) {
                return mockDetector;
            }
        };
    }

    private KafkaAutoRebalanceImbalanceDetector fixableViolationsDetector(Kafka kafka) {
        return new KafkaAutoRebalanceImbalanceDetector(RECONCILIATION, kafka, supplier) {
            @Override
            public CompletableFuture<GoalViolationInfo> checkForGoalViolations() {
                return CompletableFuture.completedFuture(
                        new GoalViolationInfo(DETECTION_TIME, GoalViolationInfo.Fixability.FIXABLE,
                                List.of("DiskUsageDistributionGoal"), List.of()));
            }
            @Override
            public CompletableFuture<Boolean> shouldTriggerRebalance(Instant detectionTime) {
                return CompletableFuture.completedFuture(true);
            }
            @Override
            public boolean isInMaintenanceWindow() {
                return true;
            }
            @Override
            public CompletableFuture<Boolean> isRebalanceRunning() {
                return CompletableFuture.completedFuture(false);
            }
            @Override
            public CompletableFuture<List<String>> validateTemplateGoals(String templateName) {
                return CompletableFuture.completedFuture(List.of());
            }
        };
    }

    private KafkaAutoRebalanceImbalanceDetector noViolationsDetector(Kafka kafka) {
        return new KafkaAutoRebalanceImbalanceDetector(RECONCILIATION, kafka, supplier) {
            @Override
            public CompletableFuture<GoalViolationInfo> checkForGoalViolations() {
                return CompletableFuture.completedFuture(null);
            }
            @Override
            public CompletableFuture<Boolean> isRebalanceRunning() {
                return CompletableFuture.completedFuture(false);
            }
            @Override
            public CompletableFuture<List<String>> validateTemplateGoals(String templateName) {
                return CompletableFuture.completedFuture(List.of());
            }
        };
    }

    private KafkaAutoRebalanceImbalanceDetector unfixableViolationsDetector(Kafka kafka) {
        return new KafkaAutoRebalanceImbalanceDetector(RECONCILIATION, kafka, supplier) {
            @Override
            public CompletableFuture<GoalViolationInfo> checkForGoalViolations() {
                return CompletableFuture.completedFuture(
                        new GoalViolationInfo(DETECTION_TIME, GoalViolationInfo.Fixability.UNFIXABLE,
                                List.of(), List.of("RackAwareGoal")));
            }
            @Override
            public CompletableFuture<Boolean> shouldTriggerRebalance(Instant detectionTime) {
                return CompletableFuture.completedFuture(true);
            }
            @Override
            public boolean isInMaintenanceWindow() {
                return true;
            }
            @Override
            public CompletableFuture<Boolean> isRebalanceRunning() {
                return CompletableFuture.completedFuture(false);
            }
            @Override
            public CompletableFuture<List<String>> validateTemplateGoals(String templateName) {
                return CompletableFuture.completedFuture(List.of());
            }
        };
    }

    private KafkaAutoRebalanceImbalanceDetector activeRebalanceDetector(Kafka kafka) {
        return new KafkaAutoRebalanceImbalanceDetector(RECONCILIATION, kafka, supplier) {
            @Override
            public CompletableFuture<Boolean> isRebalanceRunning() {
                return CompletableFuture.completedFuture(true);
            }
            @Override
            public CompletableFuture<List<String>> validateTemplateGoals(String templateName) {
                return CompletableFuture.completedFuture(List.of());
            }
        };
    }

    private Kafka buildKafkaWithState(KafkaAutoRebalanceState state) {
        KafkaBuilder builder = new KafkaBuilder()
                .withNewMetadata().withName(CLUSTER_NAME).withNamespace(NAMESPACE).endMetadata()
                .withNewSpec()
                    .withNewCruiseControl()
                        .withAutoRebalance(
                                new KafkaAutoRebalanceConfigurationBuilder()
                                        .withMode(KafkaAutoRebalanceMode.IMBALANCE)
                                        .build())
                    .endCruiseControl()
                .endSpec();
        builder.withNewStatus()
                .withAutoRebalance(new KafkaAutoRebalanceStatusBuilder()
                        .withState(state)
                        .withLastTransitionTime(StatusUtils.iso8601Now())
                        .build())
                .endStatus();
        return builder.build();
    }

    private KafkaRebalance buildAutoImbalanceKr(KafkaRebalanceState state) {
        return new KafkaRebalanceBuilder()
                .withNewMetadata()
                    .withName(KafkaResources.autoRebalancingKafkaRebalanceResourceName(CLUSTER_NAME, KafkaAutoRebalanceMode.IMBALANCE))
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withMode(KafkaRebalanceMode.FULL)
                .endSpec()
                .withNewStatus()
                    .withConditions(new ConditionBuilder().withType(state.name()).withStatus("True").build())
                .endStatus()
                .build();
    }
}
