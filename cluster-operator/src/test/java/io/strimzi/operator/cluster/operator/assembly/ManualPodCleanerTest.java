/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.StrimziPodSetBuilder;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.StatefulSetOperator;
import io.strimzi.operator.common.operator.resource.StrimziPodSetOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.AbstractScalableNamespacedResourceOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.vertx.core.Future;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class ManualPodCleanerTest {
    private final static String CONTROLLER_NAME = "controller";
    private final static Labels SELECTOR = Labels.fromString("selector=" + CONTROLLER_NAME);
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();

    @Test
    public void testManualPodCleanupOnePodWithSts(VertxTestContext context) {
        manualPodCleanupOnePod(context, false);
    }

    @Test
    public void testManualPodCleanupOnePodWithPodSets(VertxTestContext context) {
        manualPodCleanupOnePod(context, true);
    }

    private void manualPodCleanupOnePod(VertxTestContext context, boolean useStrimziPodSets)    {
        List<Pod> pods = List.of(
                podWithName(CONTROLLER_NAME + "-0"),
                podWithNameAndAnnotations(CONTROLLER_NAME + "-1", Collections.singletonMap(AbstractScalableNamespacedResourceOperator.ANNO_STRIMZI_IO_DELETE_POD_AND_PVC, "true")),
                podWithName(CONTROLLER_NAME + "-2")
        );

        List<PersistentVolumeClaim> pvcs = List.of(
                pvcWithName("data-" + CONTROLLER_NAME + "-0"),
                pvcWithName("data-" + CONTROLLER_NAME + "-1"),
                pvcWithName("data-" + CONTROLLER_NAME + "-2")
        );

        manualPodCleanup(context, useStrimziPodSets, pods, pvcs, List.of(CONTROLLER_NAME + "-1"), List.of("data-" + CONTROLLER_NAME + "-1"));
    }

    @Test
    public void testManualPodCleanupJbodWithSts(VertxTestContext context) {
        manualPodCleanupJbod(context, false);
    }

    @Test
    public void testManualPodCleanupJbodWithPodSets(VertxTestContext context) {
        manualPodCleanupJbod(context, true);
    }

    private void manualPodCleanupJbod(VertxTestContext context, boolean useStrimziPodSets)    {
        List<Pod> pods = List.of(
                podWithName(CONTROLLER_NAME + "-0"),
                podWithNameAndAnnotations(CONTROLLER_NAME + "-1", Collections.singletonMap(AbstractScalableNamespacedResourceOperator.ANNO_STRIMZI_IO_DELETE_POD_AND_PVC, "true")),
                podWithName(CONTROLLER_NAME + "-2")
        );

        List<PersistentVolumeClaim> pvcs = List.of(
                pvcWithName("data-0-" + CONTROLLER_NAME + "-0"),
                pvcWithName("data-0-" + CONTROLLER_NAME + "-1"),
                pvcWithName("data-0-" + CONTROLLER_NAME + "-2"),
                pvcWithName("data-1-" + CONTROLLER_NAME + "-0"),
                pvcWithName("data-1-" + CONTROLLER_NAME + "-1"),
                pvcWithName("data-1-" + CONTROLLER_NAME + "-2")
        );

        manualPodCleanup(context, useStrimziPodSets, pods, pvcs, List.of(CONTROLLER_NAME + "-1"), List.of("data-0-" + CONTROLLER_NAME + "-1", "data-1-" + CONTROLLER_NAME + "-1"));
    }

    @Test
    public void testManualPodCleanupMultiplePodsWithSts(VertxTestContext context) {
        manualPodCleanupMultiplePods(context, false);
    }

    @Test
    public void testManualPodCleanupMultiplePodsWithPodSets(VertxTestContext context) {
        manualPodCleanupMultiplePods(context, true);
    }

    private void manualPodCleanupMultiplePods(VertxTestContext context, boolean useStrimziPodSets)    {
        List<Pod> pods = List.of(
                podWithName(CONTROLLER_NAME + "-0"),
                podWithNameAndAnnotations(CONTROLLER_NAME + "-1", Collections.singletonMap(AbstractScalableNamespacedResourceOperator.ANNO_STRIMZI_IO_DELETE_POD_AND_PVC, "true")),
                podWithNameAndAnnotations(CONTROLLER_NAME + "-2", Collections.singletonMap(AbstractScalableNamespacedResourceOperator.ANNO_STRIMZI_IO_DELETE_POD_AND_PVC, "true"))
        );

        List<PersistentVolumeClaim> pvcs = List.of(
                pvcWithName("data-0-" + CONTROLLER_NAME + "-0"),
                pvcWithName("data-0-" + CONTROLLER_NAME + "-1"),
                pvcWithName("data-0-" + CONTROLLER_NAME + "-2"),
                pvcWithName("data-1-" + CONTROLLER_NAME + "-0"),
                pvcWithName("data-1-" + CONTROLLER_NAME + "-1"),
                pvcWithName("data-1-" + CONTROLLER_NAME + "-2")
        );

        manualPodCleanup(context, useStrimziPodSets, pods, pvcs, List.of(CONTROLLER_NAME + "-1"), List.of("data-0-" + CONTROLLER_NAME + "-1", "data-1-" + CONTROLLER_NAME + "-1"));
    }

    @Test
    public void testManualPodCleanupNoPodsWithSts(VertxTestContext context) {
        manualPodCleanupNoPods(context, false);
    }

    @Test
    public void testManualPodCleanupNoPodsWithPodSets(VertxTestContext context) {
        manualPodCleanupNoPods(context, true);
    }

    private void manualPodCleanupNoPods(VertxTestContext context, boolean useStrimziPodSets)    {
        List<Pod> pods = List.of(
                podWithName(CONTROLLER_NAME + "-0"),
                podWithName(CONTROLLER_NAME + "-1"),
                podWithName(CONTROLLER_NAME + "-2")
        );

        List<PersistentVolumeClaim> pvcs = List.of(
                pvcWithName("data-0-" + CONTROLLER_NAME + "-0"),
                pvcWithName("data-0-" + CONTROLLER_NAME + "-1"),
                pvcWithName("data-0-" + CONTROLLER_NAME + "-2"),
                pvcWithName("data-1-" + CONTROLLER_NAME + "-0"),
                pvcWithName("data-1-" + CONTROLLER_NAME + "-1"),
                pvcWithName("data-1-" + CONTROLLER_NAME + "-2")
        );

        manualPodCleanup(context, useStrimziPodSets, pods, pvcs, List.of(), List.of());
    }

    private void manualPodCleanup(VertxTestContext context, boolean useStrimziPodSets, List<Pod> pods, List<PersistentVolumeClaim> pvcs, List<String> podsToBeDeleted, List<String> pvcsToBeDeleted) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        StatefulSetOperator mockStsOps = supplier.stsOperations;

        ArgumentCaptor<StatefulSet> stsReconciliationCaptor = ArgumentCaptor.forClass(StatefulSet.class);
        ArgumentCaptor<StrimziPodSet> podSetReconciliationCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);

        if (useStrimziPodSets) {
            when(mockPodSetOps.getAsync(any(), eq(CONTROLLER_NAME))).thenReturn(Future.succeededFuture(new StrimziPodSetBuilder().withNewMetadata().withName(CONTROLLER_NAME).endMetadata().withNewSpec().withPods(PodSetUtils.podsToMaps(pods)).endSpec().build()));
            when(mockPodSetOps.reconcile(any(), any(), eq(CONTROLLER_NAME), podSetReconciliationCaptor.capture())).thenReturn(Future.succeededFuture());
        } else {
            when(mockStsOps.getAsync(any(), eq(CONTROLLER_NAME))).thenReturn(Future.succeededFuture(new StatefulSetBuilder().withNewMetadata().withName(CONTROLLER_NAME).endMetadata().build()));
            when(mockStsOps.deleteAsync(any(), any(), eq(CONTROLLER_NAME), anyBoolean())).thenReturn(Future.succeededFuture());
            when(mockStsOps.reconcile(any(), any(), eq(CONTROLLER_NAME), stsReconciliationCaptor.capture())).thenReturn(Future.succeededFuture());
        }

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), eq(SELECTOR))).thenReturn(Future.succeededFuture(pods));
        ArgumentCaptor<String> podDeletionCaptor = ArgumentCaptor.forClass(String.class);
        when(mockPodOps.deleteAsync(any(), any(), podDeletionCaptor.capture(), anyBoolean())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), any(), any(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        PvcOperator mockPvcOps = supplier.pvcOperations;
        when(mockPvcOps.listAsync(any(), eq(SELECTOR))).thenReturn(Future.succeededFuture(pvcs));
        ArgumentCaptor<String> pvcDeletionCaptor = ArgumentCaptor.forClass(String.class);
        when(mockPvcOps.deleteAsync(any(), any(), pvcDeletionCaptor.capture(), anyBoolean())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<String> pvcReconciliationCaptor = ArgumentCaptor.forClass(String.class);
        when(mockPvcOps.reconcile(any(), any(), pvcReconciliationCaptor.capture(), any())).thenReturn(Future.succeededFuture());

        ClusterOperatorConfig config;
        if (useStrimziPodSets) {
            config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS, ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS);
        } else {
            config = ResourceUtils.dummyClusterOperatorConfig(VERSIONS, ClusterOperatorConfig.DEFAULT_OPERATION_TIMEOUT_MS, "-UseStrimziPodSets");
        }

        ManualPodCleaner cleaner = new ManualPodCleaner(
                Reconciliation.DUMMY_RECONCILIATION,
                CONTROLLER_NAME,
                SELECTOR,
                config,
                supplier
        );

        Checkpoint async = context.checkpoint();
        cleaner.maybeManualPodCleaning(pvcs)
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    if (useStrimziPodSets) {
                        if (podsToBeDeleted.size() > 0) {
                            // PodSet was reconciled twice => once teo remove the deleted pod and once to add it back
                            assertThat(podSetReconciliationCaptor.getAllValues().size(), is(2));
                            assertThat(podSetReconciliationCaptor.getAllValues().get(0).getSpec().getPods().size(), is(2));
                            assertThat(podSetReconciliationCaptor.getAllValues().get(1).getSpec().getPods().size(), is(3));
                        } else {
                            assertThat(podSetReconciliationCaptor.getAllValues().size(), is(0));
                        }
                    } else {
                        if (podsToBeDeleted.size() > 0) {
                            // StatefulSet was deleted once and reconciled once to recreate it
                            verify(mockStsOps, times(1)).deleteAsync(any(), any(), eq(CONTROLLER_NAME), anyBoolean());
                            assertThat(stsReconciliationCaptor.getAllValues().size(), is(1));
                            assertThat(stsReconciliationCaptor.getValue(), is(notNullValue()));
                        } else {
                            verify(mockStsOps, never()).deleteAsync(any(), any(), eq(CONTROLLER_NAME), anyBoolean());
                            assertThat(stsReconciliationCaptor.getAllValues().size(), is(0));
                        }
                    }

                    // Verify the deleted pod
                    assertThat(podDeletionCaptor.getAllValues().size(), is(podsToBeDeleted.size()));
                    assertThat(podDeletionCaptor.getAllValues(), is(podsToBeDeleted));

                    // Verify the deleted and recreated pvc
                    assertThat(pvcDeletionCaptor.getAllValues().size(), is(pvcsToBeDeleted.size()));
                    assertThat(pvcDeletionCaptor.getAllValues(), is(pvcsToBeDeleted));
                    assertThat(pvcReconciliationCaptor.getAllValues().size(), is(pvcsToBeDeleted.size()));
                    assertThat(pvcReconciliationCaptor.getAllValues(), is(pvcsToBeDeleted));

                    async.flag();
                })));
    }

    // Internal utility methods
    private Pod podWithName(String name) {
        return podWithNameAndAnnotations(name, Collections.emptyMap());
    }

    private Pod podWithNameAndAnnotations(String name, Map<String, String> annotations) {
        return new PodBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withAnnotations(annotations)
                .endMetadata()
                .build();
    }

    private PersistentVolumeClaim pvcWithName(String name) {
        return new PersistentVolumeClaimBuilder()
                .withNewMetadata()
                    .withName(name)
                .endMetadata()
                .build();
    }
}
