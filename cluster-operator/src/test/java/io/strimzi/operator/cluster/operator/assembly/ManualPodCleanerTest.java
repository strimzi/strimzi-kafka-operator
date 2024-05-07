/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.podset.StrimziPodSetBuilder;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PvcOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class ManualPodCleanerTest {
    private final static String CONTROLLER_NAME = "controller";
    private final static Labels SELECTOR = Labels.fromString("selector=" + CONTROLLER_NAME);

    @Test
    public void testManualPodCleanupOnePodWithPodSets(VertxTestContext context) {
        List<Pod> pods = List.of(
                podWithName(CONTROLLER_NAME + "-0"),
                podWithNameAndAnnotations(CONTROLLER_NAME + "-1", Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_DELETE_POD_AND_PVC, "true")),
                podWithName(CONTROLLER_NAME + "-2")
        );

        List<PersistentVolumeClaim> pvcs = List.of(
                pvcWithName("data-" + CONTROLLER_NAME + "-0"),
                pvcWithName("data-" + CONTROLLER_NAME + "-1"),
                pvcWithName("data-" + CONTROLLER_NAME + "-2")
        );

        manualPodCleanup(context, pods, pvcs, List.of(CONTROLLER_NAME + "-1"), List.of("data-" + CONTROLLER_NAME + "-1"));
    }

    @Test
    public void testManualPodCleanupJbodWithPodSets(VertxTestContext context) {
        List<Pod> pods = List.of(
                podWithName(CONTROLLER_NAME + "-0"),
                podWithNameAndAnnotations(CONTROLLER_NAME + "-1", Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_DELETE_POD_AND_PVC, "true")),
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

        manualPodCleanup(context, pods, pvcs, List.of(CONTROLLER_NAME + "-1"), List.of("data-0-" + CONTROLLER_NAME + "-1", "data-1-" + CONTROLLER_NAME + "-1"));
    }

    @Test
    public void testManualPodCleanupMultiplePodsWithPodSets(VertxTestContext context) {
        List<Pod> pods = List.of(
                podWithName(CONTROLLER_NAME + "-0"),
                podWithNameAndAnnotations(CONTROLLER_NAME + "-1", Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_DELETE_POD_AND_PVC, "true")),
                podWithNameAndAnnotations(CONTROLLER_NAME + "-2", Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_DELETE_POD_AND_PVC, "true"))
        );

        List<PersistentVolumeClaim> pvcs = List.of(
                pvcWithName("data-0-" + CONTROLLER_NAME + "-0"),
                pvcWithName("data-0-" + CONTROLLER_NAME + "-1"),
                pvcWithName("data-0-" + CONTROLLER_NAME + "-2"),
                pvcWithName("data-1-" + CONTROLLER_NAME + "-0"),
                pvcWithName("data-1-" + CONTROLLER_NAME + "-1"),
                pvcWithName("data-1-" + CONTROLLER_NAME + "-2")
        );

        manualPodCleanup(context, pods, pvcs, List.of(CONTROLLER_NAME + "-1"), List.of("data-0-" + CONTROLLER_NAME + "-1", "data-1-" + CONTROLLER_NAME + "-1"));
    }

    @Test
    public void testManualPodCleanupNoPodsWithPodSets(VertxTestContext context) {
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

        manualPodCleanup(context, pods, pvcs, List.of(), List.of());
    }

    private void manualPodCleanup(VertxTestContext context, List<Pod> pods, List<PersistentVolumeClaim> pvcs, List<String> podsToBeDeleted, List<String> pvcsToBeDeleted) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;

        ArgumentCaptor<StrimziPodSet> podSetReconciliationCaptor = ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockPodSetOps.getAsync(any(), eq(CONTROLLER_NAME))).thenReturn(Future.succeededFuture(new StrimziPodSetBuilder().withNewMetadata().withName(CONTROLLER_NAME).endMetadata().withNewSpec().withPods(PodSetUtils.podsToMaps(pods)).endSpec().build()));
        when(mockPodSetOps.reconcile(any(), any(), eq(CONTROLLER_NAME), podSetReconciliationCaptor.capture())).thenReturn(Future.succeededFuture());

        PodOperator mockPodOps = supplier.podOperations;
        when(mockPodOps.listAsync(any(), eq(SELECTOR))).thenReturn(Future.succeededFuture(pods));
        ArgumentCaptor<String> podDeletionCaptor = ArgumentCaptor.forClass(String.class);
        when(mockPodOps.deleteAsync(any(), any(), podDeletionCaptor.capture(), anyBoolean())).thenReturn(Future.succeededFuture());
        when(mockPodOps.readiness(any(), any(), any(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());

        PvcOperator mockPvcOps = supplier.pvcOperations;
        when(mockPvcOps.listAsync(any(), eq(SELECTOR))).thenReturn(Future.succeededFuture(pvcs));
        ArgumentCaptor<String> pvcDeletionCaptor = ArgumentCaptor.forClass(String.class);
        when(mockPvcOps.deleteAsync(any(), any(), pvcDeletionCaptor.capture(), anyBoolean())).thenReturn(Future.succeededFuture());

        ManualPodCleaner cleaner = new ManualPodCleaner(
                Reconciliation.DUMMY_RECONCILIATION,
                SELECTOR,
                supplier
        );

        Checkpoint async = context.checkpoint();
        cleaner.maybeManualPodCleaning()
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    if (podsToBeDeleted.size() > 0) {
                        // PodSet was reconciled to remove the pod from it
                        assertThat(podSetReconciliationCaptor.getAllValues().size(), is(1));
                        assertThat(podSetReconciliationCaptor.getAllValues().get(0).getSpec().getPods().size(), is(2));
                    } else {
                        assertThat(podSetReconciliationCaptor.getAllValues().size(), is(0));
                    }

                    // Verify the deleted pod
                    assertThat(podDeletionCaptor.getAllValues().size(), is(podsToBeDeleted.size()));
                    assertThat(podDeletionCaptor.getAllValues(), is(podsToBeDeleted));

                    // Verify the deleted and recreated pvc
                    assertThat(pvcDeletionCaptor.getAllValues().size(), is(pvcsToBeDeleted.size()));
                    assertThat(pvcDeletionCaptor.getAllValues(), is(pvcsToBeDeleted));

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
