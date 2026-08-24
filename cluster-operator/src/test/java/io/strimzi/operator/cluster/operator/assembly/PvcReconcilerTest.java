/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimConditionBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.storage.StorageClass;
import io.fabric8.kubernetes.api.model.storage.StorageClassBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PvcOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StorageClassOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@Timeout(value = 30, unit = TimeUnit.SECONDS)
public class PvcReconcilerTest {
    private final static String NAMESPACE = "testns";
    private final static String CLUSTER_NAME = "testkafka";
    private final static String STORAGE_CLASS_NAME = "mysc";
    private final static StorageClass RESIZABLE_STORAGE_CLASS = new StorageClassBuilder()
            .withNewMetadata()
                .withName(STORAGE_CLASS_NAME)
            .endMetadata()
            .withAllowVolumeExpansion(true)
            .build();
    private final static StorageClass NONRESIZABLE_STORAGE_CLASS = new StorageClassBuilder()
            .withNewMetadata()
                .withName(STORAGE_CLASS_NAME)
            .endMetadata()
            .withAllowVolumeExpansion(false)
            .build();

    // No volumes exist and should be created => this emulates new cluster deployment
    @Test
    public void testNoExistingVolumes()  {
        List<PersistentVolumeClaim> pvcs = List.of(
                createPvc("data-pod-0"),
                createPvc("data-pod-1"),
                createPvc("data-pod-2")
        );

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the PVC Operator
        PvcOperator mockPvcOps = supplier.pvcOperations;
        when(mockPvcOps.getAsync(eq(NAMESPACE), ArgumentMatchers.startsWith("data-"))).thenReturn(CompletableFuture.completedFuture(null));
        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(any(), anyString(), anyString(), pvcCaptor.capture())).thenReturn(CompletableFuture.completedFuture(null));

        // Mock the StorageClass Operator
        StorageClassOperator mockSco = supplier.storageClassOperations;
        when(mockSco.getAsync(eq(STORAGE_CLASS_NAME))).thenReturn(CompletableFuture.completedFuture(RESIZABLE_STORAGE_CLASS));

        // Reconcile the PVCs
        PvcReconciler reconciler = new PvcReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                mockPvcOps,
                mockSco
        );

        var res = reconciler.resizeAndReconcilePvcs(new KafkaStatus(), pvcs)
                .toCompletableFuture().join();

        assertThat(res.size(), is(0));
        assertThat(pvcCaptor.getAllValues().size(), is(3));
        assertThat(pvcCaptor.getAllValues(), is(pvcs));
    }

    // Volumes exist already before and are reconciled
    @Test
    public void testNotBoundVolumes()  {
        List<PersistentVolumeClaim> pvcs = List.of(
                createPvc("data-pod-0"),
                createPvc("data-pod-1"),
                createPvc("data-pod-2")
        );

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the PVC Operator
        PvcOperator mockPvcOps = supplier.pvcOperations;
        when(mockPvcOps.getAsync(eq(NAMESPACE), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    return CompletableFuture.completedFuture(pvcs.stream().filter(pvc -> pvcName.equals(pvc.getMetadata().getName())).findFirst().orElse(null));
                });
        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(any(), anyString(), anyString(), pvcCaptor.capture())).thenReturn(CompletableFuture.completedFuture(null));

        // Mock the StorageClass Operator
        StorageClassOperator mockSco = supplier.storageClassOperations;
        when(mockSco.getAsync(eq(STORAGE_CLASS_NAME))).thenReturn(CompletableFuture.completedFuture(RESIZABLE_STORAGE_CLASS));

        // Reconcile the PVCs
        PvcReconciler reconciler = new PvcReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                mockPvcOps,
                mockSco
        );

        var res = reconciler.resizeAndReconcilePvcs(new KafkaStatus(), pvcs)
                .toCompletableFuture().join();

        assertThat(res.size(), is(0));
        assertThat(pvcCaptor.getAllValues().size(), is(3));
        assertThat(pvcCaptor.getAllValues(), is(pvcs));
    }

    // Volumes exist with smaller size and are Bound with resizing supported => should be reconciled
    @Test
    public void testVolumesBoundExpandableStorageClass()  {
        List<PersistentVolumeClaim> pvcs = List.of(
                createPvc("data-pod-0"),
                createPvc("data-pod-1"),
                createPvc("data-pod-2")
        );

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the PVC Operator
        PvcOperator mockPvcOps = supplier.pvcOperations;
        when(mockPvcOps.getAsync(eq(NAMESPACE), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    PersistentVolumeClaim currentPvc = pvcs.stream().filter(pvc -> pvcName.equals(pvc.getMetadata().getName())).findFirst().orElse(null);

                    if (currentPvc != null) {
                        PersistentVolumeClaim pvcWithStatus = new PersistentVolumeClaimBuilder(currentPvc)
                                .editSpec()
                                    .withNewResources()
                                        .withRequests(Map.of("storage", new Quantity("50Gi", null)))
                                    .endResources()
                                .endSpec()
                                .withNewStatus()
                                    .withPhase("Bound")
                                    .withCapacity(Map.of("storage", new Quantity("50Gi", null)))
                                .endStatus()
                                .build();

                        return CompletableFuture.completedFuture(pvcWithStatus);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(any(), anyString(), anyString(), pvcCaptor.capture())).thenReturn(CompletableFuture.completedFuture(null));

        // Mock the StorageClass Operator
        StorageClassOperator mockSco = supplier.storageClassOperations;
        when(mockSco.getAsync(eq(STORAGE_CLASS_NAME))).thenReturn(CompletableFuture.completedFuture(RESIZABLE_STORAGE_CLASS));

        // Reconcile the PVCs
        PvcReconciler reconciler = new PvcReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                mockPvcOps,
                mockSco
        );

        var res = reconciler.resizeAndReconcilePvcs(new KafkaStatus(), pvcs)
                .toCompletableFuture().join();

        assertThat(res.size(), is(0));
        assertThat(pvcCaptor.getAllValues().size(), is(3));
        assertThat(pvcCaptor.getAllValues(), is(pvcs));
    }

    // Tests volume reconciliation when the PVC has some weird value
    //         => we cannot handle it successfully, but we should fail the reconciliation
    @Test
    public void testVolumesBoundExpandableStorageClassWithInvalidSize()  {
        List<PersistentVolumeClaim> pvcs = List.of(
                createPvc("data-pod-0"),
                createPvc("data-pod-1"),
                createPvc("data-pod-2")
        );

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the PVC Operator
        PvcOperator mockPvcOps = supplier.pvcOperations;
        when(mockPvcOps.getAsync(eq(NAMESPACE), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    PersistentVolumeClaim currentPvc = pvcs.stream().filter(pvc -> pvcName.equals(pvc.getMetadata().getName())).findFirst().orElse(null);

                    if (currentPvc != null) {
                        PersistentVolumeClaim pvcWithStatus = new PersistentVolumeClaimBuilder(currentPvc)
                                .editSpec()
                                    .withNewResources()
                                        .withRequests(Map.of("storage", new Quantity("-50000000000200Gi", null)))
                                    .endResources()
                                .endSpec()
                                .withNewStatus()
                                    .withPhase("Bound")
                                    .withCapacity(Map.of("storage", new Quantity("50Gi", null)))
                                .endStatus()
                                .build();

                        return CompletableFuture.completedFuture(pvcWithStatus);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(any(), anyString(), anyString(), pvcCaptor.capture())).thenReturn(CompletableFuture.completedFuture(null));

        // Mock the StorageClass Operator
        StorageClassOperator mockSco = supplier.storageClassOperations;
        when(mockSco.getAsync(eq(STORAGE_CLASS_NAME))).thenReturn(CompletableFuture.completedFuture(RESIZABLE_STORAGE_CLASS));

        // Reconcile the PVCs
        PvcReconciler reconciler = new PvcReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                mockPvcOps,
                mockSco
        );

        CompletionException ex = assertThrows(CompletionException.class, () ->
                reconciler.resizeAndReconcilePvcs(new KafkaStatus(), pvcs)
                        .toCompletableFuture().join()
        );
        assertThat(ex.getCause(), is(instanceOf(IllegalArgumentException.class)));
        assertThat(ex.getCause().getMessage(), is("Invalid memory suffix: -50000000000200Gi"));
    }

    // Volumes exist with smaller size and are Bound without resizing supported => should NOT be reconciled
    @Test
    public void testVolumesBoundNonExpandableStorageClass()  {
        List<PersistentVolumeClaim> pvcs = List.of(
                createPvc("data-pod-0"),
                createPvc("data-pod-1"),
                createPvc("data-pod-2")
        );

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the PVC Operator
        PvcOperator mockPvcOps = supplier.pvcOperations;
        when(mockPvcOps.getAsync(eq(NAMESPACE), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    PersistentVolumeClaim currentPvc = pvcs.stream().filter(pvc -> pvcName.equals(pvc.getMetadata().getName())).findFirst().orElse(null);

                    if (currentPvc != null) {
                        PersistentVolumeClaim pvcWithStatus = new PersistentVolumeClaimBuilder(currentPvc)
                                .editSpec()
                                    .withNewResources()
                                        .withRequests(Map.of("storage", new Quantity("50Gi", null)))
                                    .endResources()
                                .endSpec()
                                .withNewStatus()
                                    .withPhase("Bound")
                                    .withCapacity(Map.of("storage", new Quantity("50Gi", null)))
                                .endStatus()
                                .build();

                        return CompletableFuture.completedFuture(pvcWithStatus);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(any(), anyString(), anyString(), pvcCaptor.capture())).thenReturn(CompletableFuture.completedFuture(null));

        // Mock the StorageClass Operator
        StorageClassOperator mockSco = supplier.storageClassOperations;
        when(mockSco.getAsync(eq(STORAGE_CLASS_NAME))).thenReturn(CompletableFuture.completedFuture(NONRESIZABLE_STORAGE_CLASS));

        // Reconcile the PVCs
        PvcReconciler reconciler = new PvcReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                mockPvcOps,
                mockSco
        );

        // Used to capture the warning condition
        KafkaStatus kafkaStatus = new KafkaStatus();

        var res = reconciler.resizeAndReconcilePvcs(kafkaStatus, pvcs)
                .toCompletableFuture().join();

        assertThat(res.size(), is(0));
        assertThat(pvcCaptor.getAllValues().size(), is(0));
        assertThat(kafkaStatus.getConditions().size(), is(3));
        kafkaStatus.getConditions().stream().forEach(c -> {
            assertThat(c.getReason(), is("PvcResizingWarning"));
            assertThat(c.getMessage(), containsString("Storage Class mysc does not support resizing of volumes."));
        });
    }

    // Volumes exist with smaller size and are Bound without storage class => should NOT be reconciled
    @Test
    public void testVolumesBoundMissingStorageClass()  {
        List<PersistentVolumeClaim> pvcs = List.of(
                createPvc("data-pod-0"),
                createPvc("data-pod-1"),
                createPvc("data-pod-2")
        );

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the PVC Operator
        PvcOperator mockPvcOps = supplier.pvcOperations;
        when(mockPvcOps.getAsync(eq(NAMESPACE), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    PersistentVolumeClaim currentPvc = pvcs.stream().filter(pvc -> pvcName.equals(pvc.getMetadata().getName())).findFirst().orElse(null);

                    if (currentPvc != null) {
                        PersistentVolumeClaim pvcWithStatus = new PersistentVolumeClaimBuilder(currentPvc)
                                .editSpec()
                                    .withNewResources()
                                        .withRequests(Map.of("storage", new Quantity("50Gi", null)))
                                    .endResources()
                                .endSpec()
                                .withNewStatus()
                                    .withPhase("Bound")
                                    .withCapacity(Map.of("storage", new Quantity("50Gi", null)))
                                .endStatus()
                                .build();

                        return CompletableFuture.completedFuture(pvcWithStatus);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(any(), anyString(), anyString(), pvcCaptor.capture())).thenReturn(CompletableFuture.completedFuture(null));

        // Mock the StorageClass Operator
        StorageClassOperator mockSco = supplier.storageClassOperations;
        when(mockSco.getAsync(eq(STORAGE_CLASS_NAME))).thenReturn(CompletableFuture.completedFuture(null));

        // Reconcile the PVCs
        PvcReconciler reconciler = new PvcReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                mockPvcOps,
                mockSco
        );

        // Used to capture the warning condition
        KafkaStatus kafkaStatus = new KafkaStatus();

        var res = reconciler.resizeAndReconcilePvcs(kafkaStatus, pvcs)
                .toCompletableFuture().join();

        assertThat(res.size(), is(0));
        assertThat(pvcCaptor.getAllValues().size(), is(0));
        assertThat(kafkaStatus.getConditions().size(), is(3));
        kafkaStatus.getConditions().stream().forEach(c -> {
            assertThat(c.getReason(), is("PvcResizingWarning"));
            assertThat(c.getMessage(), containsString("Storage Class mysc not found."));
        });
    }

    // Volumes exist with smaller size and are Bound without storage class => should NOT be reconciled
    @Test
    public void testVolumesBoundWithoutStorageClass()  {
        List<PersistentVolumeClaim> pvcs = List.of(
                createPvc("data-pod-0"),
                createPvc("data-pod-1"),
                createPvc("data-pod-2")
        );

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the PVC Operator
        PvcOperator mockPvcOps = supplier.pvcOperations;
        when(mockPvcOps.getAsync(eq(NAMESPACE), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    PersistentVolumeClaim currentPvc = pvcs.stream().filter(pvc -> pvcName.equals(pvc.getMetadata().getName())).findFirst().orElse(null);

                    if (currentPvc != null) {
                        PersistentVolumeClaim pvcWithStatus = new PersistentVolumeClaimBuilder(currentPvc)
                                .editSpec()
                                    .withStorageClassName(null)
                                    .withNewResources()
                                        .withRequests(Map.of("storage", new Quantity("50Gi", null)))
                                    .endResources()
                                .endSpec()
                                .withNewStatus()
                                    .withPhase("Bound")
                                    .withCapacity(Map.of("storage", new Quantity("50Gi", null)))
                                .endStatus()
                                .build();

                        return CompletableFuture.completedFuture(pvcWithStatus);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(any(), anyString(), anyString(), pvcCaptor.capture())).thenReturn(CompletableFuture.completedFuture(null));

        // Reconcile the PVCs
        PvcReconciler reconciler = new PvcReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                mockPvcOps,
                supplier.storageClassOperations
        );

        // Used to capture the warning condition
        KafkaStatus kafkaStatus = new KafkaStatus();

        var res = reconciler.resizeAndReconcilePvcs(kafkaStatus, pvcs)
                .toCompletableFuture().join();

        assertThat(res.size(), is(0));
        assertThat(pvcCaptor.getAllValues().size(), is(0));
        assertThat(kafkaStatus.getConditions().size(), is(3));
        kafkaStatus.getConditions().stream().forEach(c -> {
            assertThat(c.getReason(), is("PvcResizingWarning"));
            assertThat(c.getMessage(), containsString("does not use any Storage Class and cannot be resized."));
        });
    }

    // Volumes are resizing => we have to wait, no reconcile
    @Test
    public void testVolumesResizing()  {
        List<PersistentVolumeClaim> pvcs = List.of(
                createPvc("data-pod-0"),
                createPvc("data-pod-1"),
                createPvc("data-pod-2")
        );

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the PVC Operator
        PvcOperator mockPvcOps = supplier.pvcOperations;
        when(mockPvcOps.getAsync(eq(NAMESPACE), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    PersistentVolumeClaim currentPvc = pvcs.stream().filter(pvc -> pvcName.equals(pvc.getMetadata().getName())).findFirst().orElse(null);

                    if (currentPvc != null) {
                        PersistentVolumeClaim pvcWithStatus = new PersistentVolumeClaimBuilder(currentPvc)
                                .withNewStatus()
                                    .withPhase("Bound")
                                    .withConditions(new PersistentVolumeClaimConditionBuilder()
                                            .withStatus("True")
                                            .withType("Resizing")
                                            .build())
                                    .withCapacity(Map.of("storage", new Quantity("50Gi", null)))
                                .endStatus()
                                .build();

                        return CompletableFuture.completedFuture(pvcWithStatus);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(any(), anyString(), anyString(), pvcCaptor.capture())).thenReturn(CompletableFuture.completedFuture(null));

        // Mock the StorageClass Operator
        StorageClassOperator mockSco = supplier.storageClassOperations;
        when(mockSco.getAsync(eq(STORAGE_CLASS_NAME))).thenReturn(CompletableFuture.completedFuture(RESIZABLE_STORAGE_CLASS));

        // Reconcile the PVCs
        PvcReconciler reconciler = new PvcReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                mockPvcOps,
                mockSco
        );

        var res = reconciler.resizeAndReconcilePvcs(new KafkaStatus(), pvcs)
                .toCompletableFuture().join();

        assertThat(res.size(), is(0));
        assertThat(pvcCaptor.getAllValues().size(), is(0));
    }

    // Volumes need restart for file system resizing => No reconciliation, mark for restart
    @Test
    public void testVolumesWaitingForRestart()  {
        List<PersistentVolumeClaim> pvcs = List.of(
                createPvc("data-pod-0"),
                createPvc("data-pod-1"),
                createPvc("data-pod-2")
        );

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the PVC Operator
        PvcOperator mockPvcOps = supplier.pvcOperations;
        when(mockPvcOps.getAsync(eq(NAMESPACE), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    PersistentVolumeClaim currentPvc = pvcs.stream().filter(pvc -> pvcName.equals(pvc.getMetadata().getName())).findFirst().orElse(null);

                    if (currentPvc != null) {
                        PersistentVolumeClaim pvcWithStatus = new PersistentVolumeClaimBuilder(currentPvc)
                                .withNewStatus()
                                    .withPhase("Bound")
                                    .withConditions(new PersistentVolumeClaimConditionBuilder()
                                            .withStatus("True")
                                            .withType("FileSystemResizePending")
                                            .build())
                                    .withCapacity(Map.of("storage", new Quantity("50Gi", null)))
                                .endStatus()
                                .build();

                        return CompletableFuture.completedFuture(pvcWithStatus);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(any(), anyString(), anyString(), pvcCaptor.capture())).thenReturn(CompletableFuture.completedFuture(null));

        // Mock the StorageClass Operator
        StorageClassOperator mockSco = supplier.storageClassOperations;
        when(mockSco.getAsync(eq(STORAGE_CLASS_NAME))).thenReturn(CompletableFuture.completedFuture(RESIZABLE_STORAGE_CLASS));

        // Reconcile the PVCs
        PvcReconciler reconciler = new PvcReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                mockPvcOps,
                mockSco
        );

        var res = reconciler.resizeAndReconcilePvcs(new KafkaStatus(), pvcs)
                .toCompletableFuture().join();

        assertThat(res.size(), is(3));
        assertThat(res, is(Set.of(0, 1, 2)));
        assertThat(pvcCaptor.getAllValues().size(), is(0));
    }

    // Volumes are resized => nothing to do, we just reconcile
    @Test
    public void testVolumesResized()  {
        List<PersistentVolumeClaim> pvcs = List.of(
                createPvc("data-pod-0"),
                createPvc("data-pod-1"),
                createPvc("data-pod-2")
        );

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the PVC Operator
        PvcOperator mockPvcOps = supplier.pvcOperations;
        when(mockPvcOps.getAsync(eq(NAMESPACE), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    PersistentVolumeClaim currentPvc = pvcs.stream().filter(pvc -> pvcName.equals(pvc.getMetadata().getName())).findFirst().orElse(null);

                    if (currentPvc != null) {
                        PersistentVolumeClaim pvcWithStatus = new PersistentVolumeClaimBuilder(currentPvc)
                                .withNewStatus()
                                    .withPhase("Bound")
                                    .withCapacity(Map.of("storage", new Quantity("100Gi", null)))
                                .endStatus()
                                .build();

                        return CompletableFuture.completedFuture(pvcWithStatus);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(any(), anyString(), anyString(), pvcCaptor.capture())).thenReturn(CompletableFuture.completedFuture(null));

        // Mock the StorageClass Operator
        StorageClassOperator mockSco = supplier.storageClassOperations;
        when(mockSco.getAsync(eq(STORAGE_CLASS_NAME))).thenReturn(CompletableFuture.completedFuture(RESIZABLE_STORAGE_CLASS));

        // Reconcile the PVCs
        PvcReconciler reconciler = new PvcReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                mockPvcOps,
                mockSco
        );

        var res = reconciler.resizeAndReconcilePvcs(new KafkaStatus(), pvcs)
                .toCompletableFuture().join();

        assertThat(res.size(), is(0));
        assertThat(pvcCaptor.getAllValues().size(), is(3));
        assertThat(pvcCaptor.getAllValues(), is(pvcs));
    }

    // Not needed volumes with delete claim are deleted
    @Test
    public void testVolumesDeletion()  {
        PersistentVolumeClaim pvcWithDeleteClaim = new PersistentVolumeClaimBuilder(createPvc("data-pod-3"))
                .editMetadata()
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM, "true"))
                .endMetadata()
                .build();

        List<String> desiredPvcs = List.of(
                "data-pod-0",
                "data-pod-1",
                "data-pod-2"
        );

        List<String> maybeDeletePvcs = List.of(
                "data-pod-0",
                "data-pod-1",
                "data-pod-2",
                "data-pod-3",
                "data-pod-4"
        );

        List<PersistentVolumeClaim> pvcs = List.of(
                createPvc("data-pod-0"),
                createPvc("data-pod-1"),
                createPvc("data-pod-2"),
                pvcWithDeleteClaim,
                createPvc("data-pod-4")
        );

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the PVC Operator
        PvcOperator mockPvcOps = supplier.pvcOperations;
        when(mockPvcOps.getAsync(eq(NAMESPACE), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    return CompletableFuture.completedFuture(pvcs.stream().filter(pvc -> pvcName.equals(pvc.getMetadata().getName())).findFirst().orElse(null));
                });
        ArgumentCaptor<String> pvcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(any(), anyString(), pvcNameCaptor.capture(), pvcCaptor.capture())).thenReturn(CompletableFuture.completedFuture(null));

        // Reconcile the PVCs
        PvcReconciler reconciler = new PvcReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                mockPvcOps,
                supplier.storageClassOperations
        );

        reconciler.deletePersistentClaims(new ArrayList<>(maybeDeletePvcs), new ArrayList<>(desiredPvcs))
                .toCompletableFuture().join();

        assertThat(pvcNameCaptor.getAllValues().size(), is(1));
        assertThat(pvcNameCaptor.getValue(), is("data-pod-3"));
        assertThat(pvcCaptor.getAllValues().size(), is(1));
        assertThat(pvcCaptor.getValue(), is(nullValue()));
    }

    private PersistentVolumeClaim createPvc(String name)   {
        return new PersistentVolumeClaimBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withAnnotations(Map.of())
                .endMetadata()
                .withNewSpec()
                    .withAccessModes("ReadWriteOnce")
                    .withNewResources()
                        .withRequests(Map.of("storage", new Quantity("100Gi", null)))
                    .endResources()
                    .withStorageClassName(PvcReconcilerTest.STORAGE_CLASS_NAME)
                    .withVolumeMode("Filesystem")
                .endSpec()
                .build();
    }
}
