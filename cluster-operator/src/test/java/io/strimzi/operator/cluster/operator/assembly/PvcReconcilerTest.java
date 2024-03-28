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
import io.vertx.core.Future;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
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
    public void testNoExistingVolumes(VertxTestContext context)  {
        List<PersistentVolumeClaim> pvcs = List.of(
                createPvc("data-pod-0"),
                createPvc("data-pod-1"),
                createPvc("data-pod-2")
        );

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the PVC Operator
        PvcOperator mockPvcOps = supplier.pvcOperations;
        when(mockPvcOps.getAsync(eq(NAMESPACE), ArgumentMatchers.startsWith("data-"))).thenReturn(Future.succeededFuture());
        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(any(), anyString(), anyString(), pvcCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock the StorageClass Operator
        StorageClassOperator mockSco = supplier.storageClassOperations;
        when(mockSco.getAsync(eq(STORAGE_CLASS_NAME))).thenReturn(Future.succeededFuture(RESIZABLE_STORAGE_CLASS));

        // Reconcile the PVCs
        PvcReconciler reconciler = new PvcReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                mockPvcOps,
                mockSco
        );

        Checkpoint async = context.checkpoint();
        reconciler.resizeAndReconcilePvcs(new KafkaStatus(), i -> "pod-" + i, pvcs)
                .onComplete(res -> {
                    assertThat(res.succeeded(), is(true));

                    assertThat(res.result().size(), is(0));

                    assertThat(pvcCaptor.getAllValues().size(), is(3));
                    assertThat(pvcCaptor.getAllValues(), is(pvcs));

                    async.flag();
                });
    }

    // Volumes exist already before and are reconciled
    @Test
    public void testNotBoundVolumes(VertxTestContext context)  {
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
                    return Future.succeededFuture(pvcs.stream().filter(pvc -> pvcName.equals(pvc.getMetadata().getName())).findFirst().orElse(null));
                });
        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(any(), anyString(), anyString(), pvcCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock the StorageClass Operator
        StorageClassOperator mockSco = supplier.storageClassOperations;
        when(mockSco.getAsync(eq(STORAGE_CLASS_NAME))).thenReturn(Future.succeededFuture(RESIZABLE_STORAGE_CLASS));

        // Reconcile the PVCs
        PvcReconciler reconciler = new PvcReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                mockPvcOps,
                mockSco
        );

        Checkpoint async = context.checkpoint();
        reconciler.resizeAndReconcilePvcs(new KafkaStatus(), i -> "pod-" + i, pvcs)
                .onComplete(res -> {
                    assertThat(res.succeeded(), is(true));

                    assertThat(res.result().size(), is(0));

                    assertThat(pvcCaptor.getAllValues().size(), is(3));
                    assertThat(pvcCaptor.getAllValues(), is(pvcs));

                    async.flag();
                });
    }

    // Volumes exist with smaller size and are Bound with resizing supported => should be reconciled
    @Test
    public void testVolumesBoundExpandableStorageClass(VertxTestContext context)  {
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

                        return Future.succeededFuture(pvcWithStatus);
                    } else {
                        return Future.succeededFuture();
                    }
                });
        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(any(), anyString(), anyString(), pvcCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock the StorageClass Operator
        StorageClassOperator mockSco = supplier.storageClassOperations;
        when(mockSco.getAsync(eq(STORAGE_CLASS_NAME))).thenReturn(Future.succeededFuture(RESIZABLE_STORAGE_CLASS));

        // Reconcile the PVCs
        PvcReconciler reconciler = new PvcReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                mockPvcOps,
                mockSco
        );

        Checkpoint async = context.checkpoint();
        reconciler.resizeAndReconcilePvcs(new KafkaStatus(), i -> "pod-" + i, pvcs)
                .onComplete(res -> {
                    assertThat(res.succeeded(), is(true));

                    assertThat(res.result().size(), is(0));

                    assertThat(pvcCaptor.getAllValues().size(), is(3));
                    assertThat(pvcCaptor.getAllValues(), is(pvcs));

                    async.flag();
                });
    }

    // Tests volume reconciliation when the PVC has some weird value
    //         => we cannot handle it successfully, but we should fail the reconciliation
    @Test
    public void testVolumesBoundExpandableStorageClassWithInvalidSize(VertxTestContext context)  {
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

                        return Future.succeededFuture(pvcWithStatus);
                    } else {
                        return Future.succeededFuture();
                    }
                });
        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(any(), anyString(), anyString(), pvcCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock the StorageClass Operator
        StorageClassOperator mockSco = supplier.storageClassOperations;
        when(mockSco.getAsync(eq(STORAGE_CLASS_NAME))).thenReturn(Future.succeededFuture(RESIZABLE_STORAGE_CLASS));

        // Reconcile the PVCs
        PvcReconciler reconciler = new PvcReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                mockPvcOps,
                mockSco
        );

        Checkpoint async = context.checkpoint();
        reconciler.resizeAndReconcilePvcs(new KafkaStatus(), i -> "pod-" + i, pvcs)
                .onComplete(res -> {
                    assertThat(res.succeeded(), is(false));
                    assertThat(res.cause(), is(instanceOf(IllegalArgumentException.class)));
                    assertThat(res.cause().getMessage(), is("Invalid memory suffix: -50000000000200Gi"));
                    async.flag();
                });
    }

    // Volumes exist with smaller size and are Bound without resizing supported => should NOT be reconciled
    @Test
    public void testVolumesBoundNonExpandableStorageClass(VertxTestContext context)  {
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

                        return Future.succeededFuture(pvcWithStatus);
                    } else {
                        return Future.succeededFuture();
                    }
                });
        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(any(), anyString(), anyString(), pvcCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock the StorageClass Operator
        StorageClassOperator mockSco = supplier.storageClassOperations;
        when(mockSco.getAsync(eq(STORAGE_CLASS_NAME))).thenReturn(Future.succeededFuture(NONRESIZABLE_STORAGE_CLASS));

        // Reconcile the PVCs
        PvcReconciler reconciler = new PvcReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                mockPvcOps,
                mockSco
        );

        // Used to capture the warning condition
        KafkaStatus kafkaStatus = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        reconciler.resizeAndReconcilePvcs(kafkaStatus, i -> "pod-" + i, pvcs)
                .onComplete(res -> {
                    assertThat(res.succeeded(), is(true));

                    assertThat(res.result().size(), is(0));

                    assertThat(pvcCaptor.getAllValues().size(), is(0));

                    assertThat(kafkaStatus.getConditions().size(), is(3));
                    kafkaStatus.getConditions().stream().forEach(c -> {
                        assertThat(c.getReason(), is("PvcResizingWarning"));
                        assertThat(c.getMessage(), containsString("Storage Class mysc does not support resizing of volumes."));
                    });

                    async.flag();
                });
    }

    // Volumes exist with smaller size and are Bound without storage class => should NOT be reconciled
    @Test
    public void testVolumesBoundMissingStorageClass(VertxTestContext context)  {
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

                        return Future.succeededFuture(pvcWithStatus);
                    } else {
                        return Future.succeededFuture();
                    }
                });
        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(any(), anyString(), anyString(), pvcCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock the StorageClass Operator
        StorageClassOperator mockSco = supplier.storageClassOperations;
        when(mockSco.getAsync(eq(STORAGE_CLASS_NAME))).thenReturn(Future.succeededFuture(null));

        // Reconcile the PVCs
        PvcReconciler reconciler = new PvcReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                mockPvcOps,
                mockSco
        );

        // Used to capture the warning condition
        KafkaStatus kafkaStatus = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        reconciler.resizeAndReconcilePvcs(kafkaStatus, i -> "pod-" + i, pvcs)
                .onComplete(res -> {
                    assertThat(res.succeeded(), is(true));

                    assertThat(res.result().size(), is(0));

                    assertThat(pvcCaptor.getAllValues().size(), is(0));

                    assertThat(kafkaStatus.getConditions().size(), is(3));
                    kafkaStatus.getConditions().stream().forEach(c -> {
                        assertThat(c.getReason(), is("PvcResizingWarning"));
                        assertThat(c.getMessage(), containsString("Storage Class mysc not found."));
                    });

                    async.flag();
                });
    }

    // Volumes exist with smaller size and are Bound without storage class => should NOT be reconciled
    @Test
    public void testVolumesBoundWithoutStorageClass(VertxTestContext context)  {
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

                        return Future.succeededFuture(pvcWithStatus);
                    } else {
                        return Future.succeededFuture();
                    }
                });
        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(any(), anyString(), anyString(), pvcCaptor.capture())).thenReturn(Future.succeededFuture());

        // Reconcile the PVCs
        PvcReconciler reconciler = new PvcReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                mockPvcOps,
                supplier.storageClassOperations
        );

        // Used to capture the warning condition
        KafkaStatus kafkaStatus = new KafkaStatus();

        Checkpoint async = context.checkpoint();
        reconciler.resizeAndReconcilePvcs(kafkaStatus, i -> "pod-" + i, pvcs)
                .onComplete(res -> {
                    assertThat(res.succeeded(), is(true));

                    assertThat(res.result().size(), is(0));

                    assertThat(pvcCaptor.getAllValues().size(), is(0));

                    assertThat(kafkaStatus.getConditions().size(), is(3));
                    kafkaStatus.getConditions().stream().forEach(c -> {
                        assertThat(c.getReason(), is("PvcResizingWarning"));
                        assertThat(c.getMessage(), containsString("does not use any Storage Class and cannot be resized."));
                    });

                    async.flag();
                });
    }

    // Volumes are resizing => we have to wait, no reconcile
    @Test
    public void testVolumesResizing(VertxTestContext context)  {
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

                        return Future.succeededFuture(pvcWithStatus);
                    } else {
                        return Future.succeededFuture();
                    }
                });
        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(any(), anyString(), anyString(), pvcCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock the StorageClass Operator
        StorageClassOperator mockSco = supplier.storageClassOperations;
        when(mockSco.getAsync(eq(STORAGE_CLASS_NAME))).thenReturn(Future.succeededFuture(RESIZABLE_STORAGE_CLASS));

        // Reconcile the PVCs
        PvcReconciler reconciler = new PvcReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                mockPvcOps,
                mockSco
        );

        Checkpoint async = context.checkpoint();
        reconciler.resizeAndReconcilePvcs(new KafkaStatus(), i -> "pod-" + i, pvcs)
                .onComplete(res -> {
                    assertThat(res.succeeded(), is(true));

                    assertThat(res.result().size(), is(0));

                    assertThat(pvcCaptor.getAllValues().size(), is(0));

                    async.flag();
                });
    }

    // Volumes need restart for file system resizing => No reconciliation, mark for restart
    @Test
    public void testVolumesWaitingForRestart(VertxTestContext context)  {
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

                        return Future.succeededFuture(pvcWithStatus);
                    } else {
                        return Future.succeededFuture();
                    }
                });
        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(any(), anyString(), anyString(), pvcCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock the StorageClass Operator
        StorageClassOperator mockSco = supplier.storageClassOperations;
        when(mockSco.getAsync(eq(STORAGE_CLASS_NAME))).thenReturn(Future.succeededFuture(RESIZABLE_STORAGE_CLASS));

        // Reconcile the PVCs
        PvcReconciler reconciler = new PvcReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                mockPvcOps,
                mockSco
        );

        Checkpoint async = context.checkpoint();
        reconciler.resizeAndReconcilePvcs(new KafkaStatus(), i -> "pod-" + i, pvcs)
                .onComplete(res -> {
                    assertThat(res.succeeded(), is(true));

                    assertThat(res.result().size(), is(3));
                    assertThat(res.result(), is(Set.of("pod-0", "pod-1", "pod-2")));

                    assertThat(pvcCaptor.getAllValues().size(), is(0));

                    async.flag();
                });
    }

    // Volumes are resized => nothing to do, we just reconcile
    @Test
    public void testVolumesResized(VertxTestContext context)  {
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

                        return Future.succeededFuture(pvcWithStatus);
                    } else {
                        return Future.succeededFuture();
                    }
                });
        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(any(), anyString(), anyString(), pvcCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock the StorageClass Operator
        StorageClassOperator mockSco = supplier.storageClassOperations;
        when(mockSco.getAsync(eq(STORAGE_CLASS_NAME))).thenReturn(Future.succeededFuture(RESIZABLE_STORAGE_CLASS));

        // Reconcile the PVCs
        PvcReconciler reconciler = new PvcReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                mockPvcOps,
                mockSco
        );

        Checkpoint async = context.checkpoint();
        reconciler.resizeAndReconcilePvcs(new KafkaStatus(), i -> "pod-" + i, pvcs)
                .onComplete(res -> {
                    assertThat(res.succeeded(), is(true));

                    assertThat(res.result().size(), is(0));

                    assertThat(pvcCaptor.getAllValues().size(), is(3));
                    assertThat(pvcCaptor.getAllValues(), is(pvcs));

                    async.flag();
                });
    }

    // Not needed volumes with delete claim are deleted
    @Test
    public void testVolumesDeletion(VertxTestContext context)  {
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
                    return Future.succeededFuture(pvcs.stream().filter(pvc -> pvcName.equals(pvc.getMetadata().getName())).findFirst().orElse(null));
                });
        ArgumentCaptor<String> pvcNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(any(), anyString(), pvcNameCaptor.capture(), pvcCaptor.capture())).thenReturn(Future.succeededFuture());

        // Reconcile the PVCs
        PvcReconciler reconciler = new PvcReconciler(
                new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME),
                mockPvcOps,
                supplier.storageClassOperations
        );

        Checkpoint async = context.checkpoint();
        reconciler.deletePersistentClaims(new ArrayList<>(maybeDeletePvcs), new ArrayList<>(desiredPvcs))
                .onComplete(res -> {
                    assertThat(res.succeeded(), is(true));

                    assertThat(pvcNameCaptor.getAllValues().size(), is(1));
                    assertThat(pvcNameCaptor.getValue(), is("data-pod-3"));

                    assertThat(pvcCaptor.getAllValues().size(), is(1));
                    assertThat(pvcCaptor.getValue(), is(nullValue()));

                    async.flag();
                });
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
