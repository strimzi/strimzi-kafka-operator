/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimConditionBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimStatusBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.storage.StorageClass;
import io.fabric8.kubernetes.api.model.storage.StorageClassBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.StorageClassOperator;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import java.io.StringReader;
import java.util.List;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public class VolumeResizingTest {
    private final KubernetesVersion kubernetesVersion = KubernetesVersion.V1_11;
    private final MockCertManager certManager = new MockCertManager();
    private final ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig();
    private static final KafkaVersion.Lookup VERSIONS = new KafkaVersion.Lookup(
            new StringReader(KafkaVersionTestUtils.getKafkaVersionYaml()),
            KafkaVersionTestUtils.getKafkaImageMap(),
            KafkaVersionTestUtils.getKafkaConnectImageMap(),
            KafkaVersionTestUtils.getKafkaConnectS2iImageMap(),
            KafkaVersionTestUtils.getKafkaMirrorMakerImageMap()) { };
    private final String namespace = "testns";
    private final String clusterName = "testkafka";
    protected static Vertx vertx;

    @BeforeClass
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterClass
    public static void after() {
        vertx.close();
    }

    public Kafka getKafkaCrd()  {
        return new KafkaBuilder()
                .withNewMetadata()
                    .withName(clusterName)
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withNewListeners()
                            .withNewPlain()
                            .endPlain()
                        .endListeners()
                        .withNewPersistentClaimStorage()
                            .withStorageClass("mysc")
                            .withSize("20Gi")
                        .endPersistentClaimStorage()
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewPersistentClaimStorage()
                            .withStorageClass("mysc")
                            .withSize("20Gi")
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .build();
    }

    @Test
    public void testNoExistingVolumes()  {
        Kafka kafka = getKafkaCrd();
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(kafka, VERSIONS);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the PVC Operator
        PvcOperator mockPvcOps = supplier.pvcOperations;

        when(mockPvcOps.getAsync(eq(namespace), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    return Future.succeededFuture();
                });

        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(anyString(), anyString(), pvcCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock the StorageClass Operator
        StorageClassOperator mockSco = supplier.storageClassOperations;

        when(mockSco.getAsync(eq("mysc")))
                .thenAnswer(invocation -> {
                    StorageClass sc = new StorageClassBuilder()
                            .withNewMetadata()
                                .withName("mysc")
                            .endMetadata()
                            .withAllowVolumeExpansion(true)
                            .build();

                    return Future.succeededFuture(sc);
                });

        MockKafkaAssemblyOperator kao = new MockKafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                supplier,
                config);

        kao.resizeVolumes(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName),
                kafka, kafkaCluster.generatePersistentVolumeClaims(kafka.getSpec().getKafka().getStorage()), kafkaCluster).setHandler(res -> {
                    assertTrue(res.succeeded());
                    assertEquals(3, pvcCaptor.getAllValues().size());
                    assertEquals(kafkaCluster.generatePersistentVolumeClaims(kafka.getSpec().getKafka().getStorage()), pvcCaptor.getAllValues());
                });
    }

    @Test
    public void testNotBoundVolumes()  {
        Kafka kafka = getKafkaCrd();
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(kafka, VERSIONS);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the PVC Operator
        PvcOperator mockPvcOps = supplier.pvcOperations;

        List<PersistentVolumeClaim> realPvcs = kafkaCluster.generatePersistentVolumeClaims(kafka.getSpec().getKafka().getStorage());
        when(mockPvcOps.getAsync(eq(namespace), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    return Future.succeededFuture(realPvcs.stream().filter(pvc -> pvcName.equals(pvc.getMetadata().getName())).findFirst().orElse(null));
                });

        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(anyString(), anyString(), pvcCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock the StorageClass Operator
        StorageClassOperator mockSco = supplier.storageClassOperations;

        when(mockSco.getAsync(eq("mysc")))
                .thenAnswer(invocation -> {
                    StorageClass sc = new StorageClassBuilder()
                            .withNewMetadata()
                            .withName("mysc")
                            .endMetadata()
                            .withAllowVolumeExpansion(true)
                            .build();

                    return Future.succeededFuture(sc);
                });

        MockKafkaAssemblyOperator kao = new MockKafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                supplier,
                config);

        kao.resizeVolumes(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName),
                kafka, kafkaCluster.generatePersistentVolumeClaims(kafka.getSpec().getKafka().getStorage()), kafkaCluster).setHandler(res -> {
                    assertTrue(res.succeeded());
                    assertEquals(3, pvcCaptor.getAllValues().size());
                    assertEquals(kafkaCluster.generatePersistentVolumeClaims(kafka.getSpec().getKafka().getStorage()), pvcCaptor.getAllValues());
                });
    }

    @Test
    public void testVolumesBoundExpandableStorageClass()  {
        Kafka kafka = getKafkaCrd();
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(kafka, VERSIONS);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the PVC Operator
        PvcOperator mockPvcOps = supplier.pvcOperations;

        List<PersistentVolumeClaim> realPvcs = kafkaCluster.generatePersistentVolumeClaims(kafka.getSpec().getKafka().getStorage());

        for (PersistentVolumeClaim pvc : realPvcs)    {
            pvc.getSpec().getResources().getRequests().put("storage", new Quantity("10Gi"));
            pvc.setStatus(new PersistentVolumeClaimStatusBuilder()
                    .withPhase("Bound")
                    .withCapacity(pvc.getSpec().getResources().getRequests())
                    .build());
        }

        when(mockPvcOps.getAsync(eq(namespace), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    return Future.succeededFuture(realPvcs.stream().filter(pvc -> pvcName.equals(pvc.getMetadata().getName())).findFirst().orElse(null));
                });

        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(anyString(), anyString(), pvcCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock the StorageClass Operator
        StorageClassOperator mockSco = supplier.storageClassOperations;

        when(mockSco.getAsync(eq("mysc")))
                .thenAnswer(invocation -> {
                    StorageClass sc = new StorageClassBuilder()
                            .withNewMetadata()
                            .withName("mysc")
                            .endMetadata()
                            .withAllowVolumeExpansion(true)
                            .build();

                    return Future.succeededFuture(sc);
                });

        MockKafkaAssemblyOperator kao = new MockKafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                supplier,
                config);

        kao.resizeVolumes(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName),
                kafka, kafkaCluster.generatePersistentVolumeClaims(kafka.getSpec().getKafka().getStorage()), kafkaCluster).setHandler(res -> {
                    assertTrue(res.succeeded());
                    assertEquals(3, pvcCaptor.getAllValues().size());
                    assertEquals(kafkaCluster.generatePersistentVolumeClaims(kafka.getSpec().getKafka().getStorage()), pvcCaptor.getAllValues());
                });
    }

    @Test
    public void testVolumesBoundNonExpandableStorageClass()  {
        Kafka kafka = getKafkaCrd();
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(kafka, VERSIONS);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the PVC Operator
        PvcOperator mockPvcOps = supplier.pvcOperations;

        List<PersistentVolumeClaim> realPvcs = kafkaCluster.generatePersistentVolumeClaims(kafka.getSpec().getKafka().getStorage());

        for (PersistentVolumeClaim pvc : realPvcs)    {
            pvc.getSpec().getResources().getRequests().put("storage", new Quantity("10Gi"));
            pvc.setStatus(new PersistentVolumeClaimStatusBuilder()
                    .withPhase("Bound")
                    .withCapacity(pvc.getSpec().getResources().getRequests())
                    .build());
        }

        when(mockPvcOps.getAsync(eq(namespace), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    return Future.succeededFuture(realPvcs.stream().filter(pvc -> pvcName.equals(pvc.getMetadata().getName())).findFirst().orElse(null));
                });

        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(anyString(), anyString(), pvcCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock the StorageClass Operator
        StorageClassOperator mockSco = supplier.storageClassOperations;

        when(mockSco.getAsync(eq("mysc")))
                .thenAnswer(invocation -> {
                    StorageClass sc = new StorageClassBuilder()
                            .withNewMetadata()
                            .withName("mysc")
                            .endMetadata()
                            .withAllowVolumeExpansion(false)
                            .build();

                    return Future.succeededFuture(sc);
                });

        MockKafkaAssemblyOperator kao = new MockKafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                supplier,
                config);

        kao.resizeVolumes(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName),
                kafka, kafkaCluster.generatePersistentVolumeClaims(kafka.getSpec().getKafka().getStorage()), kafkaCluster).setHandler(res -> {
                    assertTrue(res.succeeded());
                    // Resizing is not supported, we do not reconcile
                    assertEquals(0, pvcCaptor.getAllValues().size());
                });
    }

    @Test
    public void testVolumesResizing()  {
        Kafka kafka = getKafkaCrd();
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(kafka, VERSIONS);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the PVC Operator
        PvcOperator mockPvcOps = supplier.pvcOperations;

        List<PersistentVolumeClaim> realPvcs = kafkaCluster.generatePersistentVolumeClaims(kafka.getSpec().getKafka().getStorage());

        for (PersistentVolumeClaim pvc : realPvcs)    {
            pvc.setStatus(new PersistentVolumeClaimStatusBuilder()
                    .withPhase("Bound")
                    .withConditions(new PersistentVolumeClaimConditionBuilder()
                            .withStatus("True")
                            .withType("Resizing")
                            .build())
                    .withCapacity(singletonMap("storage", new Quantity("10Gi")))
                    .build());
        }

        when(mockPvcOps.getAsync(eq(namespace), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    return Future.succeededFuture(realPvcs.stream().filter(pvc -> pvcName.equals(pvc.getMetadata().getName())).findFirst().orElse(null));
                });

        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(anyString(), anyString(), pvcCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock the StorageClass Operator
        StorageClassOperator mockSco = supplier.storageClassOperations;

        when(mockSco.getAsync(eq("mysc")))
                .thenAnswer(invocation -> {
                    StorageClass sc = new StorageClassBuilder()
                            .withNewMetadata()
                            .withName("mysc")
                            .endMetadata()
                            .withAllowVolumeExpansion(true)
                            .build();

                    return Future.succeededFuture(sc);
                });

        MockKafkaAssemblyOperator kao = new MockKafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                supplier,
                config);

        kao.resizeVolumes(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName),
                kafka, kafkaCluster.generatePersistentVolumeClaims(kafka.getSpec().getKafka().getStorage()), kafkaCluster).setHandler(res -> {
                    assertTrue(res.succeeded());
                    // The volumes are resizing => no reconciliation
                    assertEquals(0, pvcCaptor.getAllValues().size());
                });
    }

    @Test
    public void testVolumesWaitingForRestart()  {
        Kafka kafka = getKafkaCrd();
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(kafka, VERSIONS);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the PVC Operator
        PvcOperator mockPvcOps = supplier.pvcOperations;

        List<PersistentVolumeClaim> realPvcs = kafkaCluster.generatePersistentVolumeClaims(kafka.getSpec().getKafka().getStorage());

        for (PersistentVolumeClaim pvc : realPvcs)    {
            pvc.setStatus(new PersistentVolumeClaimStatusBuilder()
                    .withPhase("Bound")
                    .withConditions(new PersistentVolumeClaimConditionBuilder()
                            .withStatus("True")
                            .withType("FileSystemResizePending")
                            .build())
                    .withCapacity(singletonMap("storage", new Quantity("10Gi")))
                    .build());
        }

        when(mockPvcOps.getAsync(eq(namespace), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    return Future.succeededFuture(realPvcs.stream().filter(pvc -> pvcName.equals(pvc.getMetadata().getName())).findFirst().orElse(null));
                });

        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(anyString(), anyString(), pvcCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock the StorageClass Operator
        StorageClassOperator mockSco = supplier.storageClassOperations;

        when(mockSco.getAsync(eq("mysc")))
                .thenAnswer(invocation -> {
                    StorageClass sc = new StorageClassBuilder()
                            .withNewMetadata()
                            .withName("mysc")
                            .endMetadata()
                            .withAllowVolumeExpansion(true)
                            .build();

                    return Future.succeededFuture(sc);
                });

        MockKafkaAssemblyOperator kao = new MockKafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                supplier,
                config);

        kao.resizeVolumes(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName),
                kafka, kafkaCluster.generatePersistentVolumeClaims(kafka.getSpec().getKafka().getStorage()), kafkaCluster).setHandler(res -> {
                    assertTrue(res.succeeded());

                    // The volumes are waiting for pod restart => no reconciliation
                    assertEquals(0, pvcCaptor.getAllValues().size());

                    for (int i = 0; i < kafkaCluster.getReplicas(); i++)    {
                        assertTrue(res.result().fsResizingRestartRequest.contains(kafkaCluster.getPodName(i)));
                    }

                });
    }

    @Test
    public void testVolumesResized()  {
        Kafka kafka = getKafkaCrd();
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(kafka, VERSIONS);
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock the PVC Operator
        PvcOperator mockPvcOps = supplier.pvcOperations;

        List<PersistentVolumeClaim> realPvcs = kafkaCluster.generatePersistentVolumeClaims(kafka.getSpec().getKafka().getStorage());

        for (PersistentVolumeClaim pvc : realPvcs)    {
            pvc.setStatus(new PersistentVolumeClaimStatusBuilder()
                    .withPhase("Bound")
                    .withCapacity(singletonMap("storage", new Quantity("20Gi")))
                    .build());
        }

        when(mockPvcOps.getAsync(eq(namespace), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    return Future.succeededFuture(realPvcs.stream().filter(pvc -> pvcName.equals(pvc.getMetadata().getName())).findFirst().orElse(null));
                });

        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(anyString(), anyString(), pvcCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock the StorageClass Operator
        StorageClassOperator mockSco = supplier.storageClassOperations;

        when(mockSco.getAsync(eq("mysc")))
                .thenAnswer(invocation -> {
                    StorageClass sc = new StorageClassBuilder()
                            .withNewMetadata()
                            .withName("mysc")
                            .endMetadata()
                            .withAllowVolumeExpansion(true)
                            .build();

                    return Future.succeededFuture(sc);
                });

        MockKafkaAssemblyOperator kao = new MockKafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, kubernetesVersion),
                certManager,
                supplier,
                config);

        kao.resizeVolumes(new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, namespace, clusterName),
                kafka, kafkaCluster.generatePersistentVolumeClaims(kafka.getSpec().getKafka().getStorage()), kafkaCluster).setHandler(res -> {
                    assertTrue(res.succeeded());

                    assertEquals(3, pvcCaptor.getAllValues().size());
                    assertEquals(kafkaCluster.generatePersistentVolumeClaims(kafka.getSpec().getKafka().getStorage()), pvcCaptor.getAllValues());
                });
    }

    // This allows to test the resizing on its own without any other methods being called and mocked
    class MockKafkaAssemblyOperator extends KafkaAssemblyOperator  {
        public MockKafkaAssemblyOperator(Vertx vertx, PlatformFeaturesAvailability pfa, CertManager certManager, ResourceOperatorSupplier supplier, ClusterOperatorConfig config) {
            super(vertx, pfa, certManager, supplier, config);
        }

        public Future<ReconciliationState> resizeVolumes(Reconciliation reconciliation, Kafka kafkaAssembly, List<PersistentVolumeClaim> pvcs, KafkaCluster kafkaCluster) {
            ReconciliationState rs = createReconciliationState(reconciliation, kafkaAssembly);
            return rs.maybeResizeReconcilePvcs(pvcs, kafkaCluster);
        }
    }
}
