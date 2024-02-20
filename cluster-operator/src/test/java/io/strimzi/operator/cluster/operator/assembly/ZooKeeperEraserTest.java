/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorage;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.SharedEnvironmentProvider;
import io.strimzi.operator.cluster.model.VolumeUtils;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.strimzi.operator.common.operator.resource.StrimziPodSetOperator;
import io.vertx.core.Future;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static io.strimzi.api.kafka.model.kafka.Storage.deleteClaim;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class ZooKeeperEraserTest {

    private static final String NAMESPACE = "my-namespace";
    private static final String NAME = "my-cluster";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(NAME)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withNewKafka()
                    .withReplicas(3)
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName("plain")
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .build())
                .endKafka()
                .withNewZookeeper()
                    .withReplicas(3)
                    .withNewPersistentClaimStorage()
                        .withSize("123")
                        .withStorageClass("foo")
                        .withDeleteClaim(true)
                    .endPersistentClaimStorage()
                .endZookeeper()
            .endSpec()
            .build();

    @Test
    public void testZookeeperEraserReconcilePVCDeletionWithDeleteClaimTrue(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        SecretOperator mockSecretOps = supplier.secretOperations;
        PvcOperator mockPvcOps = supplier.pvcOperations;
        SharedEnvironmentProvider sharedEnvironmentProvider = supplier.sharedEnvironmentProvider;

        ZookeeperCluster zkCluster = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, VERSIONS, sharedEnvironmentProvider);

        when(mockPodSetOps.deleteAsync(any(), anyString(), anyString(), anyBoolean())).thenAnswer(i -> Future.succeededFuture());
        when(mockSecretOps.deleteAsync(any(), anyString(), anyString(), anyBoolean())).thenReturn(Future.succeededFuture());
        when(mockSaOps.deleteAsync(any(), anyString(), anyString(), anyBoolean())).thenReturn(Future.succeededFuture());
        when(mockServiceOps.deleteAsync(any(), anyString(), anyString(), anyBoolean())).thenReturn(Future.succeededFuture());
        when(mockNetPolicyOps.deleteAsync(any(), anyString(), anyString(), anyBoolean())).thenReturn(Future.succeededFuture());
        when(mockCmOps.deleteAsync(any(), anyString(), anyString(), anyBoolean())).thenReturn(Future.succeededFuture());
        when(mockPdbOps.deleteAsync(any(), anyString(), anyString(), anyBoolean())).thenReturn(Future.succeededFuture());

        // Mock the PVC Operator
        Map<String, PersistentVolumeClaim> zkPvcs = createZooPvcs(NAMESPACE, zkCluster.getStorage(), zkCluster.nodes(),
                (replica, storageId) -> VolumeUtils.DATA_VOLUME_NAME + "-" + KafkaResources.zookeeperPodName(KAFKA.getMetadata().getName(), replica),  deleteClaim(KAFKA.getSpec().getZookeeper().getStorage()));

        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(any(), anyString(), anyString(), pvcCaptor.capture())).thenReturn(Future.succeededFuture());
        when(mockPvcOps.getAsync(anyString(), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    if (pvcName.contains(zkCluster.getComponentName())) {
                        return Future.succeededFuture(zkPvcs.get(pvcName));
                    }
                    return Future.succeededFuture(null);
                });
        when(mockPvcOps.listAsync(anyString(), ArgumentMatchers.any(Labels.class)))
                .thenAnswer(invocation -> Future.succeededFuture(zkPvcs.values().stream().toList()));

        // test reconcile
        ZooKeeperEraser rcnclr = new ZooKeeperEraser(
                Reconciliation.DUMMY_RECONCILIATION,
                supplier
        );

        Checkpoint async = context.checkpoint();
        rcnclr.reconcile()
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    verify(mockCmOps, times(1)).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockSaOps, times(1)).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockServiceOps, times(2)).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockSecretOps, times(2)).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockNetPolicyOps, times(1)).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockPodSetOps, times(1)).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockPdbOps, times(1)).deleteAsync(any(), any(), any(), anyBoolean());

                    // Check PVCs
                    verify(mockPvcOps, times(3)).getAsync(any(), any());
                    verify(mockPvcOps, times(1)).listAsync(any(), ArgumentMatchers.any(Labels.class));
                    verify(mockPvcOps, times(3)).reconcile(any(), any(), any(), any());
                    assertThat(pvcCaptor.getAllValues().size(), is(3));
                    assertThat(pvcCaptor.getAllValues().get(0), is(nullValue()));
                    assertThat(pvcCaptor.getAllValues().get(1), is(nullValue()));
                    assertThat(pvcCaptor.getAllValues().get(2), is(nullValue()));
                    async.flag();
                })));
    }

    @Test
    public void testZookeeperEraserReconcilePVCDeletionWithDeleteClaimFalse(VertxTestContext context) {

        Kafka patchedKafka = new KafkaBuilder(KAFKA)
                .editOrNewSpec()
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewPersistentClaimStorage()
                            .withSize("123")
                            .withStorageClass("foo")
                            .withDeleteClaim(false)
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        SecretOperator mockSecretOps = supplier.secretOperations;
        PvcOperator mockPvcOps = supplier.pvcOperations;
        SharedEnvironmentProvider sharedEnvironmentProvider = supplier.sharedEnvironmentProvider;

        ZookeeperCluster zkCluster = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, patchedKafka, VERSIONS, sharedEnvironmentProvider);

        when(mockPodSetOps.deleteAsync(any(), anyString(), anyString(), anyBoolean())).thenAnswer(i -> Future.succeededFuture());
        when(mockSecretOps.deleteAsync(any(), anyString(), anyString(), anyBoolean())).thenReturn(Future.succeededFuture());
        when(mockSaOps.deleteAsync(any(), anyString(), anyString(), anyBoolean())).thenReturn(Future.succeededFuture());
        when(mockServiceOps.deleteAsync(any(), anyString(), anyString(), anyBoolean())).thenReturn(Future.succeededFuture());
        when(mockNetPolicyOps.deleteAsync(any(), anyString(), anyString(), anyBoolean())).thenReturn(Future.succeededFuture());
        when(mockCmOps.deleteAsync(any(), anyString(), anyString(), anyBoolean())).thenReturn(Future.succeededFuture());
        when(mockPdbOps.deleteAsync(any(), anyString(), anyString(), anyBoolean())).thenReturn(Future.succeededFuture());

        // Mock the PVC Operator
        Map<String, PersistentVolumeClaim> zkPvcs = createZooPvcs(NAMESPACE, zkCluster.getStorage(), zkCluster.nodes(),
                (replica, storageId) -> VolumeUtils.DATA_VOLUME_NAME + "-" + KafkaResources.zookeeperPodName(patchedKafka.getMetadata().getName(), replica), deleteClaim(patchedKafka.getSpec().getZookeeper().getStorage()));

        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.getAsync(anyString(), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    if (pvcName.contains(zkCluster.getComponentName())) {
                        return Future.succeededFuture(zkPvcs.get(pvcName));
                    }
                    return Future.succeededFuture(null);
                });
        when(mockPvcOps.listAsync(anyString(), ArgumentMatchers.any(Labels.class)))
                .thenAnswer(invocation -> Future.succeededFuture(zkPvcs.values().stream().toList()));

        // test reconcile
        ZooKeeperEraser rcnclr = new ZooKeeperEraser(
                Reconciliation.DUMMY_RECONCILIATION,
                supplier
        );

        Checkpoint async = context.checkpoint();
        rcnclr.reconcile()
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    verify(mockCmOps, times(1)).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockSaOps, times(1)).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockServiceOps, times(2)).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockSecretOps, times(2)).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockNetPolicyOps, times(1)).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockPodSetOps, times(1)).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockPdbOps, times(1)).deleteAsync(any(), any(), any(), anyBoolean());

                    // Check PVCs
                    verify(mockPvcOps, times(3)).getAsync(any(), any());
                    verify(mockPvcOps, times(3)).getAsync(any(), any());
                    // no reconcile since there was no PVC deletion
                    verify(mockPvcOps, times(0)).reconcile(any(), any(), any(), any());
                    assertThat(pvcCaptor.getAllValues().size(), is(0));
                    async.flag();
                })));
    }

    private Map<String, PersistentVolumeClaim> createZooPvcs(String namespace, Storage storage, Set<NodeRef> nodes,
                                                             BiFunction<Integer, Integer, String> pvcNameFunction, boolean deleteClaim) {

        Map<String, PersistentVolumeClaim> pvcs = new HashMap<>();
        for (NodeRef node : nodes) {
            Integer storageId = ((PersistentClaimStorage) storage).getId();
            String pvcName = pvcNameFunction.apply(node.nodeId(), storageId);
            pvcs.put(pvcName, createPvc(namespace, pvcName, deleteClaim));
        }
        return pvcs;
    }

    private PersistentVolumeClaim createPvc(String namespace, String pvcName, boolean deleteClaim) {
        return new PersistentVolumeClaimBuilder()
                .withNewMetadata()
                    .withNamespace(namespace)
                    .withName(pvcName)
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM, String.valueOf(deleteClaim)))
                .endMetadata()
                .build();
    }
}
