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
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.NetworkPolicyOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodDisruptionBudgetOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PvcOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceAccountOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.TimeoutException;
import io.strimzi.operator.common.model.Labels;
import io.vertx.core.Future;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import java.util.HashMap;
import java.util.List;
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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class ZooKeeperEraserTest {

    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final Reconciliation RECONCILIATION = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME);
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(CLUSTER_NAME)
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

        ZookeeperCluster zkCluster = ZookeeperCluster.fromCrd(RECONCILIATION, KAFKA, VERSIONS, sharedEnvironmentProvider);

        ArgumentCaptor<String> podSetDeletionCaptor = ArgumentCaptor.forClass(String.class);
        when(mockPodSetOps.deleteAsync(any(), anyString(), podSetDeletionCaptor.capture(), anyBoolean())).thenAnswer(i -> Future.succeededFuture());

        ArgumentCaptor<String> secretDeletionCaptor = ArgumentCaptor.forClass(String.class);
        when(mockSecretOps.deleteAsync(any(), anyString(), secretDeletionCaptor.capture(), anyBoolean())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> saDeletionCaptor = ArgumentCaptor.forClass(String.class);
        when(mockSaOps.deleteAsync(any(), anyString(), saDeletionCaptor.capture(), anyBoolean())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> serviceDeletionCaptor = ArgumentCaptor.forClass(String.class);
        when(mockServiceOps.deleteAsync(any(), anyString(), serviceDeletionCaptor.capture(), anyBoolean())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> netPolicyDeletionCaptor = ArgumentCaptor.forClass(String.class);
        when(mockNetPolicyOps.deleteAsync(any(), anyString(), netPolicyDeletionCaptor.capture(), anyBoolean())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> cmDeletionCaptor = ArgumentCaptor.forClass(String.class);
        when(mockCmOps.deleteAsync(any(), anyString(), cmDeletionCaptor.capture(), anyBoolean())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> pdbDeletionCaptor = ArgumentCaptor.forClass(String.class);
        when(mockPdbOps.deleteAsync(any(), anyString(), pdbDeletionCaptor.capture(), anyBoolean())).thenReturn(Future.succeededFuture());

        // Mock the PVC Operator
        Map<String, PersistentVolumeClaim> zkPvcs = createZooPvcs(NAMESPACE, zkCluster.getStorage(), zkCluster.nodes(),
                (replica, storageId) -> VolumeUtils.DATA_VOLUME_NAME + "-" + KafkaResources.zookeeperPodName(KAFKA.getMetadata().getName(), replica),  deleteClaim(KAFKA.getSpec().getZookeeper().getStorage()));

        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        ArgumentCaptor<String> pvcDeletionCaptor = ArgumentCaptor.forClass(String.class);
        when(mockPvcOps.reconcile(any(), anyString(), pvcDeletionCaptor.capture(), pvcCaptor.capture())).thenReturn(Future.succeededFuture());
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
        ZooKeeperEraser zkEraser = new ZooKeeperEraser(
                RECONCILIATION,
                supplier
        );

        Checkpoint async = context.checkpoint();
        zkEraser.reconcile()
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    verify(mockCmOps, times(1)).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockSaOps, times(1)).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockServiceOps, times(2)).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockSecretOps, times(2)).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockNetPolicyOps, times(1)).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockPodSetOps, times(1)).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockPdbOps, times(1)).deleteAsync(any(), any(), any(), anyBoolean());

                    assertThat(netPolicyDeletionCaptor.getAllValues(), is(List.of("my-cluster-network-policy-zookeeper")));
                    assertThat(serviceDeletionCaptor.getAllValues(), is(List.of("my-cluster-zookeeper-client", "my-cluster-zookeeper-nodes")));
                    assertThat(saDeletionCaptor.getAllValues(), is(List.of("my-cluster-zookeeper")));
                    assertThat(secretDeletionCaptor.getAllValues(), is(List.of("my-cluster-zookeeper-jmx", "my-cluster-zookeeper-nodes")));
                    assertThat(podSetDeletionCaptor.getAllValues(), is(List.of("my-cluster-zookeeper")));
                    assertThat(cmDeletionCaptor.getAllValues(), is(List.of("my-cluster-zookeeper-config")));
                    assertThat(pdbDeletionCaptor.getAllValues(), is(List.of("my-cluster-zookeeper")));

                    // Check PVCs
                    verify(mockPvcOps, times(3)).getAsync(any(), any());
                    verify(mockPvcOps, times(1)).listAsync(any(), ArgumentMatchers.any(Labels.class));
                    verify(mockPvcOps, times(3)).reconcile(any(), any(), any(), any());
                    assertThat(pvcDeletionCaptor.getAllValues(), is(List.of("data-my-cluster-zookeeper-2", "data-my-cluster-zookeeper-0", "data-my-cluster-zookeeper-1")));
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

        ZookeeperCluster zkCluster = ZookeeperCluster.fromCrd(RECONCILIATION, patchedKafka, VERSIONS, sharedEnvironmentProvider);

        ArgumentCaptor<String> podSetDeletionCaptor = ArgumentCaptor.forClass(String.class);
        when(mockPodSetOps.deleteAsync(any(), anyString(), podSetDeletionCaptor.capture(), anyBoolean())).thenAnswer(i -> Future.succeededFuture());

        ArgumentCaptor<String> secretDeletionCaptor = ArgumentCaptor.forClass(String.class);
        when(mockSecretOps.deleteAsync(any(), anyString(), secretDeletionCaptor.capture(), anyBoolean())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> saDeletionCaptor = ArgumentCaptor.forClass(String.class);
        when(mockSaOps.deleteAsync(any(), anyString(), saDeletionCaptor.capture(), anyBoolean())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> serviceDeletionCaptor = ArgumentCaptor.forClass(String.class);
        when(mockServiceOps.deleteAsync(any(), anyString(), serviceDeletionCaptor.capture(), anyBoolean())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> netPolicyDeletionCaptor = ArgumentCaptor.forClass(String.class);
        when(mockNetPolicyOps.deleteAsync(any(), anyString(), netPolicyDeletionCaptor.capture(), anyBoolean())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> cmDeletionCaptor = ArgumentCaptor.forClass(String.class);
        when(mockCmOps.deleteAsync(any(), anyString(), cmDeletionCaptor.capture(), anyBoolean())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> pdbDeletionCaptor = ArgumentCaptor.forClass(String.class);
        when(mockPdbOps.deleteAsync(any(), anyString(), pdbDeletionCaptor.capture(), anyBoolean())).thenReturn(Future.succeededFuture());

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
        ZooKeeperEraser zkEraser = new ZooKeeperEraser(
                RECONCILIATION,
                supplier
        );

        Checkpoint async = context.checkpoint();
        zkEraser.reconcile()
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    verify(mockCmOps, times(1)).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockSaOps, times(1)).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockServiceOps, times(2)).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockSecretOps, times(2)).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockNetPolicyOps, times(1)).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockPodSetOps, times(1)).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockPdbOps, times(1)).deleteAsync(any(), any(), any(), anyBoolean());

                    assertThat(netPolicyDeletionCaptor.getAllValues(), is(List.of("my-cluster-network-policy-zookeeper")));
                    assertThat(serviceDeletionCaptor.getAllValues(), is(List.of("my-cluster-zookeeper-client", "my-cluster-zookeeper-nodes")));
                    assertThat(saDeletionCaptor.getAllValues(), is(List.of("my-cluster-zookeeper")));
                    assertThat(secretDeletionCaptor.getAllValues(), is(List.of("my-cluster-zookeeper-jmx", "my-cluster-zookeeper-nodes")));
                    assertThat(podSetDeletionCaptor.getAllValues(), is(List.of("my-cluster-zookeeper")));
                    assertThat(cmDeletionCaptor.getAllValues(), is(List.of("my-cluster-zookeeper-config")));
                    assertThat(pdbDeletionCaptor.getAllValues(), is(List.of("my-cluster-zookeeper")));

                    // Check PVCs
                    verify(mockPvcOps, times(3)).getAsync(any(), any());
                    verify(mockPvcOps, times(1)).listAsync(any(), ArgumentMatchers.any(Labels.class));
                    // no reconcile since there was no PVC deletion
                    verify(mockPvcOps, never()).reconcile(any(), any(), any(), any());
                    assertThat(pvcCaptor.getAllValues().size(), is(0));
                    async.flag();
                })));
    }

    @Test
    public void testZookeeperEraserReconcileFailedDueToServiceAccountDeletionTimeout(VertxTestContext context) {

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

        ZookeeperCluster zkCluster = ZookeeperCluster.fromCrd(RECONCILIATION, KAFKA, VERSIONS, sharedEnvironmentProvider);

        ArgumentCaptor<String> podSetDeletionCaptor = ArgumentCaptor.forClass(String.class);
        when(mockPodSetOps.deleteAsync(any(), anyString(), podSetDeletionCaptor.capture(), anyBoolean())).thenAnswer(i -> Future.succeededFuture());

        ArgumentCaptor<String> secretDeletionCaptor = ArgumentCaptor.forClass(String.class);
        when(mockSecretOps.deleteAsync(any(), anyString(), secretDeletionCaptor.capture(), anyBoolean())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> saDeletionCaptor = ArgumentCaptor.forClass(String.class);
        when(mockSaOps.deleteAsync(any(), anyString(), saDeletionCaptor.capture(), anyBoolean())).thenReturn(Future.failedFuture(new TimeoutException("Timed out while deleting the resource")));

        ArgumentCaptor<String> serviceDeletionCaptor = ArgumentCaptor.forClass(String.class);
        when(mockServiceOps.deleteAsync(any(), anyString(), serviceDeletionCaptor.capture(), anyBoolean())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> netPolicyDeletionCaptor = ArgumentCaptor.forClass(String.class);
        when(mockNetPolicyOps.deleteAsync(any(), anyString(), netPolicyDeletionCaptor.capture(), anyBoolean())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> cmDeletionCaptor = ArgumentCaptor.forClass(String.class);
        when(mockCmOps.deleteAsync(any(), anyString(), cmDeletionCaptor.capture(), anyBoolean())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> pdbDeletionCaptor = ArgumentCaptor.forClass(String.class);
        when(mockPdbOps.deleteAsync(any(), anyString(), pdbDeletionCaptor.capture(), anyBoolean())).thenReturn(Future.succeededFuture());

        // Mock the PVC Operator
        Map<String, PersistentVolumeClaim> zkPvcs = createZooPvcs(NAMESPACE, zkCluster.getStorage(), zkCluster.nodes(),
                (replica, storageId) -> VolumeUtils.DATA_VOLUME_NAME + "-" + KafkaResources.zookeeperPodName(KAFKA.getMetadata().getName(), replica), deleteClaim(KAFKA.getSpec().getZookeeper().getStorage()));

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
        ZooKeeperEraser zkEraser = new ZooKeeperEraser(
                RECONCILIATION,
                supplier
        );

        Checkpoint async = context.checkpoint();
        zkEraser.reconcile()
                .onComplete(context.failing(v -> context.verify(() -> {
                    verify(mockCmOps, never()).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockSaOps, times(1)).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockServiceOps, never()).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockSecretOps, times(1)).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockNetPolicyOps, times(1)).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockPodSetOps, never()).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockPdbOps, never()).deleteAsync(any(), any(), any(), anyBoolean());
                    verify(mockPvcOps, never()).getAsync(any(), any());
                    verify(mockPvcOps, never()).listAsync(any(), ArgumentMatchers.any(Labels.class));
                    // no reconcile since there was no PVC deletion
                    verify(mockPvcOps, never()).reconcile(any(), any(), any(), any());
                    assertThat(pvcCaptor.getAllValues().size(), is(0));

                    assertThat(netPolicyDeletionCaptor.getAllValues(), is(List.of("my-cluster-network-policy-zookeeper")));
                    assertThat(saDeletionCaptor.getAllValues(), is(List.of("my-cluster-zookeeper")));
                    assertThat(secretDeletionCaptor.getAllValues(), is(List.of("my-cluster-zookeeper-jmx")));

                    // asserting error message
                    assertThat(v.getMessage(), is("Timed out while deleting the resource"));
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
