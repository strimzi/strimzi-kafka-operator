/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.kafka.JbodStorage;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.KRaftMetadataStorage;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.clustersecurity.kafka.KafkaClusterSecurityContext;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.InvalidResourceException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Timeout(value = 30, unit = TimeUnit.SECONDS)
public class KafkaClusterCreatorTest {
    private final static String NAMESPACE = "my-ns";
    private final static String CLUSTER_NAME = "my-cluster";
    private final static Reconciliation RECONCILIATION = new Reconciliation("test", "kind", NAMESPACE, CLUSTER_NAME);
    private final static ClusterOperatorConfig CO_CONFIG = ResourceUtils.dummyClusterOperatorConfig();

    private final static Kafka KAFKA = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("tls")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .build())
                    .endKafka()
                .endSpec()
                .build();

    private final static KafkaNodePool POOL_A = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("pool-a")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.BROKER)
            .endSpec()
            .build();
    private final static KafkaNodePool POOL_A_WITH_STATUS = new KafkaNodePoolBuilder(POOL_A)
            .withNewStatus()
                .withRoles(ProcessRoles.BROKER)
                .withNodeIds(1000, 1001, 1002)
            .endStatus()
            .build();
    private final static KafkaNodePool POOL_A_WITH_STATUS_5_NODES = new KafkaNodePoolBuilder(POOL_A)
            .withNewStatus()
                .withRoles(ProcessRoles.BROKER)
                .withNodeIds(1000, 1001, 1002, 1003, 1004)
            .endStatus()
            .build();

    private final static KafkaNodePool POOL_B = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("pool-b")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("200Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.BROKER)
            .endSpec()
            .build();
    private final static KafkaNodePool POOL_B_WITH_STATUS = new KafkaNodePoolBuilder(POOL_B)
            .withNewStatus()
                .withRoles(ProcessRoles.BROKER)
                .withNodeIds(2000, 2001, 2002)
            .endStatus()
            .build();
    private final static KafkaNodePool POOL_B_WITH_STATUS_5_NODES = new KafkaNodePoolBuilder(POOL_B)
            .withNewStatus()
                .withRoles(ProcessRoles.BROKER)
                .withNodeIds(2000, 2001, 2002, 2003, 2004)
            .endStatus()
            .build();

    private final static KafkaNodePool POOL_MIXED = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("pool-mixed")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("200Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.CONTROLLER, ProcessRoles.BROKER)
            .endSpec()
            .build();
    private final static KafkaNodePool POOL_MIXED_WITH_STATUS = new KafkaNodePoolBuilder(POOL_MIXED)
            .withNewStatus()
                .withRoles(ProcessRoles.CONTROLLER, ProcessRoles.BROKER)
                .withNodeIds(3000, 3001, 3002)
            .endStatus()
            .build();
    private final static KafkaNodePool POOL_MIXED_WITH_STATUS_5_NODES = new KafkaNodePoolBuilder(POOL_MIXED)
            .withNewStatus()
                .withRoles(ProcessRoles.CONTROLLER, ProcessRoles.BROKER)
                .withNodeIds(3000, 3001, 3002, 3003, 3004)
            .endStatus()
            .build();
    private final static KafkaNodePool POOL_MIXED_NOT_MIXED_ANYMORE = new KafkaNodePoolBuilder(POOL_MIXED_WITH_STATUS)
            .editSpec()
                .removeFromRoles(ProcessRoles.BROKER)
            .endSpec()
            .build();

    private final static KafkaNodePool POOL_CONTROLLERS = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("pool-controllers")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("200Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.CONTROLLER)
            .endSpec()
            .build();
    private final static KafkaNodePool POOL_CONTROLLERS_WITH_STATUS = new KafkaNodePoolBuilder(POOL_CONTROLLERS)
            .withNewStatus()
                .withRoles(ProcessRoles.CONTROLLER)
                .withNodeIds(3000, 3001, 3002)
            .endStatus()
            .build();
    private final static KafkaNodePool POOL_CONTROLLERS_WITH_STATUS_5_NODES = new KafkaNodePoolBuilder(POOL_CONTROLLERS)
            .withNewStatus()
                .withRoles(ProcessRoles.CONTROLLER)
                .withNodeIds(3000, 3001, 3002, 3003, 3004)
            .endStatus()
            .build();

    // A pool which keeps both volumes, used when only another pool removes one
    private final static KafkaNodePool POOL_A_WITH_STATUS_AND_BOTH_VOLUMES = new KafkaNodePoolBuilder(POOL_A_WITH_STATUS)
            .editSpec()
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build(),
                            new PersistentClaimStorageBuilder().withId(1).withSize("100Gi").build())
                .endJbodStorage()
            .endSpec()
            .build();

    // Both pools run on two volumes and both drop volume 1, but only pool-a is blocked
    private final static Map<String, Storage> OLD_STORAGE_TWO_VOLUMES_IN_BOTH_POOLS = Map.of(
            "my-cluster-pool-a",
            new JbodStorageBuilder()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build(),
                            new PersistentClaimStorageBuilder().withId(1).withSize("100Gi").build())
                    .build(),
            "my-cluster-pool-b",
            new JbodStorageBuilder()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("200Gi").build(),
                            new PersistentClaimStorageBuilder().withId(1).withSize("200Gi").build())
                    .build());

    // pool-a runs with the KRaft metadata log on volume 1, which is the volume it tries to remove
    private final static Map<String, Storage> OLD_STORAGE_WITH_KRAFT_METADATA_ON_VOLUME_1 = Map.of(
            "my-cluster-pool-a",
            new JbodStorageBuilder()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build(),
                            new PersistentClaimStorageBuilder().withId(1).withSize("100Gi").withKraftMetadata(KRaftMetadataStorage.SHARED).build())
                    .build());

    // Storage the mixed-role, controller-only and pool-a pools run on, so that a volume removal can be requested from them
    private final static Map<String, Storage> OLD_STORAGE_MIXED_AND_CONTROLLERS = Map.of(
            "my-cluster-pool-mixed",
            new JbodStorageBuilder()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("200Gi").build(),
                            new PersistentClaimStorageBuilder().withId(1).withSize("200Gi").build())
                    .build(),
            "my-cluster-pool-controllers",
            new JbodStorageBuilder()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("200Gi").build(),
                            new PersistentClaimStorageBuilder().withId(1).withSize("200Gi").build())
                    .build(),
            "my-cluster-pool-a",
            new JbodStorageBuilder()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build(),
                            new PersistentClaimStorageBuilder().withId(1).withSize("100Gi").build())
                    .build());

    // Storage the pools currently run on. pool-a runs on two volumes, pool-b on one.
    private final static Map<String, Storage> OLD_STORAGE_TWO_VOLUMES_IN_POOL_A = Map.of(
            "my-cluster-pool-a",
            new JbodStorageBuilder()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build(),
                            new PersistentClaimStorageBuilder().withId(1).withSize("100Gi").build())
                    .build(),
            "my-cluster-pool-b",
            new JbodStorageBuilder()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("200Gi").build())
                    .build());

    private final static KafkaNodePool POOL_A_WITH_STATUS_2_NODES = new KafkaNodePoolBuilder(POOL_A)
            .withNewStatus()
                .withRoles(ProcessRoles.BROKER)
                .withNodeIds(1000, 1001)
            .endStatus()
            .build();

    // pool-a drops volume 1 and grows volume 0. Both changes are reverted together.
    private final static KafkaNodePool POOL_A_WITH_STATUS_AND_BIGGER_VOLUME_0 = new KafkaNodePoolBuilder(POOL_A_WITH_STATUS)
            .editSpec()
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("150Gi").build())
                .endJbodStorage()
            .endSpec()
            .build();

    private final static KafkaNodePool POOL_B_WITH_STATUS_AND_BIGGER_VOLUME = new KafkaNodePoolBuilder(POOL_B_WITH_STATUS)
            .editSpec()
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("300Gi").build())
                .endJbodStorage()
            .endSpec()
            .build();

    //////////////////////////////////////////////////
    // KRaft tests
    //////////////////////////////////////////////////

    @Test
    public void testNewClusterWithKRaft() throws Exception {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        KafkaCluster kc = creator.prepareKafkaCluster(KAFKA, List.of(POOL_CONTROLLERS, POOL_A, POOL_B), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, true, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                .toCompletableFuture()
                .join();

        // Kafka cluster is created
        assertThat(kc, is(notNullValue()));
        assertThat(kc.nodes().size(), is(9));
        assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(0, 1, 2, 3, 4, 5, 6, 7, 8)));
        assertThat(kc.removedNodes(), is(Set.of()));

        // Check the status conditions
        assertThat(kafkaStatus.getConditions(), is(nullValue()));

        // No scale-down => scale-down check is not done
        verify(supplier.brokersInUseCheck, never()).brokersInUse(any(), any(), any());
    }

    @Test
    public void testNewClusterWithMixedNodesKRaft() throws Exception {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        KafkaCluster kc = creator.prepareKafkaCluster(KAFKA, List.of(POOL_MIXED), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, true, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                .toCompletableFuture()
                .join();

        // Kafka cluster is created
        assertThat(kc, is(notNullValue()));
        assertThat(kc.nodes().size(), is(3));
        assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(0, 1, 2)));
        assertThat(kc.removedNodes(), is(Set.of()));

        // Check the status conditions
        assertThat(kafkaStatus.getConditions(), is(nullValue()));

        // No scale-down => scale-down check is not done
        verify(supplier.brokersInUseCheck, never()).brokersInUse(any(), any(), any());
    }

    @Test
    public void testExistingClusterWithKRaft() throws Exception {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        KafkaCluster kc = creator.prepareKafkaCluster(KAFKA, List.of(POOL_CONTROLLERS_WITH_STATUS, POOL_A_WITH_STATUS, POOL_B_WITH_STATUS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, true, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                .toCompletableFuture()
                .join();

        // Kafka cluster is created
        assertThat(kc, is(notNullValue()));
        assertThat(kc.nodes().size(), is(9));
        assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(1000, 1001, 1002, 2000, 2001, 2002, 3000, 3001, 3002)));
        assertThat(kc.removedNodes(), is(Set.of()));

        // Check the status conditions
        assertThat(kafkaStatus.getConditions(), is(nullValue()));

        // No scale-down => scale-down check is not done
        verify(supplier.brokersInUseCheck, never()).brokersInUse(any(), any(), any());
    }

    @Test
    public void testExistingClusterWithMixedNodesKRaft() throws Exception {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        KafkaCluster kc = creator.prepareKafkaCluster(KAFKA, List.of(POOL_MIXED_WITH_STATUS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, true, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                .toCompletableFuture()
                .join();

        // Kafka cluster is created
        assertThat(kc, is(notNullValue()));
        assertThat(kc.nodes().size(), is(3));
        assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(3000, 3001, 3002)));
        assertThat(kc.removedNodes(), is(Set.of()));

        // Check the status conditions
        assertThat(kafkaStatus.getConditions(), is(nullValue()));

        // No scale-down => scale-down check is not done
        verify(supplier.brokersInUseCheck, never()).brokersInUse(any(), any(), any());
    }

    @Test
    public void testRevertScaleDownWithKRaft() throws Exception {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock brokers-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.brokersInUse(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(Set.of(1000, 1001, 1002, 1003, 2004)));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        KafkaCluster kc = creator.prepareKafkaCluster(KAFKA, List.of(POOL_CONTROLLERS_WITH_STATUS_5_NODES, POOL_A_WITH_STATUS_5_NODES, POOL_B_WITH_STATUS_5_NODES), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, true, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                .toCompletableFuture()
                .join();

        // Kafka cluster is created
        assertThat(kc, is(notNullValue()));
        assertThat(kc.nodes().size(), is(13));
        assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(1000, 1001, 1002, 1003, 1004, 2000, 2001, 2002, 2003, 2004, 3000, 3001, 3002)));
        assertThat(kc.removedNodes(), is(Set.of(3003, 3004))); // Controllers are not affected

        // Check the status conditions
        assertThat(kafkaStatus.getConditions().size(), is(2));
        assertThat(kafkaStatus.getConditions().get(0).getStatus(), is("True"));
        assertThat(kafkaStatus.getConditions().get(0).getType(), is("Warning"));
        assertThat(kafkaStatus.getConditions().get(0).getReason(), is("ScaleDownPreventionCheck"));
        assertThat(kafkaStatus.getConditions().get(0).getMessage(), is("Reverting scale-down of KafkaNodePool pool-a by changing number of replicas to 5"));
        assertThat(kafkaStatus.getConditions().get(1).getStatus(), is("True"));
        assertThat(kafkaStatus.getConditions().get(1).getType(), is("Warning"));
        assertThat(kafkaStatus.getConditions().get(1).getReason(), is("ScaleDownPreventionCheck"));
        assertThat(kafkaStatus.getConditions().get(1).getMessage(), is("Reverting scale-down of KafkaNodePool pool-b by changing number of replicas to 5"));

        // Scale-down reverted => should be called twice as we still scale down controllers after the revert is done
        verify(supplier.brokersInUseCheck, times(2)).brokersInUse(any(), any(), any());
    }

    @Test
    public void testRevertScaleDownWithKRaftMixedNodes() throws Exception {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock brokers-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.brokersInUse(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(Set.of(3000, 3001, 3002, 3003)));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        KafkaCluster kc = creator.prepareKafkaCluster(KAFKA, List.of(POOL_MIXED_WITH_STATUS_5_NODES), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, true, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                .toCompletableFuture()
                .join();

        // Kafka cluster is created
        assertThat(kc, is(notNullValue()));
        assertThat(kc.nodes().size(), is(5));
        assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(3000, 3001, 3002, 3003, 3004)));
        assertThat(kc.removedNodes(), is(Set.of()));

        // Check the status conditions
        assertThat(kafkaStatus.getConditions().size(), is(1));
        assertThat(kafkaStatus.getConditions().get(0).getStatus(), is("True"));
        assertThat(kafkaStatus.getConditions().get(0).getType(), is("Warning"));
        assertThat(kafkaStatus.getConditions().get(0).getReason(), is("ScaleDownPreventionCheck"));
        assertThat(kafkaStatus.getConditions().get(0).getMessage(), is("Reverting scale-down of KafkaNodePool pool-mixed by changing number of replicas to 5"));

        // Scale-down reverted => should be called twice as we still scale down controllers after the revert is done
        verify(supplier.brokersInUseCheck, times(1)).brokersInUse(any(), any(), any());
    }

    @Test
    public void testCorrectScaleDownWithKRaft() throws Exception {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock brokers-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.brokersInUse(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(Set.of(1000, 1001, 1002, 2000, 2001, 2002, 3000, 3001, 3002)));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        KafkaCluster kc = creator.prepareKafkaCluster(KAFKA, List.of(POOL_CONTROLLERS_WITH_STATUS_5_NODES, POOL_A_WITH_STATUS_5_NODES, POOL_B_WITH_STATUS_5_NODES), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, true, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                .toCompletableFuture()
                .join();

        // Kafka cluster is created
        assertThat(kc, is(notNullValue()));
        assertThat(kc.nodes().size(), is(9));
        assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(1000, 1001, 1002, 2000, 2001, 2002, 3000, 3001, 3002)));
        assertThat(kc.removedNodes(), is(Set.of(1003, 1004, 2003, 2004, 3003, 3004)));

        // Check the status conditions
        assertThat(kafkaStatus.getConditions(), is(nullValue()));

        // Scale-down reverted => should be called once
        verify(supplier.brokersInUseCheck, times(1)).brokersInUse(any(), any(), any());
    }

    @Test
    public void testThrowsRevertScaleDownFailsWithKRaft() throws Exception {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock brokers-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.brokersInUse(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(Set.of(1003, 1004, 2003, 2004)));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        Exception e = assertThrows(Exception.class, () ->
                creator.prepareKafkaCluster(KAFKA, List.of(POOL_CONTROLLERS_WITH_STATUS_5_NODES, POOL_A_WITH_STATUS_5_NODES, POOL_B_WITH_STATUS_5_NODES), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, false, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                        .toCompletableFuture()
                        .join());

        // CompletionStage wraps exceptions in CompletionException
        Throwable cause = e.getCause();
        assertThat(cause, instanceOf(InvalidResourceException.class));
        assertThat(cause.getMessage(), is("Following errors were found when processing the Kafka custom resource: [Cannot scale-down Kafka brokers [3003, 3004, 1003, 1004, 2003, 2004] because they have assigned partition-replicas.]"));

        // Check the status conditions
        assertThat(kafkaStatus.getConditions(), is(nullValue()));

        // Scale-down failed => should be called once
        verify(supplier.brokersInUseCheck, times(1)).brokersInUse(any(), any(), any());
    }

    @Test
    public void testSkipScaleDownCheckWithKRaft() throws Exception {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_SKIP_BROKER_SCALEDOWN_CHECK, "true")
                .endMetadata()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        KafkaCluster kc = creator.prepareKafkaCluster(kafka, List.of(POOL_CONTROLLERS_WITH_STATUS_5_NODES, POOL_A_WITH_STATUS_5_NODES, POOL_B_WITH_STATUS_5_NODES), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, false, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                .toCompletableFuture()
                .join();

        // Kafka cluster is created
        assertThat(kc, is(notNullValue()));
        assertThat(kc.nodes().size(), is(9));
        assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(1000, 1001, 1002, 2000, 2001, 2002, 3000, 3001, 3002)));
        assertThat(kc.removedNodes(), is(Set.of(1003, 1004, 2003, 2004, 3003, 3004)));

        // Check the status conditions
        assertThat(kafkaStatus.getConditions(), is(nullValue()));

        // Scale-down check skipped => should be never called
        verify(supplier.brokersInUseCheck, never()).brokersInUse(any(), any(), any());
    }

    @Test
    public void testRevertRoleChangeWithKRaftMixedNodes() throws Exception {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock brokers-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.brokersInUse(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(Set.of(1000, 1001, 1002, 2000, 2001, 2002, 3000, 3001, 3002)));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        KafkaCluster kc = creator.prepareKafkaCluster(KAFKA, List.of(POOL_MIXED_NOT_MIXED_ANYMORE, POOL_A_WITH_STATUS, POOL_B_WITH_STATUS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, true, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                .toCompletableFuture()
                .join();

        // Kafka cluster is created
        assertThat(kc, is(notNullValue()));
        assertThat(kc.nodes().size(), is(9));
        assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(1000, 1001, 1002, 2000, 2001, 2002, 3000, 3001, 3002)));
        assertThat(kc.brokerNodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(1000, 1001, 1002, 2000, 2001, 2002, 3000, 3001, 3002)));
        assertThat(kc.controllerNodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(3000, 3001, 3002)));
        assertThat(kc.removedNodes(), is(Set.of()));
        assertThat(kc.usedToBeBrokerNodes(), is(Set.of()));

        // Check the status conditions
        assertThat(kafkaStatus.getConditions().size(), is(1));
        assertThat(kafkaStatus.getConditions().get(0).getStatus(), is("True"));
        assertThat(kafkaStatus.getConditions().get(0).getType(), is("Warning"));
        assertThat(kafkaStatus.getConditions().get(0).getReason(), is("ScaleDownPreventionCheck"));
        assertThat(kafkaStatus.getConditions().get(0).getMessage(), is("Reverting role change of KafkaNodePool pool-mixed (setting roles to [CONTROLLER, BROKER])"));

        // Scale-down reverted => should be called twice as we still scale down controllers after the revert is done
        verify(supplier.brokersInUseCheck, times(1)).brokersInUse(any(), any(), any());
    }

    @Test
    public void testRevertRoleChangeWithKRaftDedicatedNodes() throws Exception {
        KafkaNodePool poolBFromBrokerToControllerOnly = new KafkaNodePoolBuilder(POOL_B_WITH_STATUS)
                .editSpec()
                    .withRoles(ProcessRoles.CONTROLLER)
                .endSpec()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock brokers-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.brokersInUse(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(Set.of(1000, 1001, 1002, 2000, 2001, 2002, 3000, 3001, 3002)));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        KafkaCluster kc = creator.prepareKafkaCluster(KAFKA, List.of(POOL_MIXED_WITH_STATUS, POOL_A_WITH_STATUS, poolBFromBrokerToControllerOnly), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, true, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                .toCompletableFuture()
                .join();

        // Kafka cluster is created
        assertThat(kc, is(notNullValue()));
        assertThat(kc.nodes().size(), is(9));
        assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(1000, 1001, 1002, 2000, 2001, 2002, 3000, 3001, 3002)));
        assertThat(kc.brokerNodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(1000, 1001, 1002, 2000, 2001, 2002, 3000, 3001, 3002)));
        assertThat(kc.controllerNodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(3000, 3001, 3002)));
        assertThat(kc.removedNodes(), is(Set.of()));
        assertThat(kc.usedToBeBrokerNodes(), is(Set.of()));

        // Check the status conditions
        assertThat(kafkaStatus.getConditions().size(), is(1));
        assertThat(kafkaStatus.getConditions().get(0).getStatus(), is("True"));
        assertThat(kafkaStatus.getConditions().get(0).getType(), is("Warning"));
        assertThat(kafkaStatus.getConditions().get(0).getReason(), is("ScaleDownPreventionCheck"));
        assertThat(kafkaStatus.getConditions().get(0).getMessage(), is("Reverting role change of KafkaNodePool pool-b (setting roles to [BROKER])"));

        // Scale-down reverted => should be called twice as we still scale down controllers after the revert is done
        verify(supplier.brokersInUseCheck, times(1)).brokersInUse(any(), any(), any());
    }

    @Test
    public void testCorrectRoleChangeWithKRaft() throws Exception {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock brokers-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.brokersInUse(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(Set.of(1000, 1001, 1002, 2000, 2001, 20022)));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        KafkaCluster kc = creator.prepareKafkaCluster(KAFKA, List.of(POOL_MIXED_NOT_MIXED_ANYMORE, POOL_A_WITH_STATUS, POOL_B_WITH_STATUS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, true, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                .toCompletableFuture()
                .join();

        // Kafka cluster is created
        assertThat(kc, is(notNullValue()));
        assertThat(kc.nodes().size(), is(9));
        assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(1000, 1001, 1002, 2000, 2001, 2002, 3000, 3001, 3002)));
        assertThat(kc.removedNodes(), is(Set.of()));
        assertThat(kc.usedToBeBrokerNodes(), is(Set.of(3000, 3001, 3002)));

        // Check the status conditions
        assertThat(kafkaStatus.getConditions(), is(nullValue()));

        // Scale-down reverted => should be called once
        verify(supplier.brokersInUseCheck, times(1)).brokersInUse(any(), any(), any());
    }

    @Test
    public void testThrowsRevertBrokerChangeFailsWithKRaft() throws Exception {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock brokers-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.brokersInUse(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(Set.of(3000, 3002)));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        Exception e = assertThrows(Exception.class, () ->
                creator.prepareKafkaCluster(KAFKA, List.of(POOL_MIXED_NOT_MIXED_ANYMORE, POOL_A_WITH_STATUS, POOL_B_WITH_STATUS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, false, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                        .toCompletableFuture()
                        .join());

        // CompletionStage wraps exceptions in CompletionException
        Throwable cause = e.getCause();
        assertThat(cause, instanceOf(InvalidResourceException.class));
        assertThat(cause.getMessage(), is("Following errors were found when processing the Kafka custom resource: [Cannot remove the broker role from nodes [3000, 3001, 3002] because they have assigned partition-replicas.]"));

        // Check the status conditions
        assertThat(kafkaStatus.getConditions(), is(nullValue()));

        // Scale-down failed => should be called once
        verify(supplier.brokersInUseCheck, times(1)).brokersInUse(any(), any(), any());
    }

    @Test
    public void testSkipRoleChangeCheckWithKRaft() throws Exception {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_SKIP_BROKER_SCALEDOWN_CHECK, "true")
                .endMetadata()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        KafkaCluster kc = creator.prepareKafkaCluster(kafka, List.of(POOL_MIXED_NOT_MIXED_ANYMORE, POOL_A_WITH_STATUS, POOL_B_WITH_STATUS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, false, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                .toCompletableFuture()
                .join();

        // Kafka cluster is created
        assertThat(kc, is(notNullValue()));
        assertThat(kc.nodes().size(), is(9));
        assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(1000, 1001, 1002, 2000, 2001, 2002, 3000, 3001, 3002)));
        assertThat(kc.removedNodes(), is(Set.of()));
        assertThat(kc.usedToBeBrokerNodes(), is(Set.of(3000, 3001, 3002)));

        // Check the status conditions
        assertThat(kafkaStatus.getConditions(), is(nullValue()));

        // Scale-down check skipped => should be never called
        verify(supplier.brokersInUseCheck, never()).brokersInUse(any(), any(), any());
    }

    @Test
    public void testRevertVolumeRemovalWithKRaft() throws Exception {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock volumes-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.volumesInUse(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(new BrokersInUseCheck.VolumesInUse(Map.of(1000, Set.of(1)), Map.of())));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        KafkaCluster kc = creator.prepareKafkaCluster(KAFKA, List.of(POOL_CONTROLLERS_WITH_STATUS, POOL_A_WITH_STATUS_AND_BIGGER_VOLUME_0, POOL_B_WITH_STATUS_AND_BIGGER_VOLUME), OLD_STORAGE_TWO_VOLUMES_IN_POOL_A, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, true, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                .toCompletableFuture()
                .join();

        // Kafka cluster is created and the volume is still there
        assertThat(kc, is(notNullValue()));
        assertThat(kc.removedJbodVolumes(), is(Map.of()));

        // The whole storage of pool-a goes back to what the cluster runs on
        assertThat(kc.getStorageByPoolName().get("pool-a"), is(OLD_STORAGE_TWO_VOLUMES_IN_POOL_A.get("my-cluster-pool-a")));
        // pool-b removes no volume, so its own storage change is kept
        assertThat(kc.getStorageByPoolName().get("pool-b"), is(POOL_B_WITH_STATUS_AND_BIGGER_VOLUME.getSpec().getStorage()));

        // Check the status conditions
        assertThat(kafkaStatus.getConditions().size(), is(1));
        assertThat(kafkaStatus.getConditions().get(0).getStatus(), is("True"));
        assertThat(kafkaStatus.getConditions().get(0).getType(), is("Warning"));
        assertThat(kafkaStatus.getConditions().get(0).getReason(), is("ScaleDownPreventionCheck"));
        assertThat(kafkaStatus.getConditions().get(0).getMessage(), is("Reverting all storage changes of KafkaNodePool pool-a because they remove JBOD volumes which are not empty"));

        // Volume removal reverted => the check runs once, and is not needed after the revert
        verify(supplier.brokersInUseCheck, times(1)).volumesInUse(any(), any(), any(), any());
    }

    @Test
    public void testCorrectVolumeRemovalWithKRaft() throws Exception {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock volumes-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.volumesInUse(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(new BrokersInUseCheck.VolumesInUse(Map.of(), Map.of())));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        KafkaCluster kc = creator.prepareKafkaCluster(KAFKA, List.of(POOL_CONTROLLERS_WITH_STATUS, POOL_A_WITH_STATUS, POOL_B_WITH_STATUS_AND_BIGGER_VOLUME), OLD_STORAGE_TWO_VOLUMES_IN_POOL_A, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, true, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                .toCompletableFuture()
                .join();

        // Kafka cluster is created and the volume is removed
        assertThat(kc, is(notNullValue()));
        assertThat(kc.removedJbodVolumes(), is(Map.of(1000, Set.of(1), 1001, Set.of(1), 1002, Set.of(1))));
        assertThat(kc.getStorageByPoolName().get("pool-a"), is(POOL_A.getSpec().getStorage()));
        assertThat(kc.getStorageByPoolName().get("pool-b"), is(POOL_B_WITH_STATUS_AND_BIGGER_VOLUME.getSpec().getStorage()));

        // Check the status conditions
        assertThat(kafkaStatus.getConditions(), is(nullValue()));

        // Nothing was reverted => the check runs once
        verify(supplier.brokersInUseCheck, times(1)).volumesInUse(any(), any(), any(), any());
    }

    @Test
    public void testThrowsRevertVolumeRemovalFailsWithKRaft() throws Exception {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock volumes-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.volumesInUse(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(new BrokersInUseCheck.VolumesInUse(Map.of(1000, Set.of(1)), Map.of())));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        Exception e = assertThrows(Exception.class, () ->
                creator.prepareKafkaCluster(KAFKA, List.of(POOL_CONTROLLERS_WITH_STATUS, POOL_A_WITH_STATUS, POOL_B_WITH_STATUS_AND_BIGGER_VOLUME), OLD_STORAGE_TWO_VOLUMES_IN_POOL_A, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, false, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                        .toCompletableFuture()
                        .join());

        // CompletionStage wraps exceptions in CompletionException
        Throwable cause = e.getCause();
        assertThat(cause, instanceOf(InvalidResourceException.class));
        assertThat(cause.getMessage(), is("Following errors were found when processing the Kafka custom resource: [Cannot remove the JBOD volumes [1] from Kafka brokers [1000] because they have assigned partition-replicas.]"));

        // Check the status conditions
        assertThat(kafkaStatus.getConditions(), is(nullValue()));

        // Volume removal failed => should be called once
        verify(supplier.brokersInUseCheck, times(1)).volumesInUse(any(), any(), any(), any());
    }

    @Test
    public void testSkipVolumeRemovalCheckWithKRaft() throws Exception {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_SKIP_BROKER_SCALEDOWN_CHECK, "true")
                .endMetadata()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        KafkaCluster kc = creator.prepareKafkaCluster(kafka, List.of(POOL_CONTROLLERS_WITH_STATUS, POOL_A_WITH_STATUS, POOL_B_WITH_STATUS_AND_BIGGER_VOLUME), OLD_STORAGE_TWO_VOLUMES_IN_POOL_A, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, false, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                .toCompletableFuture()
                .join();

        // Kafka cluster is created and the volume is removed without asking Kafka
        assertThat(kc, is(notNullValue()));
        assertThat(kc.getStorageByPoolName().get("pool-a"), is(POOL_A.getSpec().getStorage()));

        // Check the status conditions
        assertThat(kafkaStatus.getConditions(), is(nullValue()));

        // Volume removal check skipped => should be never called
        verify(supplier.brokersInUseCheck, never()).volumesInUse(any(), any(), any(), any());
    }

    @Test
    public void testVolumeRemovalIgnoresNodesWhichDoNotExistYetWithKRaft() throws Exception {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock volumes-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.volumesInUse(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(new BrokersInUseCheck.VolumesInUse(Map.of(), Map.of())));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        KafkaCluster kc = creator.prepareKafkaCluster(KAFKA, List.of(POOL_CONTROLLERS_WITH_STATUS, POOL_A_WITH_STATUS_2_NODES), OLD_STORAGE_TWO_VOLUMES_IN_POOL_A, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, true, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                .toCompletableFuture()
                .join();

        // Node 1002 is being added, so it has no data and Kafka cannot be asked about it
        assertThat(kc, is(notNullValue()));
        assertThat(kc.removedJbodVolumes(), is(Map.of(1000, Set.of(1), 1001, Set.of(1))));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<Integer, Set<Integer>>> removedVolumesCaptor = ArgumentCaptor.forClass(Map.class);
        verify(supplier.brokersInUseCheck, times(1)).volumesInUse(any(), any(), any(), removedVolumesCaptor.capture());
        assertThat(removedVolumesCaptor.getValue(), is(Map.of(1000, Set.of(1), 1001, Set.of(1))));
    }

    @Test
    public void testBlockedVolumeRemovalDoesNotBlockAnAllowedScaleDownWithKRaft() throws Exception {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock brokers-in-use check => the brokers being scaled down are empty
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.brokersInUse(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(Set.of()));

        // Mock volumes-in-use check
        when(brokersInUseOps.volumesInUse(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(new BrokersInUseCheck.VolumesInUse(Map.of(1000, Set.of(1)), Map.of())));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        creator.prepareKafkaCluster(KAFKA, List.of(POOL_CONTROLLERS_WITH_STATUS, POOL_A_WITH_STATUS, POOL_B_WITH_STATUS_5_NODES), OLD_STORAGE_TWO_VOLUMES_IN_POOL_A, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, true, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                .toCompletableFuture()
                .join();

        // pool-b scales down normally, so its nodes must not be cordoned because of the volume problem in pool-a
        assertThat(creator.scalingDownBlockedNodes(), is(Set.of()));
    }

    @Test
    public void testRevertVolumeRemovalWithKRaftMixedNodes() throws Exception {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock volumes-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.volumesInUse(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(new BrokersInUseCheck.VolumesInUse(Map.of(3000, Set.of(1)), Map.of())));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        KafkaCluster kc = creator.prepareKafkaCluster(KAFKA, List.of(POOL_MIXED_WITH_STATUS), OLD_STORAGE_MIXED_AND_CONTROLLERS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, true, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                .toCompletableFuture()
                .join();

        // A mixed-role node holds partition replicas, so it is checked like any other broker
        assertThat(kc, is(notNullValue()));
        assertThat(kc.getStorageByPoolName().get("pool-mixed"), is(OLD_STORAGE_MIXED_AND_CONTROLLERS.get("my-cluster-pool-mixed")));
        assertThat(kafkaStatus.getConditions().size(), is(1));
        assertThat(kafkaStatus.getConditions().get(0).getMessage(), is("Reverting all storage changes of KafkaNodePool pool-mixed because they remove JBOD volumes which are not empty"));

        verify(supplier.brokersInUseCheck, times(1)).volumesInUse(any(), any(), any(), any());
    }

    @Test
    public void testVolumeRemovalFromControllerOnlyPoolIsNotCheckedWithKRaft() throws Exception {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        KafkaCluster kc = creator.prepareKafkaCluster(KAFKA, List.of(POOL_CONTROLLERS_WITH_STATUS, POOL_A_WITH_STATUS_AND_BOTH_VOLUMES), OLD_STORAGE_MIXED_AND_CONTROLLERS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, true, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                .toCompletableFuture()
                .join();

        // Controllers hold no partition replicas, so their volumes are not checked and the removal is allowed
        assertThat(kc, is(notNullValue()));
        assertThat(kc.removedJbodVolumes(), is(Map.of()));
        assertThat(kafkaStatus.getConditions(), is(nullValue()));

        verify(supplier.brokersInUseCheck, never()).volumesInUse(any(), any(), any(), any());
    }

    @Test
    public void testRevertVolumeRemovalOfOnePoolOnlyWithKRaft() throws Exception {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock volumes-in-use check => only pool-a is blocked. After pool-a is reverted, only pool-b is left to check.
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.volumesInUse(any(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(new BrokersInUseCheck.VolumesInUse(Map.of(1000, Set.of(1)), Map.of())))
                .thenReturn(CompletableFuture.completedFuture(new BrokersInUseCheck.VolumesInUse(Map.of(), Map.of())));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        KafkaCluster kc = creator.prepareKafkaCluster(KAFKA, List.of(POOL_CONTROLLERS_WITH_STATUS, POOL_A_WITH_STATUS, POOL_B_WITH_STATUS), OLD_STORAGE_TWO_VOLUMES_IN_BOTH_POOLS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, true, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                .toCompletableFuture()
                .join();

        assertThat(kc, is(notNullValue()));
        // pool-a is blocked and put back
        assertThat(kc.getStorageByPoolName().get("pool-a"), is(OLD_STORAGE_TWO_VOLUMES_IN_BOTH_POOLS.get("my-cluster-pool-a")));
        // pool-b removes an empty volume, so its removal goes ahead
        assertThat(kc.getStorageByPoolName().get("pool-b"), is(POOL_B.getSpec().getStorage()));

        assertThat(kafkaStatus.getConditions().size(), is(1));
        assertThat(kafkaStatus.getConditions().get(0).getMessage(), is("Reverting all storage changes of KafkaNodePool pool-a because they remove JBOD volumes which are not empty"));

        // pool-a is reverted, so the removal left in pool-b is checked a second time
        verify(supplier.brokersInUseCheck, times(2)).volumesInUse(any(), any(), any(), any());
    }

    @Test
    public void testRevertVolumeRemovalKeepsKRaftMetadataMarkWithKRaft() throws Exception {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock volumes-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.volumesInUse(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(new BrokersInUseCheck.VolumesInUse(Map.of(1000, Set.of(1)), Map.of())));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        KafkaCluster kc = creator.prepareKafkaCluster(KAFKA, List.of(POOL_CONTROLLERS_WITH_STATUS, POOL_A_WITH_STATUS), OLD_STORAGE_WITH_KRAFT_METADATA_ON_VOLUME_1, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, true, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                .toCompletableFuture()
                .join();

        // The refused change must not move the KRaft metadata log, so the mark stays on volume 1
        assertThat(kc, is(notNullValue()));
        assertThat(kc.getStorageByPoolName().get("pool-a"), is(OLD_STORAGE_WITH_KRAFT_METADATA_ON_VOLUME_1.get("my-cluster-pool-a")));
        assertThat(((JbodStorage) kc.getStorageByPoolName().get("pool-a")).getVolumes().get(1).getKraftMetadata(), is(KRaftMetadataStorage.SHARED));
    }

    @Test
    public void testRevertVolumeRemovalWhenBrokerDidNotAnswerWithKRaft() throws Exception {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock volumes-in-use check => the volume is empty, but the broker could not be reached
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.volumesInUse(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(new BrokersInUseCheck.VolumesInUse(Map.of(), Map.of(1000, Set.of(1)))));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        KafkaCluster kc = creator.prepareKafkaCluster(KAFKA, List.of(POOL_CONTROLLERS_WITH_STATUS, POOL_A_WITH_STATUS), OLD_STORAGE_TWO_VOLUMES_IN_POOL_A, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, true, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                .toCompletableFuture()
                .join();

        // The removal is still blocked, but the user is not told the volume holds replicas
        assertThat(kc, is(notNullValue()));
        assertThat(kc.getStorageByPoolName().get("pool-a"), is(OLD_STORAGE_TWO_VOLUMES_IN_POOL_A.get("my-cluster-pool-a")));
        assertThat(kafkaStatus.getConditions().size(), is(1));
        assertThat(kafkaStatus.getConditions().get(0).getMessage(), is("Reverting all storage changes of KafkaNodePool pool-a because they remove JBOD volumes which could not be checked, because a broker did not answer or a log directory is offline"));
    }

    @Test
    public void testRevertVolumeRemovalReportsBothReasonsWithKRaft() throws Exception {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock volumes-in-use check => one node of the pool has replicas, another node could not be reached
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.volumesInUse(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(new BrokersInUseCheck.VolumesInUse(Map.of(1000, Set.of(1)), Map.of(1001, Set.of(1)))));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        creator.prepareKafkaCluster(KAFKA, List.of(POOL_CONTROLLERS_WITH_STATUS, POOL_A_WITH_STATUS), OLD_STORAGE_TWO_VOLUMES_IN_POOL_A, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, true, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                .toCompletableFuture()
                .join();

        // The message must not change its meaning once the user fixes only one of the two reasons
        assertThat(kafkaStatus.getConditions().size(), is(1));
        assertThat(kafkaStatus.getConditions().get(0).getMessage(), is("Reverting all storage changes of KafkaNodePool pool-a because they remove JBOD volumes which are not empty or could not be checked, because a broker did not answer or a log directory is offline"));
    }

    @Test
    public void testThrowsWhenBrokerDidNotAnswerWithKRaft() throws Exception {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.volumesInUse(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(new BrokersInUseCheck.VolumesInUse(Map.of(), Map.of(1000, Set.of(1)))));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        Exception e = assertThrows(Exception.class, () ->
                creator.prepareKafkaCluster(KAFKA, List.of(POOL_CONTROLLERS_WITH_STATUS, POOL_A_WITH_STATUS), OLD_STORAGE_TWO_VOLUMES_IN_POOL_A, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, false, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                        .toCompletableFuture()
                        .join());

        assertThat(e.getCause(), instanceOf(InvalidResourceException.class));
        assertThat(e.getCause().getMessage(), is("Following errors were found when processing the Kafka custom resource: [Cannot remove the JBOD volumes [1] from Kafka brokers [1000] because it is not known whether they are empty. The broker did not answer, or the log directory is offline.]"));
    }

    @Test
    public void testErrorPairsEveryVolumeWithItsOwnBrokersWithKRaft() throws Exception {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock volumes-in-use check => two nodes are blocked on volume 1 and one node on volume 2
        Map<Integer, Set<Integer>> notEmpty = new LinkedHashMap<>();
        notEmpty.put(1000, Set.of(1));
        notEmpty.put(1001, Set.of(1));
        notEmpty.put(1002, Set.of(2));

        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.volumesInUse(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(new BrokersInUseCheck.VolumesInUse(notEmpty, Map.of())));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        Exception e = assertThrows(Exception.class, () ->
                creator.prepareKafkaCluster(KAFKA, List.of(POOL_CONTROLLERS_WITH_STATUS, POOL_A_WITH_STATUS), OLD_STORAGE_TWO_VOLUMES_IN_POOL_A, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, false, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                        .toCompletableFuture()
                        .join());

        // Nodes blocked on the same volumes are listed together, so no volume is paired with a broker which is fine
        assertThat(e.getCause(), instanceOf(InvalidResourceException.class));
        assertThat(e.getCause().getMessage(), is("Following errors were found when processing the Kafka custom resource: [Cannot remove the JBOD volumes [1] from Kafka brokers [1000, 1001], JBOD volumes [2] from Kafka brokers [1002] because they have assigned partition-replicas.]"));
    }

    @Test
    public void testVolumeRemovalChecksNodesWhichAreBeingRemovedWithKRaft() throws Exception {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock brokers-in-use check => the brokers being scaled down are empty, so the scale-down is allowed
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.brokersInUse(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(Set.of()));

        // Mock volumes-in-use check
        when(brokersInUseOps.volumesInUse(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(new BrokersInUseCheck.VolumesInUse(Map.of(), Map.of())));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        KafkaCluster kc = creator.prepareKafkaCluster(KAFKA, List.of(POOL_CONTROLLERS_WITH_STATUS, POOL_A_WITH_STATUS_5_NODES), OLD_STORAGE_TWO_VOLUMES_IN_POOL_A, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, true, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                .toCompletableFuture()
                .join();

        // Nodes 1003 and 1004 are being scaled down, but they still run and still hold their volumes, so they are
        // checked too. Leaving them out makes the second pass see nodes the first pass never checked.
        assertThat(kc, is(notNullValue()));
        assertThat(kc.removedJbodVolumes().keySet(), is(Set.of(1000, 1001, 1002, 1003, 1004)));
    }

    @Test
    public void testRevertVolumeRemovalAndRoleChangeTogetherWithKRaft() throws Exception {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock brokers-in-use check => the nodes losing the broker role still have partition replicas
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.brokersInUse(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(Set.of(3000, 3001, 3002)));

        // Mock volumes-in-use check => the removed volume still has partition replicas as well
        when(brokersInUseOps.volumesInUse(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(new BrokersInUseCheck.VolumesInUse(Map.of(3000, Set.of(1), 3001, Set.of(1), 3002, Set.of(1)), Map.of())));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        KafkaCluster kc = creator.prepareKafkaCluster(KAFKA, List.of(POOL_MIXED_NOT_MIXED_ANYMORE, POOL_A_WITH_STATUS_AND_BOTH_VOLUMES), OLD_STORAGE_MIXED_AND_CONTROLLERS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, true, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                .toCompletableFuture()
                .join();

        // The nodes are brokers right now, so their volumes are checked even though the pool asks to be a controller
        // only. Both changes are reverted in the first pass, so the second pass finds nothing to check.
        assertThat(kc, is(notNullValue()));
        assertThat(kc.brokerNodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(1000, 1001, 1002, 3000, 3001, 3002)));
        assertThat(kc.getStorageByPoolName().get("pool-mixed"), is(OLD_STORAGE_MIXED_AND_CONTROLLERS.get("my-cluster-pool-mixed")));

        assertThat(kafkaStatus.getConditions().size(), is(2));
        assertThat(kafkaStatus.getConditions().get(0).getMessage(), is("Reverting role change of KafkaNodePool pool-mixed (setting roles to [CONTROLLER, BROKER])"));
        assertThat(kafkaStatus.getConditions().get(1).getMessage(), is("Reverting all storage changes of KafkaNodePool pool-mixed because they remove JBOD volumes which are not empty"));

        verify(supplier.brokersInUseCheck, times(1)).volumesInUse(any(), any(), any(), any());
    }

    @Test
    public void testThrowsWhenSecondPassBlocksAnotherPoolWithKRaft() throws Exception {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock volumes-in-use check => pool-a is blocked first, and pool-b gets a new replica before it is checked again
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.volumesInUse(any(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(new BrokersInUseCheck.VolumesInUse(Map.of(1000, Set.of(1)), Map.of())))
                .thenReturn(CompletableFuture.completedFuture(new BrokersInUseCheck.VolumesInUse(Map.of(2000, Set.of(1)), Map.of())));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(RECONCILIATION, CO_CONFIG, supplier);

        Exception e = assertThrows(Exception.class, () ->
                creator.prepareKafkaCluster(KAFKA, List.of(POOL_CONTROLLERS_WITH_STATUS, POOL_A_WITH_STATUS, POOL_B_WITH_STATUS), OLD_STORAGE_TWO_VOLUMES_IN_BOTH_POOLS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, kafkaStatus, true, KafkaClusterSecurityContext.DEFAULT_KAFKA_CLUSTER_SECURITY_CONTEXT)
                        .toCompletableFuture()
                        .join());

        // Only the blocked pool is reverted, so a pool which becomes blocked between the two passes fails the
        // reconciliation. The next reconciliation starts again and reverts it.
        assertThat(e.getCause(), instanceOf(InvalidResourceException.class));
        assertThat(e.getCause().getMessage(), is("Following errors were found when processing the Kafka custom resource: [Cannot remove the JBOD volumes [1] from Kafka brokers [2000] because they have assigned partition-replicas.]"));
    }
}
