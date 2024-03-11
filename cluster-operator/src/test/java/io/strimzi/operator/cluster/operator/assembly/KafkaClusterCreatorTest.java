/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaMetadataConfigurationState;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaClusterCreatorTest {
    private final static String NAMESPACE = "my-ns";
    private final static String CLUSTER_NAME = "my-cluster";
    private final static Reconciliation RECONCILIATION = new Reconciliation("test", "kind", NAMESPACE, CLUSTER_NAME);
    private final static KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private final static KafkaVersionChange VERSION_CHANGE = new KafkaVersionChange(
            VERSIONS.defaultVersion(),
            VERSIONS.defaultVersion(),
            VERSIONS.defaultVersion().protocolVersion(),
            VERSIONS.defaultVersion().messageVersion(),
            VERSIONS.defaultVersion().metadataVersion()
    );
    private final static ClusterOperatorConfig CO_CONFIG = ResourceUtils.dummyClusterOperatorConfig();

    private final static Kafka KAFKA = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("tls")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .build())
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .build();
    private final static Kafka KAFKA_WITH_POOLS = new KafkaBuilder(KAFKA)
                .withNewMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled")
                .endMetadata()
                .build();
    private final static Kafka KAFKA_WITH_KRAFT = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(NAMESPACE)
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled", Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled"))
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

    private static final Map<String, List<String>> CURRENT_PODS_3_NODES = Map.of("my-cluster-kafka", List.of("my-cluster-kafka-0", "my-cluster-kafka-1", "my-cluster-kafka-2"));
    private static final Map<String, List<String>> CURRENT_PODS_5_NODES = Map.of("my-cluster-kafka", List.of("my-cluster-kafka-0", "my-cluster-kafka-1", "my-cluster-kafka-2", "my-cluster-kafka-3", "my-cluster-kafka-4"));

    private static Vertx vertx;
    private static WorkerExecutor sharedWorkerExecutor;

    @BeforeAll
    public static void beforeAll()  {
        vertx = Vertx.vertx();
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");
    }

    @AfterAll
    public static void afterAll()    {
        sharedWorkerExecutor.close();
        vertx.close();
    }

    //////////////////////////////////////////////////
    // Regular Kafka cluster tests
    //////////////////////////////////////////////////

    @Test
    public void testNewClusterWithoutNodePools(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(vertx, RECONCILIATION, CO_CONFIG, KafkaMetadataConfigurationState.ZK, supplier);

        Checkpoint async = context.checkpoint();
        creator.prepareKafkaCluster(KAFKA, null, Map.of(), Map.of(), VERSION_CHANGE, kafkaStatus, true)
                .onComplete(context.succeeding(kc -> context.verify(() -> {
                    // Kafka cluster is created
                    assertThat(kc, is(notNullValue()));
                    assertThat(kc.nodes().size(), is(3));
                    assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(0, 1, 2)));
                    assertThat(kc.removedNodes(), is(Set.of()));

                    // Check the status conditions
                    assertThat(kafkaStatus.getConditions(), is(nullValue()));

                    // No scale-down => scale-down check is not done
                    verify(supplier.brokersInUseCheck, never()).brokersInUse(any(), any(), any(), any());

                    async.flag();
                })));
    }

    @Test
    public void testExistingClusterWithoutNodePools(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(vertx, RECONCILIATION, CO_CONFIG, KafkaMetadataConfigurationState.ZK, supplier);

        Checkpoint async = context.checkpoint();
        creator.prepareKafkaCluster(KAFKA, null, Map.of(), CURRENT_PODS_3_NODES, VERSION_CHANGE, kafkaStatus, true)
                .onComplete(context.succeeding(kc -> context.verify(() -> {
                    // Kafka cluster is created
                    assertThat(kc, is(notNullValue()));
                    assertThat(kc.nodes().size(), is(3));
                    assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(0, 1, 2)));
                    assertThat(kc.removedNodes(), is(Set.of()));

                    // Check the status conditions
                    assertThat(kafkaStatus.getConditions(), is(nullValue()));

                    // No scale-down => scale-down check is not done
                    verify(supplier.brokersInUseCheck, never()).brokersInUse(any(), any(), any(), any());

                    async.flag();
                })));
    }

    @Test
    public void testRevertScaleDownWithoutNodePools(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock brokers-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.brokersInUse(any(), any(), any(), any())).thenReturn(Future.succeededFuture(Set.of(0, 1, 2, 3, 4)));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(vertx, RECONCILIATION, CO_CONFIG, KafkaMetadataConfigurationState.ZK, supplier);

        Checkpoint async = context.checkpoint();
        creator.prepareKafkaCluster(KAFKA, null, Map.of(), CURRENT_PODS_5_NODES, VERSION_CHANGE, kafkaStatus, true)
                .onComplete(context.succeeding(kc -> context.verify(() -> {
                    // Kafka cluster is created
                    assertThat(kc, is(notNullValue()));
                    assertThat(kc.nodes().size(), is(5));
                    assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(0, 1, 2, 3, 4)));
                    assertThat(kc.removedNodes(), is(Set.of()));

                    // Check the status conditions
                    assertThat(kafkaStatus.getConditions().size(), is(1));
                    assertThat(kafkaStatus.getConditions().get(0).getStatus(), is("True"));
                    assertThat(kafkaStatus.getConditions().get(0).getType(), is("Warning"));
                    assertThat(kafkaStatus.getConditions().get(0).getReason(), is("ScaleDownPreventionCheck"));
                    assertThat(kafkaStatus.getConditions().get(0).getMessage(), is("Reverting scale-down of Kafka my-cluster by changing number of replicas to 5"));

                    // Scale-down reverted => should be called once
                    verify(supplier.brokersInUseCheck, times(1)).brokersInUse(any(), any(), any(), any());

                    async.flag();
                })));
    }

    @Test
    public void testCorrectScaleDownWithoutNodePools(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock brokers-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.brokersInUse(any(), any(), any(), any())).thenReturn(Future.succeededFuture(Set.of(0, 1, 2)));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(vertx, RECONCILIATION, CO_CONFIG, KafkaMetadataConfigurationState.ZK, supplier);

        Checkpoint async = context.checkpoint();
        creator.prepareKafkaCluster(KAFKA, null, Map.of(), CURRENT_PODS_5_NODES, VERSION_CHANGE, kafkaStatus, true)
                .onComplete(context.succeeding(kc -> context.verify(() -> {
                    // Kafka cluster is created
                    assertThat(kc, is(notNullValue()));
                    assertThat(kc.nodes().size(), is(3));
                    assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(0, 1, 2)));
                    assertThat(kc.removedNodes(), is(Set.of(3, 4)));

                    // Check the status conditions
                    assertThat(kafkaStatus.getConditions(), is(nullValue()));

                    // Scale-down reverted => should be called once
                    verify(supplier.brokersInUseCheck, times(1)).brokersInUse(any(), any(), any(), any());

                    async.flag();
                })));
    }

    @Test
    public void testThrowsRevertScaleDownFailsWithoutNodePools(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock brokers-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.brokersInUse(any(), any(), any(), any())).thenReturn(Future.succeededFuture(Set.of(0, 1, 2, 3, 4)));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(vertx, RECONCILIATION, CO_CONFIG, KafkaMetadataConfigurationState.ZK, supplier);

        Checkpoint async = context.checkpoint();
        creator.prepareKafkaCluster(KAFKA, null, Map.of(), CURRENT_PODS_5_NODES, VERSION_CHANGE, kafkaStatus, false)
                .onComplete(context.failing(ex -> context.verify(() -> {
                    // Check exception
                    assertThat(ex, instanceOf(InvalidResourceException.class));
                    assertThat(ex.getMessage(), is("Following errors were found when processing the Kafka custom resource: [Cannot scale-down Kafka brokers [3, 4] because they have assigned partition-replicas.]"));

                    // Check the status conditions
                    assertThat(kafkaStatus.getConditions(), is(nullValue()));

                    // Scale-down failed => should be called once
                    verify(supplier.brokersInUseCheck, times(1)).brokersInUse(any(), any(), any(), any());

                    async.flag();
                })));
    }

    @Test
    public void tesSkipScaleDownCheckWithoutNodePools(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_SKIP_BROKER_SCALEDOWN_CHECK, "true")
                .endMetadata()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock brokers-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.brokersInUse(any(), any(), any(), any())).thenReturn(Future.succeededFuture(Set.of(0, 1, 2, 3, 4)));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(vertx, RECONCILIATION, CO_CONFIG, KafkaMetadataConfigurationState.ZK, supplier);

        Checkpoint async = context.checkpoint();
        creator.prepareKafkaCluster(kafka, null, Map.of(), CURRENT_PODS_5_NODES, VERSION_CHANGE, kafkaStatus, false)
                .onComplete(context.succeeding(kc -> context.verify(() -> {
                    // Kafka cluster is created
                    assertThat(kc, is(notNullValue()));
                    assertThat(kc.nodes().size(), is(3));
                    assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(0, 1, 2)));
                    assertThat(kc.removedNodes(), is(Set.of(3, 4)));

                    // Check the status conditions
                    assertThat(kafkaStatus.getConditions(), is(nullValue()));

                    // Scale-down check skipped => should be never called
                    verify(supplier.brokersInUseCheck, never()).brokersInUse(any(), any(), any(), any());

                    async.flag();
                })));
    }

    //////////////////////////////////////////////////
    // Kafka cluster with node pools tests
    //////////////////////////////////////////////////

    @Test
    public void testNewClusterWithNodePools(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(vertx, RECONCILIATION, CO_CONFIG, KafkaMetadataConfigurationState.ZK, supplier);

        Checkpoint async = context.checkpoint();
        creator.prepareKafkaCluster(KAFKA_WITH_POOLS, List.of(POOL_A, POOL_B), Map.of(), null, VERSION_CHANGE, kafkaStatus, true)
                .onComplete(context.succeeding(kc -> context.verify(() -> {
                    // Kafka cluster is created
                    assertThat(kc, is(notNullValue()));
                    assertThat(kc.nodes().size(), is(6));
                    assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(0, 1, 2, 3, 4, 5)));
                    assertThat(kc.removedNodes(), is(Set.of()));

                    // Check the status conditions
                    assertThat(kafkaStatus.getConditions(), is(nullValue()));

                    // No scale-down => scale-down check is not done
                    verify(supplier.brokersInUseCheck, never()).brokersInUse(any(), any(), any(), any());

                    async.flag();
                })));
    }

    @Test
    public void testExistingClusterWithNodePools(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(vertx, RECONCILIATION, CO_CONFIG, KafkaMetadataConfigurationState.ZK, supplier);

        Checkpoint async = context.checkpoint();
        creator.prepareKafkaCluster(KAFKA_WITH_POOLS, List.of(POOL_A_WITH_STATUS, POOL_B_WITH_STATUS), Map.of(), null, VERSION_CHANGE, kafkaStatus, true)
                .onComplete(context.succeeding(kc -> context.verify(() -> {
                    // Kafka cluster is created
                    assertThat(kc, is(notNullValue()));
                    assertThat(kc.nodes().size(), is(6));
                    assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(1000, 1001, 1002, 2000, 2001, 2002)));
                    assertThat(kc.removedNodes(), is(Set.of()));

                    // Check the status conditions
                    assertThat(kafkaStatus.getConditions(), is(nullValue()));

                    // No scale-down => scale-down check is not done
                    verify(supplier.brokersInUseCheck, never()).brokersInUse(any(), any(), any(), any());

                    async.flag();
                })));
    }

    @Test
    public void testRevertScaleDownWithNodePools(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock brokers-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.brokersInUse(any(), any(), any(), any())).thenReturn(Future.succeededFuture(Set.of(1000, 1001, 1002, 1003, 2004)));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(vertx, RECONCILIATION, CO_CONFIG, KafkaMetadataConfigurationState.ZK, supplier);

        Checkpoint async = context.checkpoint();
        creator.prepareKafkaCluster(KAFKA_WITH_POOLS, List.of(POOL_A_WITH_STATUS_5_NODES, POOL_B_WITH_STATUS_5_NODES), Map.of(), null, VERSION_CHANGE, kafkaStatus, true)
                .onComplete(context.succeeding(kc -> context.verify(() -> {
                    // Kafka cluster is created
                    assertThat(kc, is(notNullValue()));
                    assertThat(kc.nodes().size(), is(10));
                    assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(1000, 1001, 1002, 1003, 1004, 2000, 2001, 2002, 2003, 2004)));
                    assertThat(kc.removedNodes(), is(Set.of()));

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

                    // Scale-down reverted => should be called once
                    verify(supplier.brokersInUseCheck, times(1)).brokersInUse(any(), any(), any(), any());

                    async.flag();
                })));
    }

    @Test
    public void testCorrectScaleDownWithNodePools(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock brokers-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.brokersInUse(any(), any(), any(), any())).thenReturn(Future.succeededFuture(Set.of(1000, 1001, 1002, 2000, 2001, 2002)));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(vertx, RECONCILIATION, CO_CONFIG, KafkaMetadataConfigurationState.ZK, supplier);

        Checkpoint async = context.checkpoint();
        creator.prepareKafkaCluster(KAFKA_WITH_POOLS, List.of(POOL_A_WITH_STATUS_5_NODES, POOL_B_WITH_STATUS_5_NODES), Map.of(), CURRENT_PODS_5_NODES, VERSION_CHANGE, kafkaStatus, true)
                .onComplete(context.succeeding(kc -> context.verify(() -> {
                    // Kafka cluster is created
                    assertThat(kc, is(notNullValue()));
                    assertThat(kc.nodes().size(), is(6));
                    assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(1000, 1001, 1002, 2000, 2001, 2002)));
                    assertThat(kc.removedNodes(), is(Set.of(1003, 1004, 2003, 2004)));

                    // Check the status conditions
                    assertThat(kafkaStatus.getConditions(), is(nullValue()));

                    // Scale-down reverted => should be called once
                    verify(supplier.brokersInUseCheck, times(1)).brokersInUse(any(), any(), any(), any());

                    async.flag();
                })));
    }

    @Test
    public void testThrowsRevertScaleDownFailsWithNodePools(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock brokers-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.brokersInUse(any(), any(), any(), any())).thenReturn(Future.succeededFuture(Set.of(1000, 1001, 1002, 1003, 1004, 2003, 2004)));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(vertx, RECONCILIATION, CO_CONFIG, KafkaMetadataConfigurationState.ZK, supplier);

        Checkpoint async = context.checkpoint();
        creator.prepareKafkaCluster(KAFKA_WITH_POOLS, List.of(POOL_A_WITH_STATUS_5_NODES, POOL_B_WITH_STATUS_5_NODES), Map.of(), null, VERSION_CHANGE, kafkaStatus, false)
                .onComplete(context.failing(ex -> context.verify(() -> {
                    // Check exception
                    assertThat(ex, instanceOf(InvalidResourceException.class));
                    assertThat(ex.getMessage(), is("Following errors were found when processing the Kafka custom resource: [Cannot scale-down Kafka brokers [1003, 1004, 2003, 2004] because they have assigned partition-replicas.]"));

                    // Check the status conditions
                    assertThat(kafkaStatus.getConditions(), is(nullValue()));

                    // Scale-down failed => should be called once
                    verify(supplier.brokersInUseCheck, times(1)).brokersInUse(any(), any(), any(), any());

                    async.flag();
                })));
    }

    @Test
    public void tesSkipScaleDownCheckWithNodePools(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder(KAFKA_WITH_POOLS)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_SKIP_BROKER_SCALEDOWN_CHECK, "true")
                .endMetadata()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock brokers-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.brokersInUse(any(), any(), any(), any())).thenReturn(Future.succeededFuture(Set.of(1000, 1001, 1002, 1003, 1004, 2003, 2004)));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(vertx, RECONCILIATION, CO_CONFIG, KafkaMetadataConfigurationState.ZK, supplier);

        Checkpoint async = context.checkpoint();
        creator.prepareKafkaCluster(kafka, List.of(POOL_A_WITH_STATUS_5_NODES, POOL_B_WITH_STATUS_5_NODES), Map.of(), null, VERSION_CHANGE, kafkaStatus, false)
                .onComplete(context.succeeding(kc -> context.verify(() -> {
                    // Kafka cluster is created
                    assertThat(kc, is(notNullValue()));
                    assertThat(kc.nodes().size(), is(6));
                    assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(1000, 1001, 1002, 2000, 2001, 2002)));
                    assertThat(kc.removedNodes(), is(Set.of(1003, 1004, 2003, 2004)));

                    // Check the status conditions
                    assertThat(kafkaStatus.getConditions(), is(nullValue()));

                    // Scale-down check skipped => should be never called
                    verify(supplier.brokersInUseCheck, never()).brokersInUse(any(), any(), any(), any());

                    async.flag();
                })));
    }

    //////////////////////////////////////////////////
    // KRaft tests
    //////////////////////////////////////////////////

    @Test
    public void testNewClusterWithKRaft(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(vertx, RECONCILIATION, CO_CONFIG, KafkaMetadataConfigurationState.KRAFT, supplier);

        Checkpoint async = context.checkpoint();
        creator.prepareKafkaCluster(KAFKA_WITH_KRAFT, List.of(POOL_CONTROLLERS, POOL_A, POOL_B), Map.of(), null, VERSION_CHANGE, kafkaStatus, true)
                .onComplete(context.succeeding(kc -> context.verify(() -> {
                    // Kafka cluster is created
                    assertThat(kc, is(notNullValue()));
                    assertThat(kc.nodes().size(), is(9));
                    assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(0, 1, 2, 3, 4, 5, 6, 7, 8)));
                    assertThat(kc.removedNodes(), is(Set.of()));

                    // Check the status conditions
                    assertThat(kafkaStatus.getConditions(), is(nullValue()));

                    // No scale-down => scale-down check is not done
                    verify(supplier.brokersInUseCheck, never()).brokersInUse(any(), any(), any(), any());

                    async.flag();
                })));
    }

    @Test
    public void testExistingClusterWithKRaft(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(vertx, RECONCILIATION, CO_CONFIG, KafkaMetadataConfigurationState.KRAFT, supplier);

        Checkpoint async = context.checkpoint();
        creator.prepareKafkaCluster(KAFKA_WITH_KRAFT, List.of(POOL_CONTROLLERS_WITH_STATUS, POOL_A_WITH_STATUS, POOL_B_WITH_STATUS), Map.of(), null, VERSION_CHANGE, kafkaStatus, true)
                .onComplete(context.succeeding(kc -> context.verify(() -> {
                    // Kafka cluster is created
                    assertThat(kc, is(notNullValue()));
                    assertThat(kc.nodes().size(), is(9));
                    assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(1000, 1001, 1002, 2000, 2001, 2002, 3000, 3001, 3002)));
                    assertThat(kc.removedNodes(), is(Set.of()));

                    // Check the status conditions
                    assertThat(kafkaStatus.getConditions(), is(nullValue()));

                    // No scale-down => scale-down check is not done
                    verify(supplier.brokersInUseCheck, never()).brokersInUse(any(), any(), any(), any());

                    async.flag();
                })));
    }

    @Test
    public void testRevertScaleDownWithKRaft(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock brokers-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.brokersInUse(any(), any(), any(), any())).thenReturn(Future.succeededFuture(Set.of(1000, 1001, 1002, 1003, 2004)));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(vertx, RECONCILIATION, CO_CONFIG, KafkaMetadataConfigurationState.KRAFT, supplier);

        Checkpoint async = context.checkpoint();
        creator.prepareKafkaCluster(KAFKA_WITH_KRAFT, List.of(POOL_CONTROLLERS_WITH_STATUS_5_NODES, POOL_A_WITH_STATUS_5_NODES, POOL_B_WITH_STATUS_5_NODES), Map.of(), null, VERSION_CHANGE, kafkaStatus, true)
                .onComplete(context.succeeding(kc -> context.verify(() -> {
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
                    verify(supplier.brokersInUseCheck, times(2)).brokersInUse(any(), any(), any(), any());

                    async.flag();
                })));
    }

    @Test
    public void testRevertScaleDownWithKRaftMixedNodes(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock brokers-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.brokersInUse(any(), any(), any(), any())).thenReturn(Future.succeededFuture(Set.of(3000, 3001, 3002, 3003)));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(vertx, RECONCILIATION, CO_CONFIG, KafkaMetadataConfigurationState.KRAFT, supplier);

        Checkpoint async = context.checkpoint();
        creator.prepareKafkaCluster(KAFKA_WITH_KRAFT, List.of(POOL_MIXED_WITH_STATUS_5_NODES), Map.of(), null, VERSION_CHANGE, kafkaStatus, true)
                .onComplete(context.succeeding(kc -> context.verify(() -> {
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
                    verify(supplier.brokersInUseCheck, times(1)).brokersInUse(any(), any(), any(), any());

                    async.flag();
                })));
    }

    @Test
    public void testCorrectScaleDownWithKRaft(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock brokers-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.brokersInUse(any(), any(), any(), any())).thenReturn(Future.succeededFuture(Set.of(1000, 1001, 1002, 2000, 2001, 2002, 3000, 3001, 3002)));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(vertx, RECONCILIATION, CO_CONFIG, KafkaMetadataConfigurationState.KRAFT, supplier);

        Checkpoint async = context.checkpoint();
        creator.prepareKafkaCluster(KAFKA_WITH_KRAFT, List.of(POOL_CONTROLLERS_WITH_STATUS_5_NODES, POOL_A_WITH_STATUS_5_NODES, POOL_B_WITH_STATUS_5_NODES), Map.of(), CURRENT_PODS_5_NODES, VERSION_CHANGE, kafkaStatus, true)
                .onComplete(context.succeeding(kc -> context.verify(() -> {
                    // Kafka cluster is created
                    assertThat(kc, is(notNullValue()));
                    assertThat(kc.nodes().size(), is(9));
                    assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(1000, 1001, 1002, 2000, 2001, 2002, 3000, 3001, 3002)));
                    assertThat(kc.removedNodes(), is(Set.of(1003, 1004, 2003, 2004, 3003, 3004)));

                    // Check the status conditions
                    assertThat(kafkaStatus.getConditions(), is(nullValue()));

                    // Scale-down reverted => should be called once
                    verify(supplier.brokersInUseCheck, times(1)).brokersInUse(any(), any(), any(), any());

                    async.flag();
                })));
    }

    @Test
    public void testThrowsRevertScaleDownFailsWithKRaft(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock brokers-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.brokersInUse(any(), any(), any(), any())).thenReturn(Future.succeededFuture(Set.of(1003, 1004, 2003, 2004)));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(vertx, RECONCILIATION, CO_CONFIG, KafkaMetadataConfigurationState.KRAFT, supplier);

        Checkpoint async = context.checkpoint();
        creator.prepareKafkaCluster(KAFKA_WITH_KRAFT, List.of(POOL_CONTROLLERS_WITH_STATUS_5_NODES, POOL_A_WITH_STATUS_5_NODES, POOL_B_WITH_STATUS_5_NODES), Map.of(), null, VERSION_CHANGE, kafkaStatus, false)
                .onComplete(context.failing(ex -> context.verify(() -> {
                    // Check exception
                    assertThat(ex, instanceOf(InvalidResourceException.class));
                    assertThat(ex.getMessage(), is("Following errors were found when processing the Kafka custom resource: [Cannot scale-down Kafka brokers [3003, 3004, 1003, 1004, 2003, 2004] because they have assigned partition-replicas.]"));

                    // Check the status conditions
                    assertThat(kafkaStatus.getConditions(), is(nullValue()));

                    // Scale-down failed => should be called once
                    verify(supplier.brokersInUseCheck, times(1)).brokersInUse(any(), any(), any(), any());

                    async.flag();
                })));
    }

    @Test
    public void testSkipScaleDownCheckWithKRaft(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder(KAFKA_WITH_KRAFT)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_SKIP_BROKER_SCALEDOWN_CHECK, "true")
                .endMetadata()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(vertx, RECONCILIATION, CO_CONFIG, KafkaMetadataConfigurationState.KRAFT, supplier);

        Checkpoint async = context.checkpoint();
        creator.prepareKafkaCluster(kafka, List.of(POOL_CONTROLLERS_WITH_STATUS_5_NODES, POOL_A_WITH_STATUS_5_NODES, POOL_B_WITH_STATUS_5_NODES), Map.of(), null, VERSION_CHANGE, kafkaStatus, false)
                .onComplete(context.succeeding(kc -> context.verify(() -> {
                    // Kafka cluster is created
                    assertThat(kc, is(notNullValue()));
                    assertThat(kc.nodes().size(), is(9));
                    assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(1000, 1001, 1002, 2000, 2001, 2002, 3000, 3001, 3002)));
                    assertThat(kc.removedNodes(), is(Set.of(1003, 1004, 2003, 2004, 3003, 3004)));

                    // Check the status conditions
                    assertThat(kafkaStatus.getConditions(), is(nullValue()));

                    // Scale-down check skipped => should be never called
                    verify(supplier.brokersInUseCheck, never()).brokersInUse(any(), any(), any(), any());

                    async.flag();
                })));
    }

    @Test
    public void testRevertRoleChangeWithKRaftMixedNodes(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock brokers-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.brokersInUse(any(), any(), any(), any())).thenReturn(Future.succeededFuture(Set.of(1000, 1001, 1002, 2000, 2001, 2002, 3000, 3001, 3002)));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(vertx, RECONCILIATION, CO_CONFIG, KafkaMetadataConfigurationState.KRAFT, supplier);

        Checkpoint async = context.checkpoint();
        creator.prepareKafkaCluster(KAFKA_WITH_KRAFT, List.of(POOL_MIXED_NOT_MIXED_ANYMORE, POOL_A_WITH_STATUS, POOL_B_WITH_STATUS), Map.of(), null, VERSION_CHANGE, kafkaStatus, true)
                .onComplete(context.succeeding(kc -> context.verify(() -> {
                    // Kafka cluster is created
                    assertThat(kc, is(notNullValue()));
                    assertThat(kc.nodes().size(), is(9));
                    assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(1000, 1001, 1002, 2000, 2001, 2002, 3000, 3001, 3002)));
                    assertThat(kc.removedNodes(), is(Set.of()));
                    assertThat(kc.usedToBeBrokerNodes(), is(Set.of()));

                    // Check the status conditions
                    assertThat(kafkaStatus.getConditions().size(), is(1));
                    assertThat(kafkaStatus.getConditions().get(0).getStatus(), is("True"));
                    assertThat(kafkaStatus.getConditions().get(0).getType(), is("Warning"));
                    assertThat(kafkaStatus.getConditions().get(0).getReason(), is("ScaleDownPreventionCheck"));
                    assertThat(kafkaStatus.getConditions().get(0).getMessage(), is("Reverting role change of KafkaNodePool pool-mixed by adding the broker role to it"));

                    // Scale-down reverted => should be called twice as we still scale down controllers after the revert is done
                    verify(supplier.brokersInUseCheck, times(1)).brokersInUse(any(), any(), any(), any());

                    async.flag();
                })));
    }

    @Test
    public void testCorrectRoleChangeWithKRaft(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock brokers-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.brokersInUse(any(), any(), any(), any())).thenReturn(Future.succeededFuture(Set.of(1000, 1001, 1002, 2000, 2001, 20022)));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(vertx, RECONCILIATION, CO_CONFIG, KafkaMetadataConfigurationState.KRAFT, supplier);

        Checkpoint async = context.checkpoint();
        creator.prepareKafkaCluster(KAFKA_WITH_KRAFT, List.of(POOL_MIXED_NOT_MIXED_ANYMORE, POOL_A_WITH_STATUS, POOL_B_WITH_STATUS), Map.of(), CURRENT_PODS_5_NODES, VERSION_CHANGE, kafkaStatus, true)
                .onComplete(context.succeeding(kc -> context.verify(() -> {
                    // Kafka cluster is created
                    assertThat(kc, is(notNullValue()));
                    assertThat(kc.nodes().size(), is(9));
                    assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(1000, 1001, 1002, 2000, 2001, 2002, 3000, 3001, 3002)));
                    assertThat(kc.removedNodes(), is(Set.of()));
                    assertThat(kc.usedToBeBrokerNodes(), is(Set.of(3000, 3001, 3002)));

                    // Check the status conditions
                    assertThat(kafkaStatus.getConditions(), is(nullValue()));

                    // Scale-down reverted => should be called once
                    verify(supplier.brokersInUseCheck, times(1)).brokersInUse(any(), any(), any(), any());

                    async.flag();
                })));
    }

    @Test
    public void testThrowsRevertBrokerChangeFailsWithKRaft(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        // Mock brokers-in-use check
        BrokersInUseCheck brokersInUseOps = supplier.brokersInUseCheck;
        when(brokersInUseOps.brokersInUse(any(), any(), any(), any())).thenReturn(Future.succeededFuture(Set.of(3000, 3002)));

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(vertx, RECONCILIATION, CO_CONFIG, KafkaMetadataConfigurationState.KRAFT, supplier);

        Checkpoint async = context.checkpoint();
        creator.prepareKafkaCluster(KAFKA_WITH_KRAFT, List.of(POOL_MIXED_NOT_MIXED_ANYMORE, POOL_A_WITH_STATUS, POOL_B_WITH_STATUS), Map.of(), null, VERSION_CHANGE, kafkaStatus, false)
                .onComplete(context.failing(ex -> context.verify(() -> {
                    // Check exception
                    assertThat(ex, instanceOf(InvalidResourceException.class));
                    assertThat(ex.getMessage(), is("Following errors were found when processing the Kafka custom resource: [Cannot remove the broker role from nodes [3000, 3001, 3002] because they have assigned partition-replicas.]"));

                    // Check the status conditions
                    assertThat(kafkaStatus.getConditions(), is(nullValue()));

                    // Scale-down failed => should be called once
                    verify(supplier.brokersInUseCheck, times(1)).brokersInUse(any(), any(), any(), any());

                    async.flag();
                })));
    }

    @Test
    public void testSkipRoleChangeCheckWithKRaft(VertxTestContext context) {
        Kafka kafka = new KafkaBuilder(KAFKA_WITH_KRAFT)
                .editMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_SKIP_BROKER_SCALEDOWN_CHECK, "true")
                .endMetadata()
                .build();

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);

        KafkaStatus kafkaStatus = new KafkaStatus();
        KafkaClusterCreator creator = new KafkaClusterCreator(vertx, RECONCILIATION, CO_CONFIG, KafkaMetadataConfigurationState.KRAFT, supplier);

        Checkpoint async = context.checkpoint();
        creator.prepareKafkaCluster(kafka, List.of(POOL_MIXED_NOT_MIXED_ANYMORE, POOL_A_WITH_STATUS, POOL_B_WITH_STATUS), Map.of(), null, VERSION_CHANGE, kafkaStatus, false)
                .onComplete(context.succeeding(kc -> context.verify(() -> {
                    // Kafka cluster is created
                    assertThat(kc, is(notNullValue()));
                    assertThat(kc.nodes().size(), is(9));
                    assertThat(kc.nodes().stream().map(NodeRef::nodeId).collect(Collectors.toSet()), is(Set.of(1000, 1001, 1002, 2000, 2001, 2002, 3000, 3001, 3002)));
                    assertThat(kc.removedNodes(), is(Set.of()));
                    assertThat(kc.usedToBeBrokerNodes(), is(Set.of(3000, 3001, 3002)));

                    // Check the status conditions
                    assertThat(kafkaStatus.getConditions(), is(nullValue()));

                    // Scale-down check skipped => should be never called
                    verify(supplier.brokersInUseCheck, never()).brokersInUse(any(), any(), any(), any());

                    async.flag();
                })));
    }
}
