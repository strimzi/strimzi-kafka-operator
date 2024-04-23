/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.kafka.JbodStorage;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorage;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.nodepools.NodePoolUtils;
import io.strimzi.operator.cluster.model.nodepools.VirtualNodePoolConverter;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.InvalidResourceException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class NodePoolUtilsTest {
    private final static String NAMESPACE = "my-namespace";
    private final static String CLUSTER_NAME = "my-cluster";
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();
    private final static Kafka KAFKA = new KafkaBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withNewJbodStorage()
                            .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                        .endJbodStorage()
                    .endKafka()
                .endSpec()
                .build();
    private final static KafkaNodePool POOL_CONTROLLERS = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("controllers")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.CONTROLLER)
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
    private final static KafkaNodePool POOL_B = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("pool-b")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(2)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("200Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.BROKER)
            .endSpec()
            .build();

    @Test
    public void testNewVirtualNodePool()  {
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, null, Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);

        assertThat(pools.size(), is(1));
        assertThat(pools.get(0).poolName, is(VirtualNodePoolConverter.DEFAULT_NODE_POOL_NAME));
        assertThat(pools.get(0).processRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(pools.get(0).idAssignment.toBeAdded(), is(Set.of(0, 1, 2)));
        assertThat(pools.get(0).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.current(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.desired(), is(Set.of(0, 1, 2)));
    }

    @Test
    public void testExistingVirtualNodePool()  {
        Map<String, List<String>> existingPods = Map.of(
                CLUSTER_NAME + "-" + VirtualNodePoolConverter.DEFAULT_NODE_POOL_NAME,
                List.of(
                        CLUSTER_NAME + "-" + VirtualNodePoolConverter.DEFAULT_NODE_POOL_NAME + "-0",
                        CLUSTER_NAME + "-" + VirtualNodePoolConverter.DEFAULT_NODE_POOL_NAME + "-1",
                        CLUSTER_NAME + "-" + VirtualNodePoolConverter.DEFAULT_NODE_POOL_NAME + "-2"
                )
        );

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, null, Map.of(), existingPods, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);

        assertThat(pools.size(), is(1));
        assertThat(pools.get(0).poolName, is(VirtualNodePoolConverter.DEFAULT_NODE_POOL_NAME));
        assertThat(pools.get(0).processRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(pools.get(0).idAssignment.toBeAdded(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.current(), is(Set.of(0, 1, 2)));
        assertThat(pools.get(0).idAssignment.desired(), is(Set.of(0, 1, 2)));
    }

    @Test
    public void testExistingVirtualNodePoolWithScaleUp()  {
        Map<String, List<String>> existingPods = Map.of(
                CLUSTER_NAME + "-" + VirtualNodePoolConverter.DEFAULT_NODE_POOL_NAME,
                List.of(
                        CLUSTER_NAME + "-" + VirtualNodePoolConverter.DEFAULT_NODE_POOL_NAME + "-0",
                        CLUSTER_NAME + "-" + VirtualNodePoolConverter.DEFAULT_NODE_POOL_NAME + "-1"
                )
        );

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, null, Map.of(), existingPods, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);

        assertThat(pools.size(), is(1));
        assertThat(pools.get(0).poolName, is(VirtualNodePoolConverter.DEFAULT_NODE_POOL_NAME));
        assertThat(pools.get(0).processRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(pools.get(0).idAssignment.toBeAdded(), is(Set.of(2)));
        assertThat(pools.get(0).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.current(), is(Set.of(0, 1)));
        assertThat(pools.get(0).idAssignment.desired(), is(Set.of(0, 1, 2)));
    }

    @Test
    public void testExistingVirtualNodePoolWithStorageConflict()  {
        Map<String, List<String>> existingPods = Map.of(
                CLUSTER_NAME + "-" + VirtualNodePoolConverter.DEFAULT_NODE_POOL_NAME,
                List.of(
                        CLUSTER_NAME + "-" + VirtualNodePoolConverter.DEFAULT_NODE_POOL_NAME + "-0",
                        CLUSTER_NAME + "-" + VirtualNodePoolConverter.DEFAULT_NODE_POOL_NAME + "-1",
                        CLUSTER_NAME + "-" + VirtualNodePoolConverter.DEFAULT_NODE_POOL_NAME + "-2"
                )
        );

        Map<String, Storage> existingStorage = Map.of(
                CLUSTER_NAME + "-" + VirtualNodePoolConverter.DEFAULT_NODE_POOL_NAME,
                new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("1Ti").build()).build()
        );

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, null, existingStorage, existingPods, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);

        assertThat(pools.size(), is(1));
        assertThat(pools.get(0).poolName, is(VirtualNodePoolConverter.DEFAULT_NODE_POOL_NAME));

        JbodStorage storage = (JbodStorage) pools.get(0).storage;
        assertThat(((PersistentClaimStorage) storage.getVolumes().get(0)).getSize(), is("1Ti"));
    }

    @Test
    public void testNewNodePools()  {
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_A, POOL_B), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);

        assertThat(pools.size(), is(2));

        assertThat(pools.get(0).poolName, is("pool-a"));
        assertThat(pools.get(0).processRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(pools.get(0).idAssignment.toBeAdded(), is(Set.of(0, 1, 2)));
        assertThat(pools.get(0).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.current(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.desired(), is(Set.of(0, 1, 2)));

        assertThat(pools.get(1).poolName, is("pool-b"));
        assertThat(pools.get(1).processRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(pools.get(1).idAssignment.toBeAdded(), is(Set.of(3, 4)));
        assertThat(pools.get(1).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(1).idAssignment.current(), is(Set.of()));
        assertThat(pools.get(1).idAssignment.desired(), is(Set.of(3, 4)));
    }

    @Test
    public void testExistingNodePools()  {
        KafkaNodePool poolA = new KafkaNodePoolBuilder(POOL_A)
                .withNewStatus()
                    .withNodeIds(0, 1, 2)
                .endStatus()
                .build();

        KafkaNodePool poolB = new KafkaNodePoolBuilder(POOL_B)
                .withNewStatus()
                    .withNodeIds(10, 11)
                .endStatus()
                .build();

        Map<String, Storage> existingStorage = Map.of(
                CLUSTER_NAME + "-pool-a",
                new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build(),
                CLUSTER_NAME + "-pool-b",
                new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("200Gi").build()).build()
        );

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolA, poolB), existingStorage, Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);

        assertThat(pools.size(), is(2));

        assertThat(pools.get(0).poolName, is("pool-a"));
        assertThat(pools.get(0).processRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(pools.get(0).idAssignment.toBeAdded(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.current(), is(Set.of(0, 1, 2)));
        assertThat(pools.get(0).idAssignment.desired(), is(Set.of(0, 1, 2)));

        JbodStorage storage = (JbodStorage) pools.get(0).storage;
        assertThat(((PersistentClaimStorage) storage.getVolumes().get(0)).getSize(), is("100Gi"));

        assertThat(pools.get(1).poolName, is("pool-b"));
        assertThat(pools.get(1).processRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(pools.get(1).idAssignment.toBeAdded(), is(Set.of()));
        assertThat(pools.get(1).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(1).idAssignment.current(), is(Set.of(10, 11)));
        assertThat(pools.get(1).idAssignment.desired(), is(Set.of(10, 11)));

        storage = (JbodStorage) pools.get(1).storage;
        assertThat(((PersistentClaimStorage) storage.getVolumes().get(0)).getSize(), is("200Gi"));
    }

    @Test
    public void testExistingNodePoolsScaleUpDown()  {
        KafkaNodePool poolA = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withReplicas(2)
                .endSpec()
                .withNewStatus()
                    .withNodeIds(0, 1, 2)
                .endStatus()
                .build();

        KafkaNodePool poolB = new KafkaNodePoolBuilder(POOL_B)
                .editSpec()
                    .withReplicas(3)
                .endSpec()
                .withNewStatus()
                    .withNodeIds(10, 11)
                .endStatus()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolA, poolB), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);

        assertThat(pools.size(), is(2));

        assertThat(pools.get(0).poolName, is("pool-a"));
        assertThat(pools.get(0).processRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(pools.get(0).idAssignment.toBeAdded(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.toBeRemoved(), is(Set.of(2)));
        assertThat(pools.get(0).idAssignment.current(), is(Set.of(0, 1, 2)));
        assertThat(pools.get(0).idAssignment.desired(), is(Set.of(0, 1)));

        assertThat(pools.get(1).poolName, is("pool-b"));
        assertThat(pools.get(1).processRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(pools.get(1).idAssignment.toBeAdded(), is(Set.of(3)));
        assertThat(pools.get(1).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(1).idAssignment.current(), is(Set.of(10, 11)));
        assertThat(pools.get(1).idAssignment.desired(), is(Set.of(3, 10, 11)));
    }

    @Test
    public void testExistingNodePoolsScaleUpDownWithAnnotations()  {
        KafkaNodePool poolA = new KafkaNodePoolBuilder(POOL_A)
                .editMetadata()
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[10-19]",
                            Annotations.ANNO_STRIMZI_IO_REMOVE_NODE_IDS, "[19-10]"))
                .endMetadata()
                .editSpec()
                    .withReplicas(2)
                .endSpec()
                .withNewStatus()
                    .withNodeIds(10, 11, 12)
                .endStatus()
                .build();

        KafkaNodePool poolB = new KafkaNodePoolBuilder(POOL_B)
                .editMetadata()
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NEXT_NODE_IDS, "[20-29]",
                            Annotations.ANNO_STRIMZI_IO_REMOVE_NODE_IDS, "[29-20]"))
                .endMetadata()
                .editSpec()
                    .withReplicas(3)
                .endSpec()
                .withNewStatus()
                    .withNodeIds(20, 21)
                .endStatus()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolA, poolB), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);

        assertThat(pools.size(), is(2));

        assertThat(pools.get(0).poolName, is("pool-a"));
        assertThat(pools.get(0).processRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(pools.get(0).idAssignment.toBeAdded(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.toBeRemoved(), is(Set.of(12)));
        assertThat(pools.get(0).idAssignment.current(), is(Set.of(10, 11, 12)));
        assertThat(pools.get(0).idAssignment.desired(), is(Set.of(10, 11)));

        assertThat(pools.get(1).poolName, is("pool-b"));
        assertThat(pools.get(1).processRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(pools.get(1).idAssignment.toBeAdded(), is(Set.of(22)));
        assertThat(pools.get(1).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(1).idAssignment.current(), is(Set.of(20, 21)));
        assertThat(pools.get(1).idAssignment.desired(), is(Set.of(20, 21, 22)));
    }

    @Test
    public void testNewNodePoolsWithMixedKRaftNodes()  {
        KafkaNodePool poolA = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withRoles(ProcessRoles.BROKER, ProcessRoles.CONTROLLER)
                .endSpec()
                .build();

        KafkaNodePool poolB = new KafkaNodePoolBuilder(POOL_B)
                .editSpec()
                    .withRoles(ProcessRoles.BROKER, ProcessRoles.CONTROLLER)
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolA, poolB), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);

        assertThat(pools.size(), is(2));

        assertThat(pools.get(0).poolName, is("pool-a"));
        assertThat(pools.get(0).processRoles, is(Set.of(ProcessRoles.BROKER, ProcessRoles.CONTROLLER)));

        assertThat(pools.get(1).poolName, is("pool-b"));
        assertThat(pools.get(1).processRoles, is(Set.of(ProcessRoles.BROKER, ProcessRoles.CONTROLLER)));
    }

    @Test
    public void testNewNodePoolsWithKRaft()  {
        KafkaNodePool poolA = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withRoles(ProcessRoles.CONTROLLER)
                .endSpec()
                .build();

        KafkaNodePool poolB = new KafkaNodePoolBuilder(POOL_B)
                .editSpec()
                    .withRoles(ProcessRoles.BROKER)
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolA, poolB), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);

        assertThat(pools.size(), is(2));

        assertThat(pools.get(0).poolName, is("pool-a"));
        assertThat(pools.get(0).processRoles, is(Set.of(ProcessRoles.CONTROLLER)));

        assertThat(pools.get(1).poolName, is("pool-b"));
        assertThat(pools.get(1).processRoles, is(Set.of(ProcessRoles.BROKER)));
    }

    @Test
    public void testExistingNodePoolsWIthStorageConflict()  {
        KafkaNodePool poolA = new KafkaNodePoolBuilder(POOL_A)
                .withNewStatus()
                    .withNodeIds(0, 1, 2)
                .endStatus()
                .build();

        KafkaNodePool poolB = new KafkaNodePoolBuilder(POOL_B)
                .withNewStatus()
                    .withNodeIds(10, 11)
                .endStatus()
                .build();

        Map<String, Storage> existingStorage = Map.of(
                CLUSTER_NAME + "-pool-a",
                new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build(),
                CLUSTER_NAME + "-pool-b",
                new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("1Ti").build()).build()
        );

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolA, poolB), existingStorage, Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);

        assertThat(pools.size(), is(2));

        assertThat(pools.get(0).poolName, is("pool-a"));
        assertThat(pools.get(0).processRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(pools.get(0).idAssignment.toBeAdded(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.current(), is(Set.of(0, 1, 2)));
        assertThat(pools.get(0).idAssignment.desired(), is(Set.of(0, 1, 2)));

        JbodStorage storage = (JbodStorage) pools.get(0).storage;
        assertThat(((PersistentClaimStorage) storage.getVolumes().get(0)).getSize(), is("100Gi"));

        assertThat(pools.get(1).poolName, is("pool-b"));
        assertThat(pools.get(1).processRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(pools.get(1).idAssignment.toBeAdded(), is(Set.of()));
        assertThat(pools.get(1).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(1).idAssignment.current(), is(Set.of(10, 11)));
        assertThat(pools.get(1).idAssignment.desired(), is(Set.of(10, 11)));

        storage = (JbodStorage) pools.get(1).storage;
        assertThat(((PersistentClaimStorage) storage.getVolumes().get(0)).getSize(), is("1Ti"));
    }

    @Test
    public void testValidationWithNoRoles()   {
        KafkaNodePool poolA = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withRoles()
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.validateNodePools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolA), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false));
        assertThat(ex.getMessage(), containsString("KafkaNodePool pool-a has no role defined in .spec.roles"));
    }

    @Test
    public void testValidationZooKeeperBasedWithMixedRoles()   {
        KafkaNodePool poolA = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withRoles(ProcessRoles.BROKER, ProcessRoles.CONTROLLER)
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.validateNodePools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolA), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false));
        assertThat(ex.getMessage(), containsString("KafkaNodePool pool-a contains invalid roles configuration. In a ZooKeeper-based Kafka cluster, the KafkaNodePool role has to be always set only to the 'broker' role."));
    }

    @Test
    public void testValidationZooKeeperBasedWithControllerRole()   {
        KafkaNodePool poolA = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withRoles(ProcessRoles.CONTROLLER)
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.validateNodePools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolA), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false));
        assertThat(ex.getMessage(), containsString("KafkaNodePool pool-a contains invalid roles configuration. In a ZooKeeper-based Kafka cluster, the KafkaNodePool role has to be always set only to the 'broker' role."));
    }

    @Test
    public void testKRaftValidationWithNoRoles()   {
        KafkaNodePool poolA = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withRoles()
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.validateNodePools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolA), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true));
        assertThat(ex.getMessage(), containsString("KafkaNodePool pool-a has no role defined in .spec.roles"));
    }

    @Test
    public void testKRaftValidationWithSeparateRoles()   {
        KafkaNodePool poolA = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withRoles(ProcessRoles.CONTROLLER)
                .endSpec()
                .build();

        KafkaNodePool poolB = new KafkaNodePoolBuilder(POOL_B)
                .editSpec()
                    .withRoles(ProcessRoles.BROKER)
                .endSpec()
                .build();


        assertDoesNotThrow(() -> NodePoolUtils.validateNodePools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolA, poolB), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true));
    }

    @Test
    public void testKRaftValidationWithMissingRoles()   {
        KafkaNodePool poolA = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withRoles(ProcessRoles.CONTROLLER)
                .endSpec()
                .build();
        KafkaNodePool poolB = new KafkaNodePoolBuilder(POOL_B)
                .editSpec()
                    .withRoles(ProcessRoles.BROKER)
                .endSpec()
                .build();


        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.validateNodePools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolA), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true));
        assertThat(ex.getMessage(), containsString("At least one KafkaNodePool with the broker role and at least one replica is required when KRaft mode is enabled"));

        ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.validateNodePools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolB), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true));
        assertThat(ex.getMessage(), containsString("At least one KafkaNodePool with the controller role and at least one replica is required when KRaft mode is enabled"));
    }

    @Test
    public void testKRaftValidationWithRolesWithZeroReplicas()   {
        KafkaNodePool poolAWithReplicas = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withReplicas(3)
                    .withRoles(ProcessRoles.CONTROLLER)
                .endSpec()
                .build();
        KafkaNodePool poolAWithoutReplicas = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withReplicas(0)
                    .withRoles(ProcessRoles.CONTROLLER)
                .endSpec()
                .build();
        KafkaNodePool poolBWithReplicas = new KafkaNodePoolBuilder(POOL_B)
                .editSpec()
                    .withRoles(ProcessRoles.BROKER)
                    .withReplicas(3)
                .endSpec()
                .build();
        KafkaNodePool poolBWithoutReplicas = new KafkaNodePoolBuilder(POOL_B)
                .editSpec()
                    .withRoles(ProcessRoles.BROKER)
                    .withReplicas(0)
                .endSpec()
                .build();


        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.validateNodePools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolAWithReplicas, poolBWithoutReplicas), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true));
        assertThat(ex.getMessage(), containsString("At least one KafkaNodePool with the broker role and at least one replica is required when KRaft mode is enabled"));

        ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.validateNodePools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolAWithoutReplicas, poolBWithReplicas), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true));
        assertThat(ex.getMessage(), containsString("At least one KafkaNodePool with the controller role and at least one replica is required when KRaft mode is enabled"));
    }

    @Test
    public void testValidationNoPools()   {
        KafkaNodePool poolA = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withReplicas(0)
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.validateNodePools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolA), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false));
        assertThat(ex.getMessage(), is("KafkaNodePools are enabled, but KafkaNodePools for Kafka cluster my-cluster either don't exist or have 0 replicas. Please make sure at least one KafkaNodePool resource exists, is in the same namespace as the Kafka resource, has at least one replica, and has the strimzi.io/cluster label set to the name of the Kafka resource."));
    }

    @Test
    public void testValidationKRaftJbodStorage()   {
        KafkaVersionChange oldKafkaVersion = new KafkaVersionChange(KafkaVersionTestUtils.getKafkaVersionLookup().version("3.6.0"), KafkaVersionTestUtils.getKafkaVersionLookup().version("3.6.0"), null, null, "3.6");
        KafkaVersionChange inUpgradeKafkaVersion = new KafkaVersionChange(KafkaVersionTestUtils.getKafkaVersionLookup().version("3.6.0"), KafkaVersionTestUtils.getKafkaVersionLookup().version("3.7.0"), null, null, "3.6");
        KafkaVersionChange oldMetadataKafkaVersion = new KafkaVersionChange(KafkaVersionTestUtils.getKafkaVersionLookup().version("3.7.0"), KafkaVersionTestUtils.getKafkaVersionLookup().version("3.7.0"), null, null, "3.6-IV0");

        KafkaNodePool poolA = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withNewJbodStorage()
                        .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("200Gi").build(),
                                new PersistentClaimStorageBuilder().withId(1).withSize("200Gi").build())
                    .endJbodStorage()
                .endSpec()
                .build();

        // Kafka 3.7.0 or newer => should pass
        assertDoesNotThrow(() -> NodePoolUtils.validateNodePools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, poolA, POOL_B), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true));

        // Should pass on Kafka older than 3.7.0 without KRaft
        assertDoesNotThrow(() -> NodePoolUtils.validateNodePools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolA, POOL_B), oldKafkaVersion, false));

        // Should fail on Kafka older than 3.7.0 with KRaft
        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.validateNodePools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, poolA, POOL_B), oldKafkaVersion, true));
        assertThat(ex.getMessage(), containsString("The Kafka cluster my-cluster is invalid: [Using more than one disk in a JBOD storage in KRaft mode is supported only with Apache Kafka 3.7.0 or newer and metadata version 3.7-IV2 or newer (in KafkaNodePool pool-a)]"));

        // Should fail on Kafka during upgrade from 3.6.0 to 3.7.0 with KRaft
        ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.validateNodePools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, poolA, POOL_B), inUpgradeKafkaVersion, true));
        assertThat(ex.getMessage(), containsString("The Kafka cluster my-cluster is invalid: [Using more than one disk in a JBOD storage in KRaft mode is supported only with Apache Kafka 3.7.0 or newer and metadata version 3.7-IV2 or newer (in KafkaNodePool pool-a)]"));

        // Should fail when old metadata are used
        ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.validateNodePools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, poolA, POOL_B), oldMetadataKafkaVersion, true));
        assertThat(ex.getMessage(), containsString("The Kafka cluster my-cluster is invalid: [Using more than one disk in a JBOD storage in KRaft mode is supported only with Apache Kafka 3.7.0 or newer and metadata version 3.7-IV2 or newer (in KafkaNodePool pool-a)]"));
    }

    @Test
    public void testValidationOnlyPoolsWithZeroReplicas()   {
        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.validateNodePools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false));
        assertThat(ex.getMessage(), is("KafkaNodePools are enabled, but KafkaNodePools for Kafka cluster my-cluster either don't exist or have 0 replicas. Please make sure at least one KafkaNodePool resource exists, is in the same namespace as the Kafka resource, has at least one replica, and has the strimzi.io/cluster label set to the name of the Kafka resource."));
    }

    @Test
    public void testValidationIsCalledFromMainMethod()   {
        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER));
        assertThat(ex.getMessage(), is("KafkaNodePools are enabled, but KafkaNodePools for Kafka cluster my-cluster either don't exist or have 0 replicas. Please make sure at least one KafkaNodePool resource exists, is in the same namespace as the Kafka resource, has at least one replica, and has the strimzi.io/cluster label set to the name of the Kafka resource."));
    }

    @Test
    public void testGetClusterIdIfSetInKafka() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .withNewStatus()
                    .withClusterId("my-cluster-id")
                .endStatus()
                .build();

        KafkaNodePool poolA = new KafkaNodePoolBuilder(POOL_A)
                .withNewStatus()
                    .withClusterId("my-other-cluster-id")
                .endStatus()
                .build();

        // Not set in the predefined Kafka and no pools
        assertThat(NodePoolUtils.getClusterIdIfSet(KAFKA, null), is(nullValue()));

        // Not set in the predefined Kafka and not set in pools
        assertThat(NodePoolUtils.getClusterIdIfSet(KAFKA, List.of(POOL_A)), is(nullValue()));

        // Set in our custom Kafka
        assertThat(NodePoolUtils.getClusterIdIfSet(kafka, null), is("my-cluster-id"));

        // Not set in Kafka but set in node pool
        assertThat(NodePoolUtils.getClusterIdIfSet(KAFKA, List.of(poolA)), is("my-other-cluster-id"));

        // Not set in Kafka but set in one node pool
        assertThat(NodePoolUtils.getClusterIdIfSet(KAFKA, List.of(poolA, POOL_B)), is("my-other-cluster-id"));

        // Set in both Kafka and KafkaPool
        assertThat(NodePoolUtils.getClusterIdIfSet(kafka, List.of(poolA)), is("my-cluster-id"));

        // Set in both Kafka and one KafkaPool
        assertThat(NodePoolUtils.getClusterIdIfSet(kafka, List.of(poolA, POOL_B)), is("my-cluster-id"));
    }

    @Test
    public void testGetOrGenerateClusterId() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .withNewStatus()
                    .withClusterId("my-cluster-id")
                .endStatus()
                .build();

        KafkaNodePool poolA = new KafkaNodePoolBuilder(POOL_A)
                .withNewStatus()
                    .withClusterId("my-other-cluster-id")
                .endStatus()
                .build();

        // Not set in the predefined Kafka and no pools
        assertThat(NodePoolUtils.getOrGenerateKRaftClusterId(KAFKA, List.of(POOL_B)), is(notNullValue()));

        // Not set in the predefined Kafka and not in node pool
        assertThat(NodePoolUtils.getOrGenerateKRaftClusterId(KAFKA, List.of(POOL_B)), is(notNullValue()));

        // Set in our custom Kafka and no pools
        assertThat(NodePoolUtils.getOrGenerateKRaftClusterId(kafka, null), is("my-cluster-id"));

        // Set in our custom Kafka and not in pools
        assertThat(NodePoolUtils.getOrGenerateKRaftClusterId(kafka, List.of(poolA)), is("my-cluster-id"));

        // Set not in Kafka but in KafkaPool
        assertThat(NodePoolUtils.getOrGenerateKRaftClusterId(KAFKA, List.of(poolA)), is("my-other-cluster-id"));
    }
}
