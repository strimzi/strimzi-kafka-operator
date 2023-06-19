/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.JbodStorageBuilder;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.operator.cluster.model.nodepools.NodePoolUtils;
import io.strimzi.operator.cluster.model.nodepools.VirtualNodePoolConverter;
import io.strimzi.operator.cluster.operator.resource.MockSharedEnvironmentProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.cluster.operator.resource.SharedEnvironmentProvider;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
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
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, null, Map.of(), Map.of(), false, SHARED_ENV_PROVIDER);

        assertThat(pools.size(), is(1));
        assertThat(pools.get(0).poolName, is(VirtualNodePoolConverter.DEFAULT_NODE_POOL_NAME));
        assertThat(pools.get(0).kraftRoles, is(Set.of(ProcessRoles.BROKER)));
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

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, null, Map.of(), existingPods, false, SHARED_ENV_PROVIDER);

        assertThat(pools.size(), is(1));
        assertThat(pools.get(0).poolName, is(VirtualNodePoolConverter.DEFAULT_NODE_POOL_NAME));
        assertThat(pools.get(0).kraftRoles, is(Set.of(ProcessRoles.BROKER)));
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

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, null, Map.of(), existingPods, false, SHARED_ENV_PROVIDER);

        assertThat(pools.size(), is(1));
        assertThat(pools.get(0).poolName, is(VirtualNodePoolConverter.DEFAULT_NODE_POOL_NAME));
        assertThat(pools.get(0).kraftRoles, is(Set.of(ProcessRoles.BROKER)));
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

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, null, existingStorage, existingPods, false, SHARED_ENV_PROVIDER);

        assertThat(pools.size(), is(1));
        assertThat(pools.get(0).poolName, is(VirtualNodePoolConverter.DEFAULT_NODE_POOL_NAME));

        JbodStorage storage = (JbodStorage) pools.get(0).storage;
        assertThat(((PersistentClaimStorage) storage.getVolumes().get(0)).getSize(), is("1Ti"));
    }

    @Test
    public void testNewVirtualNodePoolWithKRaft()  {
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, null, Map.of(), Map.of(), true, SHARED_ENV_PROVIDER);

        assertThat(pools.size(), is(1));
        assertThat(pools.get(0).poolName, is(VirtualNodePoolConverter.DEFAULT_NODE_POOL_NAME));
        assertThat(pools.get(0).kraftRoles, is(Set.of(ProcessRoles.BROKER, ProcessRoles.CONTROLLER)));
        assertThat(pools.get(0).idAssignment.toBeAdded(), is(Set.of(0, 1, 2)));
        assertThat(pools.get(0).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.current(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.desired(), is(Set.of(0, 1, 2)));
    }

    @Test
    public void testNewNodePools()  {
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_A, POOL_B), Map.of(), Map.of(), false, SHARED_ENV_PROVIDER);

        assertThat(pools.size(), is(2));

        assertThat(pools.get(0).poolName, is("pool-a"));
        assertThat(pools.get(0).kraftRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(pools.get(0).idAssignment.toBeAdded(), is(Set.of(0, 1, 2)));
        assertThat(pools.get(0).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.current(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.desired(), is(Set.of(0, 1, 2)));

        assertThat(pools.get(1).poolName, is("pool-b"));
        assertThat(pools.get(1).kraftRoles, is(Set.of(ProcessRoles.BROKER)));
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

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolA, poolB), existingStorage, Map.of(), false, SHARED_ENV_PROVIDER);

        assertThat(pools.size(), is(2));

        assertThat(pools.get(0).poolName, is("pool-a"));
        assertThat(pools.get(0).kraftRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(pools.get(0).idAssignment.toBeAdded(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.current(), is(Set.of(0, 1, 2)));
        assertThat(pools.get(0).idAssignment.desired(), is(Set.of(0, 1, 2)));

        JbodStorage storage = (JbodStorage) pools.get(0).storage;
        assertThat(((PersistentClaimStorage) storage.getVolumes().get(0)).getSize(), is("100Gi"));

        assertThat(pools.get(1).poolName, is("pool-b"));
        assertThat(pools.get(1).kraftRoles, is(Set.of(ProcessRoles.BROKER)));
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

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolA, poolB), Map.of(), Map.of(), false, SHARED_ENV_PROVIDER);

        assertThat(pools.size(), is(2));

        assertThat(pools.get(0).poolName, is("pool-a"));
        assertThat(pools.get(0).kraftRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(pools.get(0).idAssignment.toBeAdded(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.toBeRemoved(), is(Set.of(2)));
        assertThat(pools.get(0).idAssignment.current(), is(Set.of(0, 1, 2)));
        assertThat(pools.get(0).idAssignment.desired(), is(Set.of(0, 1)));

        assertThat(pools.get(1).poolName, is("pool-b"));
        assertThat(pools.get(1).kraftRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(pools.get(1).idAssignment.toBeAdded(), is(Set.of(12)));
        assertThat(pools.get(1).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(1).idAssignment.current(), is(Set.of(10, 11)));
        assertThat(pools.get(1).idAssignment.desired(), is(Set.of(10, 11, 12)));
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

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolA, poolB), Map.of(), Map.of(), true, SHARED_ENV_PROVIDER);

        assertThat(pools.size(), is(2));

        assertThat(pools.get(0).poolName, is("pool-a"));
        assertThat(pools.get(0).kraftRoles, is(Set.of(ProcessRoles.BROKER, ProcessRoles.CONTROLLER)));

        assertThat(pools.get(1).poolName, is("pool-b"));
        assertThat(pools.get(1).kraftRoles, is(Set.of(ProcessRoles.BROKER, ProcessRoles.CONTROLLER)));
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

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolA, poolB), Map.of(), Map.of(), true, SHARED_ENV_PROVIDER);

        assertThat(pools.size(), is(2));

        assertThat(pools.get(0).poolName, is("pool-a"));
        assertThat(pools.get(0).kraftRoles, is(Set.of(ProcessRoles.CONTROLLER)));

        assertThat(pools.get(1).poolName, is("pool-b"));
        assertThat(pools.get(1).kraftRoles, is(Set.of(ProcessRoles.BROKER)));
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

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolA, poolB), existingStorage, Map.of(), false, SHARED_ENV_PROVIDER);

        assertThat(pools.size(), is(2));

        assertThat(pools.get(0).poolName, is("pool-a"));
        assertThat(pools.get(0).kraftRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(pools.get(0).idAssignment.toBeAdded(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.current(), is(Set.of(0, 1, 2)));
        assertThat(pools.get(0).idAssignment.desired(), is(Set.of(0, 1, 2)));

        JbodStorage storage = (JbodStorage) pools.get(0).storage;
        assertThat(((PersistentClaimStorage) storage.getVolumes().get(0)).getSize(), is("100Gi"));

        assertThat(pools.get(1).poolName, is("pool-b"));
        assertThat(pools.get(1).kraftRoles, is(Set.of(ProcessRoles.BROKER)));
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

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.validateNodePools(KAFKA, List.of(poolA), false));
        assertThat(ex.getMessage(), containsString("KafkaNodePool pool-a has no role defined in .spec.roles"));
    }

    @Test
    public void testValidationZooKeeperBasedWithMixedRoles()   {
        KafkaNodePool poolA = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withRoles(ProcessRoles.BROKER, ProcessRoles.CONTROLLER)
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.validateNodePools(KAFKA, List.of(poolA), false));
        assertThat(ex.getMessage(), containsString("KafkaNodePool pool-a contains invalid roles configuration. In a ZooKeeper-based Kafka cluster, the KafkaNodePool role has to be always set only to the 'broker' role."));
    }

    @Test
    public void testValidationZooKeeperBasedWithControllerRole()   {
        KafkaNodePool poolA = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withRoles(ProcessRoles.CONTROLLER)
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.validateNodePools(KAFKA, List.of(poolA), false));
        assertThat(ex.getMessage(), containsString("KafkaNodePool pool-a contains invalid roles configuration. In a ZooKeeper-based Kafka cluster, the KafkaNodePool role has to be always set only to the 'broker' role."));
    }

    @Test
    public void testKRaftValidationWithNoRoles()   {
        KafkaNodePool poolA = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                .withRoles()
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.validateNodePools(KAFKA, List.of(poolA), true));
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


        assertDoesNotThrow(() -> NodePoolUtils.validateNodePools(KAFKA, List.of(poolA, poolB), true));
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


        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.validateNodePools(KAFKA, List.of(poolA), true));
        assertThat(ex.getMessage(), containsString("At least one KafkaNodePool with the broker role and at least one replica is required when KRaft mode is enabled"));

        ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.validateNodePools(KAFKA, List.of(poolB), true));
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


        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.validateNodePools(KAFKA, List.of(poolAWithReplicas, poolBWithoutReplicas), true));
        assertThat(ex.getMessage(), containsString("At least one KafkaNodePool with the broker role and at least one replica is required when KRaft mode is enabled"));

        ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.validateNodePools(KAFKA, List.of(poolAWithoutReplicas, poolBWithReplicas), true));
        assertThat(ex.getMessage(), containsString("At least one KafkaNodePool with the controller role and at least one replica is required when KRaft mode is enabled"));
    }

    @Test
    public void testValidationNoPools()   {
        KafkaNodePool poolA = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withReplicas(0)
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.validateNodePools(KAFKA, List.of(poolA), false));
        assertThat(ex.getMessage(), is("KafkaNodePools are enabled, but the KafkaNodePool for Kafka cluster my-cluster either don't exists or have 0 replicas. Please make sure at least one KafkaNodePool resource exists, is in the same namespace as the Kafka resource, has at least one replica, and has the strimzi.io/cluster label set to the name of the Kafka resource."));
    }

    @Test
    public void testValidationOnlyPoolsWithZEroReplicas()   {
        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.validateNodePools(KAFKA, List.of(), false));
        assertThat(ex.getMessage(), is("KafkaNodePools are enabled, but the KafkaNodePool for Kafka cluster my-cluster either don't exists or have 0 replicas. Please make sure at least one KafkaNodePool resource exists, is in the same namespace as the Kafka resource, has at least one replica, and has the strimzi.io/cluster label set to the name of the Kafka resource."));
    }

    @Test
    public void testValidationIsCalledFromMainMethod()   {
        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(), Map.of(), Map.of(), false, SHARED_ENV_PROVIDER));
        assertThat(ex.getMessage(), is("KafkaNodePools are enabled, but the KafkaNodePool for Kafka cluster my-cluster either don't exists or have 0 replicas. Please make sure at least one KafkaNodePool resource exists, is in the same namespace as the Kafka resource, has at least one replica, and has the strimzi.io/cluster label set to the name of the Kafka resource."));
    }
}
