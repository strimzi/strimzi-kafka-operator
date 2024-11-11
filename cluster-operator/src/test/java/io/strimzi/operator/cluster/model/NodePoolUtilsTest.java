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
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.nodepools.NodePoolUtils;
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
    public void testNewNodePools()  {
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_A, POOL_B), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);

        assertThat(pools.size(), is(3));

        assertThat(pools.get(0).poolName, is("controllers"));
        assertThat(pools.get(0).processRoles, is(Set.of(ProcessRoles.CONTROLLER)));
        assertThat(pools.get(0).idAssignment.toBeAdded(), is(Set.of(0, 1, 2)));
        assertThat(pools.get(0).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.current(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.desired(), is(Set.of(0, 1, 2)));

        assertThat(pools.get(1).poolName, is("pool-a"));
        assertThat(pools.get(1).processRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(pools.get(1).idAssignment.toBeAdded(), is(Set.of(3, 4, 5)));
        assertThat(pools.get(1).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(1).idAssignment.current(), is(Set.of()));
        assertThat(pools.get(1).idAssignment.desired(), is(Set.of(3, 4, 5)));

        assertThat(pools.get(2).poolName, is("pool-b"));
        assertThat(pools.get(2).processRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(pools.get(2).idAssignment.toBeAdded(), is(Set.of(6, 7)));
        assertThat(pools.get(2).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(2).idAssignment.current(), is(Set.of()));
        assertThat(pools.get(2).idAssignment.desired(), is(Set.of(6, 7)));
    }

    @Test
    public void testExistingNodePools()  {
        KafkaNodePool poolControllers = new KafkaNodePoolBuilder(POOL_CONTROLLERS)
                .withNewStatus()
                    .withNodeIds(100, 101, 102)
                .endStatus()
                .build();

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
                CLUSTER_NAME + "-controllers", new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build(),
                CLUSTER_NAME + "-pool-a", new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build(),
                CLUSTER_NAME + "-pool-b", new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("200Gi").build()).build()
        );

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolControllers, poolA, poolB), existingStorage, Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);

        assertThat(pools.size(), is(3));

        assertThat(pools.get(0).poolName, is("controllers"));
        assertThat(pools.get(0).processRoles, is(Set.of(ProcessRoles.CONTROLLER)));
        assertThat(pools.get(0).idAssignment.toBeAdded(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.current(), is(Set.of(100, 101, 102)));
        assertThat(pools.get(0).idAssignment.desired(), is(Set.of(100, 101, 102)));

        JbodStorage storage = (JbodStorage) pools.get(0).storage;
        assertThat(((PersistentClaimStorage) storage.getVolumes().get(0)).getSize(), is("100Gi"));

        assertThat(pools.get(1).poolName, is("pool-a"));
        assertThat(pools.get(1).processRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(pools.get(1).idAssignment.toBeAdded(), is(Set.of()));
        assertThat(pools.get(1).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(1).idAssignment.current(), is(Set.of(0, 1, 2)));
        assertThat(pools.get(1).idAssignment.desired(), is(Set.of(0, 1, 2)));

        storage = (JbodStorage) pools.get(1).storage;
        assertThat(((PersistentClaimStorage) storage.getVolumes().get(0)).getSize(), is("100Gi"));

        assertThat(pools.get(2).poolName, is("pool-b"));
        assertThat(pools.get(2).processRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(pools.get(2).idAssignment.toBeAdded(), is(Set.of()));
        assertThat(pools.get(2).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(2).idAssignment.current(), is(Set.of(10, 11)));
        assertThat(pools.get(2).idAssignment.desired(), is(Set.of(10, 11)));

        storage = (JbodStorage) pools.get(2).storage;
        assertThat(((PersistentClaimStorage) storage.getVolumes().get(0)).getSize(), is("200Gi"));
    }

    @Test
    public void testExistingNodePoolsScaleUpDown()  {
        KafkaNodePool poolControllers = new KafkaNodePoolBuilder(POOL_CONTROLLERS)
                .withNewStatus()
                    .withNodeIds(100, 101, 102)
                .endStatus()
                .build();

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

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolControllers, poolA, poolB), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);

        assertThat(pools.size(), is(3));

        assertThat(pools.get(0).poolName, is("controllers"));
        assertThat(pools.get(0).processRoles, is(Set.of(ProcessRoles.CONTROLLER)));
        assertThat(pools.get(0).idAssignment.toBeAdded(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.current(), is(Set.of(100, 101, 102)));
        assertThat(pools.get(0).idAssignment.desired(), is(Set.of(100, 101, 102)));

        assertThat(pools.get(1).poolName, is("pool-a"));
        assertThat(pools.get(1).processRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(pools.get(1).idAssignment.toBeAdded(), is(Set.of()));
        assertThat(pools.get(1).idAssignment.toBeRemoved(), is(Set.of(2)));
        assertThat(pools.get(1).idAssignment.current(), is(Set.of(0, 1, 2)));
        assertThat(pools.get(1).idAssignment.desired(), is(Set.of(0, 1)));

        assertThat(pools.get(2).poolName, is("pool-b"));
        assertThat(pools.get(2).processRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(pools.get(2).idAssignment.toBeAdded(), is(Set.of(3)));
        assertThat(pools.get(2).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(2).idAssignment.current(), is(Set.of(10, 11)));
        assertThat(pools.get(2).idAssignment.desired(), is(Set.of(3, 10, 11)));
    }

    @Test
    public void testExistingNodePoolsScaleUpDownWithAnnotations()  {
        KafkaNodePool poolControllers = new KafkaNodePoolBuilder(POOL_CONTROLLERS)
                .withNewStatus()
                    .withNodeIds(100, 101, 102)
                .endStatus()
                .build();

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

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolControllers, poolA, poolB), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);

        assertThat(pools.size(), is(3));

        assertThat(pools.get(0).poolName, is("controllers"));
        assertThat(pools.get(0).processRoles, is(Set.of(ProcessRoles.CONTROLLER)));
        assertThat(pools.get(0).idAssignment.toBeAdded(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.current(), is(Set.of(100, 101, 102)));
        assertThat(pools.get(0).idAssignment.desired(), is(Set.of(100, 101, 102)));

        assertThat(pools.get(1).poolName, is("pool-a"));
        assertThat(pools.get(1).processRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(pools.get(1).idAssignment.toBeAdded(), is(Set.of()));
        assertThat(pools.get(1).idAssignment.toBeRemoved(), is(Set.of(12)));
        assertThat(pools.get(1).idAssignment.current(), is(Set.of(10, 11, 12)));
        assertThat(pools.get(1).idAssignment.desired(), is(Set.of(10, 11)));

        assertThat(pools.get(2).poolName, is("pool-b"));
        assertThat(pools.get(2).processRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(pools.get(2).idAssignment.toBeAdded(), is(Set.of(22)));
        assertThat(pools.get(2).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(2).idAssignment.current(), is(Set.of(20, 21)));
        assertThat(pools.get(2).idAssignment.desired(), is(Set.of(20, 21, 22)));
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
        KafkaNodePool poolControllers = new KafkaNodePoolBuilder(POOL_CONTROLLERS)
                .withNewStatus()
                    .withNodeIds(100, 101, 102)
                .endStatus()
                .build();

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
                CLUSTER_NAME + "-controllers", new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("150Gi").build()).build(),
                CLUSTER_NAME + "-pool-a", new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("200Gi").build()).build(),
                CLUSTER_NAME + "-pool-b", new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("1Ti").build()).build()
        );

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolControllers, poolA, poolB), existingStorage, Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER);

        assertThat(pools.size(), is(3));

        assertThat(pools.get(0).poolName, is("controllers"));
        assertThat(pools.get(0).processRoles, is(Set.of(ProcessRoles.CONTROLLER)));
        assertThat(pools.get(0).idAssignment.toBeAdded(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(0).idAssignment.current(), is(Set.of(100, 101, 102)));
        assertThat(pools.get(0).idAssignment.desired(), is(Set.of(100, 101, 102)));

        JbodStorage storage = (JbodStorage) pools.get(0).storage;
        assertThat(((PersistentClaimStorage) storage.getVolumes().get(0)).getSize(), is("150Gi"));

        assertThat(pools.get(1).poolName, is("pool-a"));
        assertThat(pools.get(1).processRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(pools.get(1).idAssignment.toBeAdded(), is(Set.of()));
        assertThat(pools.get(1).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(1).idAssignment.current(), is(Set.of(0, 1, 2)));
        assertThat(pools.get(1).idAssignment.desired(), is(Set.of(0, 1, 2)));

        storage = (JbodStorage) pools.get(1).storage;
        assertThat(((PersistentClaimStorage) storage.getVolumes().get(0)).getSize(), is("200Gi"));

        assertThat(pools.get(2).poolName, is("pool-b"));
        assertThat(pools.get(2).processRoles, is(Set.of(ProcessRoles.BROKER)));
        assertThat(pools.get(2).idAssignment.toBeAdded(), is(Set.of()));
        assertThat(pools.get(2).idAssignment.toBeRemoved(), is(Set.of()));
        assertThat(pools.get(2).idAssignment.current(), is(Set.of(10, 11)));
        assertThat(pools.get(2).idAssignment.desired(), is(Set.of(10, 11)));

        storage = (JbodStorage) pools.get(2).storage;
        assertThat(((PersistentClaimStorage) storage.getVolumes().get(0)).getSize(), is("1Ti"));
    }

    @Test
    public void testValidationWithNoRoles()   {
        KafkaNodePool poolA = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withRoles()
                .endSpec()
                .build();

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.validateNodePools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolA), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true));
        assertThat(ex.getMessage(), containsString("KafkaNodePool pool-a has no role defined in .spec.roles"));
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

        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.validateNodePools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolA), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true));
        assertThat(ex.getMessage(), is("KafkaNodePools are enabled, but KafkaNodePools for Kafka cluster my-cluster either don't exist or have 0 replicas. Please make sure at least one KafkaNodePool resource exists, is in the same namespace as the Kafka resource, has at least one replica, and has the strimzi.io/cluster label set to the name of the Kafka resource."));
    }

    @Test
    public void testValidationOnlyPoolsWithZeroReplicas()   {
        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.validateNodePools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true));
        assertThat(ex.getMessage(), is("KafkaNodePools are enabled, but KafkaNodePools for Kafka cluster my-cluster either don't exist or have 0 replicas. Please make sure at least one KafkaNodePool resource exists, is in the same namespace as the Kafka resource, has at least one replica, and has the strimzi.io/cluster label set to the name of the Kafka resource."));
    }

    @Test
    public void testValidationIsCalledFromMainMethod()   {
        InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, true, SHARED_ENV_PROVIDER));
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
