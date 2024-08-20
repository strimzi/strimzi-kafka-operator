/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.kafka.KRaftMetadataStorage;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.nodepools.NodePoolUtils;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

public class KafkaSpecCheckerZooBasedWithNodePoolsTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();
    private static final String NAMESPACE = "my-namespace";
    private static final String NAME = "my-cluster";
    private static final Kafka KAFKA = new KafkaBuilder()
                .withNewMetadata()
                    .withName(NAME)
                    .withNamespace(NAMESPACE)
                    .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled"))
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withListeners(new GenericKafkaListenerBuilder().withName("tls").withPort(9093).withType(KafkaListenerType.INTERNAL).withTls().build())
                        .withConfig(Map.of("default.replication.factor", 2, "min.insync.replicas", 2))
                    .endKafka()
                .endSpec()
                .build();
    private static final KafkaNodePool POOL_A = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("pool-a")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(1)
                .withRoles(ProcessRoles.BROKER)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
            .endSpec()
            .build();
    private static final KafkaNodePool POOL_B = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("pool-b")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(1)
                .withRoles(ProcessRoles.BROKER)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
            .endSpec()
            .build();

    @Test
    public void checkReplicationFactorAndMinInSyncReplicasNotSet() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of())
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_A, POOL_B), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        KafkaSpecChecker checker = new KafkaSpecChecker(kafka.getSpec(), VERSIONS, kafkaCluster);

        List<Condition> warnings = checker.run(false);

        assertThat(warnings, hasSize(2));
        assertThat(warnings.stream().anyMatch(w -> w.getMessage().contains(KafkaConfiguration.DEFAULT_REPLICATION_FACTOR)), is(true));
        assertThat(warnings.stream().anyMatch(w -> w.getMessage().contains(KafkaConfiguration.MIN_INSYNC_REPLICAS)), is(true));
    }

    @Test
    public void checkReplicationFactorAndMinInSyncReplicasNotSetWithOnlyOneBrokerNode() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of())
                    .endKafka()
                .endSpec()
                .build();

        KafkaNodePool pool = new KafkaNodePoolBuilder(POOL_B)
                .editSpec()
                    .withReplicas(1)
                    .withRoles(ProcessRoles.BROKER)
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(pool), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        KafkaSpecChecker checker = new KafkaSpecChecker(kafka.getSpec(), VERSIONS, kafkaCluster);

        List<Condition> warnings = checker.run(false);

        // Only one broker node => No warnings
        assertThat(warnings, hasSize(0));
    }

    @Test
    public void checkKRaftMetadataConfigInZooKeeperMode() {
        // Kafka with Ephemeral storage
        KafkaNodePool ephemeralPool = new KafkaNodePoolBuilder(POOL_B)
                .editSpec()
                    .withNewEphemeralStorage()
                        .withKraftMetadata(KRaftMetadataStorage.SHARED)
                    .endEphemeralStorage()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_A, ephemeralPool), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        KafkaSpecChecker checker = new KafkaSpecChecker(KAFKA.getSpec(), VERSIONS, kafkaCluster);

        List<Condition> warnings = checker.run(false);
        assertThat(warnings, hasSize(1));
        assertThat(warnings.get(0).getReason(), is("KRaftMetadataStorageConfiguredWithoutKRaft"));
        assertThat(warnings.get(0).getMessage(), is("The Kafka custom resource or one or more of the KafkaNodePool custom resources contain the kraftMetadata configuration. This configuration is supported only for KRaft-based Kafka clusters."));

        // Kafka with Persistent storage
        KafkaNodePool persistentPool = new KafkaNodePoolBuilder(POOL_B)
                .editSpec()
                    .withNewPersistentClaimStorage()
                        .withSize("100Gi")
                        .withKraftMetadata(KRaftMetadataStorage.SHARED)
                    .endPersistentClaimStorage()
                .endSpec()
                .build();

        pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_A, persistentPool), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        checker = new KafkaSpecChecker(KAFKA.getSpec(), VERSIONS, kafkaCluster);

        warnings = checker.run(false);
        assertThat(warnings, hasSize(1));
        assertThat(warnings.get(0).getReason(), is("KRaftMetadataStorageConfiguredWithoutKRaft"));
        assertThat(warnings.get(0).getMessage(), is("The Kafka custom resource or one or more of the KafkaNodePool custom resources contain the kraftMetadata configuration. This configuration is supported only for KRaft-based Kafka clusters."));

        // Kafka with JBOD storage
        KafkaNodePool jbodPool = new KafkaNodePoolBuilder(POOL_B)
                .editSpec()
                    .withNewJbodStorage()
                        .addNewPersistentClaimStorageVolume()
                            .withId(0)
                            .withSize("100Gi")
                        .endPersistentClaimStorageVolume()
                        .addNewPersistentClaimStorageVolume()
                            .withId(0)
                            .withSize("100Gi")
                            .withKraftMetadata(KRaftMetadataStorage.SHARED)
                        .endPersistentClaimStorageVolume()
                    .endJbodStorage()
                .endSpec()
                .build();

        pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_A, jbodPool), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        checker = new KafkaSpecChecker(KAFKA.getSpec(), VERSIONS, kafkaCluster);

        warnings = checker.run(false);
        assertThat(warnings, hasSize(1));
        assertThat(warnings.get(0).getReason(), is("KRaftMetadataStorageConfiguredWithoutKRaft"));
        assertThat(warnings.get(0).getMessage(), is("The Kafka custom resource or one or more of the KafkaNodePool custom resources contain the kraftMetadata configuration. This configuration is supported only for KRaft-based Kafka clusters."));
    }

    @Test
    public void checkKRaftMetadataConfigNotUsedInZooKeeperMode() {
        // Kafka with Ephemeral storage
        KafkaNodePool ephemeralPool = new KafkaNodePoolBuilder(POOL_B)
                .editSpec()
                    .withNewEphemeralStorage()
                    .endEphemeralStorage()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_A, ephemeralPool), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        KafkaSpecChecker checker = new KafkaSpecChecker(KAFKA.getSpec(), VERSIONS, kafkaCluster);

        List<Condition> warnings = checker.run(false);
        assertThat(warnings, hasSize(0));

        // Kafka with Persistent storage
        KafkaNodePool persistentPool = new KafkaNodePoolBuilder(POOL_B)
                .editSpec()
                    .withNewPersistentClaimStorage()
                        .withSize("100Gi")
                    .endPersistentClaimStorage()
                .endSpec()
                .build();

        pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_A, persistentPool), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        checker = new KafkaSpecChecker(KAFKA.getSpec(), VERSIONS, kafkaCluster);

        warnings = checker.run(false);
        assertThat(warnings, hasSize(0));

        // Kafka with JBOD storage
        KafkaNodePool jbodPool = new KafkaNodePoolBuilder(POOL_B)
                .editSpec()
                    .withNewJbodStorage()
                        .addNewPersistentClaimStorageVolume()
                            .withId(0)
                            .withSize("100Gi")
                        .endPersistentClaimStorageVolume()
                        .addNewPersistentClaimStorageVolume()
                            .withId(0)
                            .withSize("100Gi")
                        .endPersistentClaimStorageVolume()
                    .endJbodStorage()
                .endSpec()
                .build();

        pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_A, jbodPool), Map.of(), Map.of(), KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, false, SHARED_ENV_PROVIDER);
        kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        checker = new KafkaSpecChecker(KAFKA.getSpec(), VERSIONS, kafkaCluster);

        warnings = checker.run(false);
        assertThat(warnings, hasSize(0));
    }
}
