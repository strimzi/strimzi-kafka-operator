/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.kafka.EphemeralStorage;
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

public class KafkaSpecCheckerWithNodePoolsTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();
    private static final String NAMESPACE = "ns";
    private static final String NAME = "foo";

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

    private static final KafkaNodePool CONTROLLERS = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("controllers")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withRoles(ProcessRoles.CONTROLLER)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
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
    public void checkReplicationFactorAndMinInsyncReplicasNotSet() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of())
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(POOL_A, POOL_B), Map.of(), Map.of(), false, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);
        KafkaSpecChecker checker = new KafkaSpecChecker(kafka.getSpec(), VERSIONS, kafkaCluster);

        List<Condition> warnings = checker.run(false);

        assertThat(warnings, hasSize(2));
        assertThat(warnings.stream().anyMatch(w -> w.getMessage().contains(KafkaConfiguration.DEFAULT_REPLICATION_FACTOR)), is(true));
        assertThat(warnings.stream().anyMatch(w -> w.getMessage().contains(KafkaConfiguration.MIN_INSYNC_REPLICAS)), is(true));
    }

    @Test
    public void checkReplicationFactorAndMinInsyncReplicasNotSetWithOnlyOneBrokerNode() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of())
                    .endKafka()
                .endSpec()
                .build();

        KafkaNodePool poolA = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withReplicas(1)
                    .withRoles(ProcessRoles.CONTROLLER)
                .endSpec()
                .build();

        KafkaNodePool poolB = new KafkaNodePoolBuilder(POOL_B)
                .editSpec()
                    .withReplicas(1)
                    .withRoles(ProcessRoles.BROKER)
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(poolA, poolB), Map.of(), Map.of(), true, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);
        KafkaSpecChecker checker = new KafkaSpecChecker(kafka.getSpec(), VERSIONS, kafkaCluster);

        List<Condition> warnings = checker.run(true);

        // Only one broker node => No warnings
        assertThat(warnings, hasSize(0));
    }

    @Test
    public void testKRaftWithTwoControllers() {
        KafkaNodePool poolA = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withReplicas(2)
                    .withRoles(ProcessRoles.CONTROLLER, ProcessRoles.BROKER)
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolA), Map.of(), Map.of(), true, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);
        KafkaSpecChecker checker = new KafkaSpecChecker(KAFKA.getSpec(), VERSIONS, kafkaCluster);

        List<Condition> warnings = checker.run(true);

        assertThat(warnings, hasSize(1));
        assertThat(warnings.get(0).getReason(), is("KafkaKRaftControllerNodeCount"));
        assertThat(warnings.get(0).getMessage(), is("Running KRaft controller quorum with two nodes is not advisable as both nodes will be needed to avoid downtime. It is recommended that a minimum of three nodes are used."));
    }

    @Test
    public void testKRaftWithEvenNumberOfControllers() {
        KafkaNodePool poolA = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withReplicas(4)
                    .withRoles(ProcessRoles.CONTROLLER, ProcessRoles.BROKER)
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolA), Map.of(), Map.of(), true, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);
        KafkaSpecChecker checker = new KafkaSpecChecker(KAFKA.getSpec(), VERSIONS, kafkaCluster);

        List<Condition> warnings = checker.run(true);

        assertThat(warnings, hasSize(1));
        assertThat(warnings.get(0).getReason(), is("KafkaKRaftControllerNodeCount"));
        assertThat(warnings.get(0).getMessage(), is("Running KRaft controller quorum with an odd number of nodes is recommended."));
    }

    @Test
    public void testSingleKRaftControllerWithEphemeralStorage() {
        KafkaNodePool poolA = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withReplicas(1)
                    .withRoles(ProcessRoles.CONTROLLER)
                    .withStorage(new EphemeralStorage())
                .endSpec()
                .build();

        KafkaNodePool poolB = new KafkaNodePoolBuilder(POOL_B)
                .editSpec()
                    .withReplicas(5)
                    .withRoles(ProcessRoles.BROKER)
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(poolA, poolB), Map.of(), Map.of(), true, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);
        KafkaSpecChecker checker = new KafkaSpecChecker(KAFKA.getSpec(), VERSIONS, kafkaCluster);

        List<Condition> warnings = checker.run(true);

        // Only one broker node => No warnings
        assertThat(warnings, hasSize(1));
        assertThat(warnings.get(0).getReason(), is("KafkaStorage"));
        assertThat(warnings.get(0).getMessage(), is("A Kafka cluster with a single controller node and ephemeral storage will lose data after any restart or rolling update."));
    }

    @Test
    public void testMetadataVersionIsOlderThanKafkaVersion() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withVersion(KafkaVersionTestUtils.LATEST_KAFKA_VERSION)
                        .withMetadataVersion(KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION)
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(CONTROLLERS, POOL_A, POOL_B), Map.of(), Map.of(), true, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), null, null, KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION), KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);
        KafkaSpecChecker checker = new KafkaSpecChecker(kafka.getSpec(), VERSIONS, kafkaCluster);

        List<Condition> warnings = checker.run(true);

        assertThat(warnings, hasSize(1));
        assertThat(warnings.get(0).getReason(), is("KafkaMetadataVersion"));
        assertThat(warnings.get(0).getMessage(), is("Metadata version is older than the Kafka version used by the cluster, which suggests that an upgrade is incomplete."));
    }

    @Test
    public void testMetadataVersionMatchesKafkaVersion() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withVersion(KafkaVersionTestUtils.LATEST_KAFKA_VERSION)
                        .withMetadataVersion(KafkaVersionTestUtils.LATEST_METADATA_VERSION)
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(CONTROLLERS, POOL_A, POOL_B), Map.of(), Map.of(), true, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), null, null, KafkaVersionTestUtils.LATEST_METADATA_VERSION), KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);
        KafkaSpecChecker checker = new KafkaSpecChecker(kafka.getSpec(), VERSIONS, kafkaCluster);

        List<Condition> warnings = checker.run(true);

        assertThat(warnings, hasSize(0));
    }

    @Test
    public void testUnusedConfigInKRaftBasedClusters() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .addToConfig(Map.of("inter.broker.protocol.version", "3.5", "log.message.format.version", "3.5"))
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, List.of(CONTROLLERS, POOL_A, POOL_B), Map.of(), Map.of(), true, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, KafkaMetadataConfigurationState.KRAFT, null, SHARED_ENV_PROVIDER);
        KafkaSpecChecker checker = new KafkaSpecChecker(kafka.getSpec(), VERSIONS, kafkaCluster);

        List<Condition> warnings = checker.run(true);

        assertThat(warnings, hasSize(2));
        assertThat(warnings.get(0).getReason(), is("KafkaInterBrokerProtocolVersionInKRaft"));
        assertThat(warnings.get(0).getMessage(), is("inter.broker.protocol.version is not used in KRaft-based Kafka clusters and should be removed from the Kafka custom resource."));
        assertThat(warnings.get(1).getReason(), is("KafkaLogMessageFormatVersionInKRaft"));
        assertThat(warnings.get(1).getMessage(), is("log.message.format.version is not used in KRaft-based Kafka clusters and should be removed from the Kafka custom resource."));
    }
}
