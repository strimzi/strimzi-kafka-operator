/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.kafka.EphemeralStorage;
import io.strimzi.api.kafka.model.kafka.EphemeralStorageBuilder;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
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
import io.strimzi.operator.common.Reconciliation;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

public class KafkaSpecCheckerTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();
    private static final String NAMESPACE = "my-namespace";
    private static final String NAME = "my-cluster";
    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(NAME)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withNewKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName("tls")
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls()
                            .build())
                    .withConfig(Map.of(
                            KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 3,
                            TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, 2
                    ))
                .endKafka()
            .endSpec()
            .build();
    private static final KafkaNodePool MIXED = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("mixed")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withRoles(ProcessRoles.CONTROLLER, ProcessRoles.BROKER)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
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
                .withReplicas(3)
                .withRoles(ProcessRoles.BROKER)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
            .endSpec()
            .build();

    private KafkaSpecChecker generateChecker(Kafka kafka, List<KafkaNodePool> kafkaNodePools, KafkaVersionChange versionChange) {
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, kafkaNodePools, Map.of(), versionChange, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, versionChange, null, SHARED_ENV_PROVIDER);

        return new KafkaSpecChecker(kafka.getSpec(), VERSIONS, kafkaCluster);
    }

    @Test
    public void checkEmptyWarnings() {
        KafkaSpecChecker checker = generateChecker(KAFKA, List.of(CONTROLLERS, POOL_A), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE);
        assertThat(checker.run(), empty());

        checker = generateChecker(KAFKA, List.of(MIXED), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE);
        assertThat(checker.run(), empty());
    }

    @Test
    public void checkKafkaEphemeralStorageSingleBroker() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of(
                                // We want to avoid unrelated warnings
                                KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 1,
                                TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, 1
                        ))
                    .endKafka()
                .endSpec()
            .build();

        KafkaNodePool singleNode = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withReplicas(1)
                    .withStorage(new EphemeralStorage())
                .endSpec()
            .build();

        KafkaSpecChecker checker = generateChecker(kafka, List.of(CONTROLLERS, singleNode), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE);
        List<Condition> warnings = checker.run();
        assertThat(warnings, hasSize(1));
        Condition warning = warnings.get(0);
        assertThat(warning.getReason(), is("KafkaStorage"));
        assertThat(warning.getStatus(), is("True"));
        assertThat(warning.getMessage(), is("A Kafka cluster with a single broker node and ephemeral storage will lose topic messages after any restart or rolling update."));
    }

    @Test
    public void checkKafkaEphemeralStorageSingleController() {
        KafkaNodePool singleNode = new KafkaNodePoolBuilder(CONTROLLERS)
                .editSpec()
                    .withReplicas(1)
                    .withStorage(new EphemeralStorage())
                .endSpec()
            .build();

        KafkaSpecChecker checker = generateChecker(KAFKA, List.of(singleNode, POOL_A), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE);
        List<Condition> warnings = checker.run();
        assertThat(warnings, hasSize(1));
        Condition warning = warnings.get(0);
        assertThat(warning.getReason(), is("KafkaStorage"));
        assertThat(warning.getStatus(), is("True"));
        assertThat(warning.getMessage(), is("A Kafka cluster with a single controller node and ephemeral storage will lose data after any restart or rolling update."));
    }

    @Test
    public void checkKafkaEphemeralStorageSingleMixedNode() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of(
                                // We want to avoid unrelated warnings
                                KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 1,
                                TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, 1
                        ))
                    .endKafka()
                .endSpec()
            .build();

        KafkaNodePool singleNode = new KafkaNodePoolBuilder(MIXED)
                .editSpec()
                    .withReplicas(1)
                    .withStorage(new EphemeralStorage())
                .endSpec()
            .build();

        KafkaSpecChecker checker = generateChecker(kafka, List.of(singleNode), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE);
        List<Condition> warnings = checker.run();
        assertThat(warnings, hasSize(2));

        Condition warning = warnings.get(0);
        assertThat(warning.getReason(), is("KafkaStorage"));
        assertThat(warning.getStatus(), is("True"));
        assertThat(warning.getMessage(), is("A Kafka cluster with a single broker node and ephemeral storage will lose topic messages after any restart or rolling update."));

        warning = warnings.get(1);
        assertThat(warning.getReason(), is("KafkaStorage"));
        assertThat(warning.getStatus(), is("True"));
        assertThat(warning.getMessage(), is("A Kafka cluster with a single controller node and ephemeral storage will lose data after any restart or rolling update."));
    }

    @Test
    public void checkKafkaJbodEphemeralStorageSingleBroker() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of(
                                // We want to avoid unrelated warnings
                                KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 1,
                                TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, 1
                        ))
                    .endKafka()
                .endSpec()
            .build();

        KafkaNodePool singleNode = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withReplicas(1)
                    .withStorage(
                            new JbodStorageBuilder().withVolumes(
                                    new EphemeralStorageBuilder().withId(1).build(),
                                    new EphemeralStorageBuilder().withId(2).build()
                            ).build())
                .endSpec()
            .build();

        KafkaSpecChecker checker = generateChecker(kafka, List.of(CONTROLLERS, singleNode), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE);
        List<Condition> warnings = checker.run();
        assertThat(warnings, hasSize(1));
        Condition warning = warnings.get(0);
        assertThat(warning.getReason(), is("KafkaStorage"));
        assertThat(warning.getStatus(), is("True"));
        assertThat(warning.getMessage(), is("A Kafka cluster with a single broker node and ephemeral storage will lose topic messages after any restart or rolling update."));
    }

    @Test
    public void checkKafkaJbodEphemeralStorageSingleController() {
        KafkaNodePool singleNode = new KafkaNodePoolBuilder(CONTROLLERS)
                .editSpec()
                    .withReplicas(1)
                    .withStorage(
                            new JbodStorageBuilder().withVolumes(
                                    new EphemeralStorageBuilder().withId(1).build(),
                                    new EphemeralStorageBuilder().withId(2).build()
                            ).build())
                .endSpec()
            .build();

        KafkaSpecChecker checker = generateChecker(KAFKA, List.of(singleNode, POOL_A), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE);
        List<Condition> warnings = checker.run();
        assertThat(warnings, hasSize(1));
        Condition warning = warnings.get(0);
        assertThat(warning.getReason(), is("KafkaStorage"));
        assertThat(warning.getStatus(), is("True"));
        assertThat(warning.getMessage(), is("A Kafka cluster with a single controller node and ephemeral storage will lose data after any restart or rolling update."));
    }

    @Test
    public void checkKafkaJbodEphemeralStorageSingleMixedNode() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of(
                                // We want to avoid unrelated warnings
                                KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 1,
                                TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, 1
                        ))
                    .endKafka()
                .endSpec()
            .build();

        KafkaNodePool singleNode = new KafkaNodePoolBuilder(MIXED)
                .editSpec()
                    .withReplicas(1)
                    .withStorage(
                            new JbodStorageBuilder().withVolumes(
                                    new EphemeralStorageBuilder().withId(1).build(),
                                    new EphemeralStorageBuilder().withId(2).build()
                            ).build())
                .endSpec()
            .build();

        KafkaSpecChecker checker = generateChecker(kafka, List.of(singleNode), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE);
        List<Condition> warnings = checker.run();
        assertThat(warnings, hasSize(2));

        Condition warning = warnings.get(0);
        assertThat(warning.getReason(), is("KafkaStorage"));
        assertThat(warning.getStatus(), is("True"));
        assertThat(warning.getMessage(), is("A Kafka cluster with a single broker node and ephemeral storage will lose topic messages after any restart or rolling update."));

        warning = warnings.get(1);
        assertThat(warning.getReason(), is("KafkaStorage"));
        assertThat(warning.getStatus(), is("True"));
        assertThat(warning.getMessage(), is("A Kafka cluster with a single controller node and ephemeral storage will lose data after any restart or rolling update."));
    }

    @Test
    public void checkKafkaSingleBrokerIntentionalProducesNoWarning() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of(
                                // We want to avoid unrelated warnings
                                KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 1,
                                TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, 1
                        ))
                    .endKafka()
                .endSpec()
            .build();

        KafkaNodePool singleController = new KafkaNodePoolBuilder(CONTROLLERS)
                .editSpec()
                    .withReplicas(1)
                .endSpec()
            .build();

        KafkaNodePool singleBroker = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withReplicas(1)
                .endSpec()
            .build();

        KafkaSpecChecker checker = generateChecker(kafka, List.of(singleController, singleBroker), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE);
        List<Condition> warnings = checker.run();
        assertThat(warnings, hasSize(0));
    }

    @Test
    public void checkKafkaSingleMixedNodeIntentionalProducesNoWarning() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of(
                                // We want to avoid unrelated warnings
                                KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 1,
                                TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, 1
                        ))
                    .endKafka()
                .endSpec()
            .build();

        KafkaNodePool singleNode = new KafkaNodePoolBuilder(MIXED)
                .editSpec()
                    .withReplicas(1)
                .endSpec()
            .build();

        KafkaSpecChecker checker = generateChecker(kafka, List.of(singleNode), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE);
        List<Condition> warnings = checker.run();
        assertThat(warnings, hasSize(0));
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

        KafkaSpecChecker checker = generateChecker(kafka, List.of(CONTROLLERS, POOL_A), new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), null, null, KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION));

        List<Condition> warnings = checker.run();

        assertThat(warnings, hasSize(1));
        assertThat(warnings.get(0).getReason(), is("KafkaMetadataVersion"));
        assertThat(warnings.get(0).getMessage(), is("Metadata version is older than the Kafka version used by the cluster, which suggests that an upgrade is incomplete."));
    }

    @Test
    public void testMetadataVersionIsOlderThanDefaultKafkaVersion() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withMetadataVersion(KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION)
                    .endKafka()
                .endSpec()
                .build();

        KafkaSpecChecker checker = generateChecker(kafka, List.of(CONTROLLERS, POOL_A), new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), null, null, KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION));

        List<Condition> warnings = checker.run();

        assertThat(warnings, hasSize(1));
        assertThat(warnings.get(0).getReason(), is("KafkaMetadataVersion"));
        assertThat(warnings.get(0).getMessage(), is("Metadata version is older than the Kafka version used by the cluster, which suggests that an upgrade is incomplete."));
    }

    @Test
    public void testMetadataVersionIsOlderThanKafkaVersionWithLongVersion() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withVersion(KafkaVersionTestUtils.LATEST_KAFKA_VERSION)
                        .withMetadataVersion(KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION + "-IV0")
                    .endKafka()
                .endSpec()
                .build();

        KafkaSpecChecker checker = generateChecker(kafka, List.of(CONTROLLERS, POOL_A), new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), null, null, KafkaVersionTestUtils.PREVIOUS_METADATA_VERSION));

        List<Condition> warnings = checker.run();

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

        KafkaSpecChecker checker = generateChecker(kafka, List.of(CONTROLLERS, POOL_A), new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), null, null, KafkaVersionTestUtils.LATEST_METADATA_VERSION));

        List<Condition> warnings = checker.run();

        assertThat(warnings, hasSize(0));
    }

    @Test
    public void testMetadataVersionMatchesKafkaVersionWithDefaultKafkaVersion() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withMetadataVersion(KafkaVersionTestUtils.LATEST_METADATA_VERSION)
                    .endKafka()
                .endSpec()
                .build();

        KafkaSpecChecker checker = generateChecker(kafka, List.of(CONTROLLERS, POOL_A), new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), null, null, KafkaVersionTestUtils.LATEST_METADATA_VERSION));

        List<Condition> warnings = checker.run();

        assertThat(warnings, hasSize(0));
    }

    @Test
    public void testMetadataVersionMatchesKafkaVersionWithLongVersion() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withVersion(KafkaVersionTestUtils.LATEST_KAFKA_VERSION)
                        .withMetadataVersion(KafkaVersionTestUtils.LATEST_METADATA_VERSION + "-IV0")
                    .endKafka()
                .endSpec()
                .build();

        KafkaSpecChecker checker = generateChecker(kafka, List.of(CONTROLLERS, POOL_A), new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), null, null, KafkaVersionTestUtils.LATEST_METADATA_VERSION));

        List<Condition> warnings = checker.run();

        assertThat(warnings, hasSize(0));
    }

    @Test
    public void testUnusedConfigInKRaftBasedClusters() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .addToConfig(Map.of(
                                "inter.broker.protocol.version", "3.5",
                                "log.message.format.version", "3.5"
                        ))
                    .endKafka()
                .endSpec()
                .build();

        KafkaSpecChecker checker = generateChecker(kafka, List.of(CONTROLLERS, POOL_A), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE);

        List<Condition> warnings = checker.run();

        assertThat(warnings, hasSize(2));
        assertThat(warnings.get(0).getReason(), is("KafkaInterBrokerProtocolVersionInKRaft"));
        assertThat(warnings.get(0).getMessage(), is("inter.broker.protocol.version is not used in KRaft-based Kafka clusters and should be removed from the Kafka custom resource."));
        assertThat(warnings.get(1).getReason(), is("KafkaLogMessageFormatVersionInKRaft"));
        assertThat(warnings.get(1).getMessage(), is("log.message.format.version is not used in KRaft-based Kafka clusters and should be removed from the Kafka custom resource."));
    }

    @Test
    public void testKRaftWithTwoControllers() {
        KafkaNodePool controllers = new KafkaNodePoolBuilder(CONTROLLERS)
                .editSpec()
                    .withReplicas(2)
                .endSpec()
                .build();

        KafkaSpecChecker checker = generateChecker(KAFKA, List.of(controllers, POOL_A), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE);

        List<Condition> warnings = checker.run();

        assertThat(warnings, hasSize(1));
        assertThat(warnings.get(0).getReason(), is("KafkaKRaftControllerNodeCount"));
        assertThat(warnings.get(0).getMessage(), is("Running KRaft controller quorum with two nodes is not advisable as both nodes will be needed to avoid downtime. It is recommended that a minimum of three nodes are used."));
    }

    @Test
    public void testKRaftWithTwoMixedNodes() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of(
                                // We want to avoid unrelated warnings
                                KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 1,
                                TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, 1
                        ))
                    .endKafka()
                .endSpec()
            .build();

        KafkaNodePool mixed = new KafkaNodePoolBuilder(MIXED)
                .editSpec()
                    .withReplicas(2)
                .endSpec()
                .build();

        KafkaSpecChecker checker = generateChecker(kafka, List.of(mixed), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE);

        List<Condition> warnings = checker.run();

        assertThat(warnings, hasSize(1));
        assertThat(warnings.get(0).getReason(), is("KafkaKRaftControllerNodeCount"));
        assertThat(warnings.get(0).getMessage(), is("Running KRaft controller quorum with two nodes is not advisable as both nodes will be needed to avoid downtime. It is recommended that a minimum of three nodes are used."));
    }

    @Test
    public void testKRaftWithEvenNumberOfControllers() {
        KafkaNodePool controllers = new KafkaNodePoolBuilder(CONTROLLERS)
                .editSpec()
                    .withReplicas(4)
                .endSpec()
                .build();

        KafkaSpecChecker checker = generateChecker(KAFKA, List.of(controllers, POOL_A), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE);

        List<Condition> warnings = checker.run();

        assertThat(warnings, hasSize(1));
        assertThat(warnings.get(0).getReason(), is("KafkaKRaftControllerNodeCount"));
        assertThat(warnings.get(0).getMessage(), is("Running KRaft controller quorum with an odd number of nodes is recommended."));
    }

    @Test
    public void testKRaftWithEvenNumberOfMixedNodes() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of(
                                // We want to avoid unrelated warnings
                                KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 1,
                                TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, 1
                        ))
                    .endKafka()
                .endSpec()
                .build();

        KafkaNodePool mixed = new KafkaNodePoolBuilder(MIXED)
                .editSpec()
                    .withReplicas(4)
                .endSpec()
                .build();

        KafkaSpecChecker checker = generateChecker(kafka, List.of(mixed), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE);

        List<Condition> warnings = checker.run();

        assertThat(warnings, hasSize(1));
        assertThat(warnings.get(0).getReason(), is("KafkaKRaftControllerNodeCount"));
        assertThat(warnings.get(0).getMessage(), is("Running KRaft controller quorum with an odd number of nodes is recommended."));
    }

    @Test
    public void checkReplicationFactorAndMinInSyncReplicasSet() {
        KafkaSpecChecker checker = generateChecker(KAFKA, List.of(CONTROLLERS, POOL_A), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE);
        List<Condition> warnings = checker.run();
        assertThat(warnings, hasSize(0));
    }

    @Test
    public void checkReplicationFactorAndMinInSyncReplicasSetToOne() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of(
                                KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 1,
                                TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, 1
                        ))
                    .endKafka()
                .endSpec()
            .build();

        KafkaSpecChecker checker = generateChecker(kafka, List.of(CONTROLLERS, POOL_A), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE);
        List<Condition> warnings = checker.run();
        assertThat(warnings, hasSize(0));
    }

    @Test
    public void checkReplicationFactorAndMinInSyncReplicasNotSet() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of())
                    .endKafka()
                .endSpec()
            .build();

        KafkaSpecChecker checker = generateChecker(kafka, List.of(CONTROLLERS, POOL_A), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE);
        List<Condition> warnings = checker.run();
        assertThat(warnings, hasSize(2));

        assertThat(warnings.get(0).getReason(), is("KafkaDefaultReplicationFactor"));
        assertThat(warnings.get(0).getMessage(), is("default.replication.factor option is not configured. It defaults to 1 which does not guarantee reliability and availability. You should configure this option in .spec.kafka.config."));

        assertThat(warnings.get(1).getReason(), is("KafkaMinInsyncReplicas"));
        assertThat(warnings.get(1).getMessage(), is("min.insync.replicas option is not configured. It defaults to 1 which does not guarantee reliability and availability. You should configure this option in .spec.kafka.config."));
    }

    @Test
    public void checkKRaftMetadataConfigInKRaftMode() {
        // Kafka with Ephemeral storage
        KafkaNodePool ephemeralPool = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withNewEphemeralStorage()
                        .withKraftMetadata(KRaftMetadataStorage.SHARED)
                    .endEphemeralStorage()
                .endSpec()
                .build();

        KafkaSpecChecker checker = generateChecker(KAFKA, List.of(CONTROLLERS, ephemeralPool), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE);

        List<Condition> warnings = checker.run();
        assertThat(warnings, hasSize(0));

        // Kafka with Persistent storage
        KafkaNodePool persistentPool = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withNewPersistentClaimStorage()
                        .withSize("100Gi")
                        .withKraftMetadata(KRaftMetadataStorage.SHARED)
                    .endPersistentClaimStorage()
                .endSpec()
                .build();

        checker = generateChecker(KAFKA, List.of(CONTROLLERS, persistentPool), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE);

        warnings = checker.run();
        assertThat(warnings, hasSize(0));

        // Kafka with JBOD storage
        KafkaNodePool jbodPool = new KafkaNodePoolBuilder(POOL_A)
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

        checker = generateChecker(KAFKA, List.of(CONTROLLERS, jbodPool), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE);

        warnings = checker.run();
        assertThat(warnings, hasSize(0));
    }

    @Test
    public void checkWithoutKRaftMetadataConfigInKRaftModeProducesNoWarning() {
        // Kafka with Ephemeral storage
        KafkaNodePool ephemeralPool = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withNewEphemeralStorage()
                    .endEphemeralStorage()
                .endSpec()
                .build();

        KafkaSpecChecker checker = generateChecker(KAFKA, List.of(CONTROLLERS, ephemeralPool), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE);

        List<Condition> warnings = checker.run();
        assertThat(warnings, hasSize(0));

        // Kafka with Persistent storage
        KafkaNodePool persistentPool = new KafkaNodePoolBuilder(POOL_A)
                .editSpec()
                    .withNewPersistentClaimStorage()
                        .withSize("100Gi")
                    .endPersistentClaimStorage()
                .endSpec()
                .build();

        checker = generateChecker(KAFKA, List.of(CONTROLLERS, persistentPool), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE);

        warnings = checker.run();
        assertThat(warnings, hasSize(0));

        // Kafka with JBOD storage
        KafkaNodePool jbodPool = new KafkaNodePoolBuilder(POOL_A)
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

        checker = generateChecker(KAFKA, List.of(CONTROLLERS, jbodPool), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE);

        warnings = checker.run();
        assertThat(warnings, hasSize(0));
    }
}
