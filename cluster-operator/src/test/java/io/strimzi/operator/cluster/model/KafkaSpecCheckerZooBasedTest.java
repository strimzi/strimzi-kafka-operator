/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.kafka.EphemeralStorageBuilder;
import io.strimzi.api.kafka.model.kafka.JbodStorage;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.KRaftMetadataStorage;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorage;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.nodepools.NodePoolUtils;
import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

public class KafkaSpecCheckerZooBasedTest {
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
                    .withReplicas(3)
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName("tls")
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls()
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

    private KafkaSpecChecker generateChecker(Kafka kafka, KafkaVersionChange versionChange) {
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, null, Map.of(), Map.of(), versionChange, false, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, versionChange, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        return new KafkaSpecChecker(kafka.getSpec(), VERSIONS, kafkaCluster);
    }

    @Test
    public void checkEmptyWarnings() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of(
                                KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 3,
                                KafkaConfiguration.MIN_INSYNC_REPLICAS, 2
                        ))
                    .endKafka()
                .endSpec()
                .build();
        KafkaSpecChecker checker = generateChecker(kafka, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE);
        assertThat(checker.run(false), empty());
    }

    @Test
    public void checkKafkaStorage() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withReplicas(1)
                    .endKafka()
                .endSpec()
            .build();

        KafkaSpecChecker checker = generateChecker(kafka, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE);
        List<Condition> warnings = checker.run(false);
        assertThat(warnings, hasSize(1));
        Condition warning = warnings.get(0);
        assertThat(warning.getReason(), is("KafkaStorage"));
        assertThat(warning.getStatus(), is("True"));
        assertThat(warning.getMessage(), is("A Kafka cluster with a single broker node and ephemeral storage will lose topic messages after any restart or rolling update."));
    }

    @Test
    public void checkKafkaJbodStorage() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withReplicas(1)
                        .withStorage(
                                new JbodStorageBuilder().withVolumes(
                                        new EphemeralStorageBuilder().withId(1).build(),
                                        new EphemeralStorageBuilder().withId(2).build()
                                ).build())
                    .endKafka()
                .endSpec()
            .build();

        KafkaSpecChecker checker = generateChecker(kafka, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE);
        List<Condition> warnings = checker.run(false);
        assertThat(warnings, hasSize(1));
        Condition warning = warnings.get(0);
        assertThat(warning.getReason(), is("KafkaStorage"));
        assertThat(warning.getStatus(), is("True"));
        assertThat(warning.getMessage(), is("A Kafka cluster with a single broker node and ephemeral storage will lose topic messages after any restart or rolling update."));
    }

    @Test
    public void checkLogMessageFormatVersion() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withVersion(KafkaVersionTestUtils.LATEST_KAFKA_VERSION)
                        .withConfig(Map.of(
                                KafkaConfiguration.LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                                KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 3,
                                KafkaConfiguration.MIN_INSYNC_REPLICAS, 2
                        ))
                    .endKafka()
                .endSpec()
            .build();

        KafkaSpecChecker checker = generateChecker(kafka, new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), VERSIONS.defaultVersion().protocolVersion(), KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION, null));
        List<Condition> warnings = checker.run(false);
        assertThat(warnings, hasSize(1));
        Condition warning = warnings.get(0);
        assertThat(warning.getReason(), is("KafkaLogMessageFormatVersion"));
        assertThat(warning.getStatus(), is("True"));
        assertThat(warning.getMessage(), is("log.message.format.version does not match the Kafka cluster version, which suggests that an upgrade is incomplete."));
    }

    @Test
    public void checkLogMessageFormatWithoutVersion() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of(
                                KafkaConfiguration.LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION,
                                KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 3,
                                KafkaConfiguration.MIN_INSYNC_REPLICAS, 2
                        ))
                    .endKafka()
                .endSpec()
            .build();

        KafkaSpecChecker checker = generateChecker(kafka, new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), VERSIONS.defaultVersion().protocolVersion(), KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION, null));
        List<Condition> warnings = checker.run(false);
        assertThat(warnings, hasSize(1));
        Condition warning = warnings.get(0);
        assertThat(warning.getReason(), is("KafkaLogMessageFormatVersion"));
        assertThat(warning.getStatus(), is("True"));
        assertThat(warning.getMessage(), is("log.message.format.version does not match the Kafka cluster version, which suggests that an upgrade is incomplete."));
    }

    @Test
    public void checkLogMessageFormatWithRightVersion() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of(
                                KafkaConfiguration.LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.LATEST_FORMAT_VERSION,
                                KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 3,
                                KafkaConfiguration.MIN_INSYNC_REPLICAS, 2
                        ))
                    .endKafka()
                .endSpec()
            .build();

        KafkaSpecChecker checker = generateChecker(kafka, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE);
        List<Condition> warnings = checker.run(false);
        assertThat(warnings, hasSize(0));
    }

    @Test
    public void checkLogMessageFormatWithRightLongVersion() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of(
                                KafkaConfiguration.LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.LATEST_FORMAT_VERSION + "-IV0",
                                KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 3,
                                KafkaConfiguration.MIN_INSYNC_REPLICAS, 2
                        ))
                    .endKafka()
                .endSpec()
            .build();

        KafkaSpecChecker checker = generateChecker(kafka, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE);
        List<Condition> warnings = checker.run(false);
        assertThat(warnings, hasSize(0));
    }

    @Test
    public void checkInterBrokerProtocolVersion() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withVersion(KafkaVersionTestUtils.LATEST_KAFKA_VERSION)
                        .withConfig(Map.of(
                                KafkaConfiguration.INTERBROKER_PROTOCOL_VERSION, KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION,
                                KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 3,
                                KafkaConfiguration.MIN_INSYNC_REPLICAS, 2
                        ))
                    .endKafka()
                .endSpec()
            .build();

        KafkaSpecChecker checker = generateChecker(kafka, new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION, VERSIONS.defaultVersion().messageVersion(), null));
        List<Condition> warnings = checker.run(false);
        assertThat(warnings, hasSize(1));
        Condition warning = warnings.get(0);
        assertThat(warning.getReason(), is("KafkaInterBrokerProtocolVersion"));
        assertThat(warning.getStatus(), is("True"));
        assertThat(warning.getMessage(), is("inter.broker.protocol.version does not match the Kafka cluster version, which suggests that an upgrade is incomplete."));
    }

    @Test
    public void checkInterBrokerProtocolWithoutVersion() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of(
                                KafkaConfiguration.INTERBROKER_PROTOCOL_VERSION, KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION,
                                KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 3,
                                KafkaConfiguration.MIN_INSYNC_REPLICAS, 2
                        ))
                    .endKafka()
                .endSpec()
            .build();

        KafkaSpecChecker checker = generateChecker(kafka, new KafkaVersionChange(VERSIONS.defaultVersion(), VERSIONS.defaultVersion(), KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION, VERSIONS.defaultVersion().messageVersion(), null));
        List<Condition> warnings = checker.run(false);
        assertThat(warnings, hasSize(1));
        Condition warning = warnings.get(0);
        assertThat(warning.getReason(), is("KafkaInterBrokerProtocolVersion"));
        assertThat(warning.getStatus(), is("True"));
        assertThat(warning.getMessage(), is("inter.broker.protocol.version does not match the Kafka cluster version, which suggests that an upgrade is incomplete."));
    }

    @Test
    public void checkInterBrokerProtocolWithCorrectVersion() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of(
                                KafkaConfiguration.INTERBROKER_PROTOCOL_VERSION, KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION,
                                KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 3,
                                KafkaConfiguration.MIN_INSYNC_REPLICAS, 2
                        ))
                    .endKafka()
                .endSpec()
            .build();

        KafkaSpecChecker checker = generateChecker(kafka, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE);
        List<Condition> warnings = checker.run(false);
        assertThat(warnings, hasSize(0));
    }

    @Test
    public void checkInterBrokerProtocolWithCorrectLongVersion() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of(
                                KafkaConfiguration.INTERBROKER_PROTOCOL_VERSION, KafkaVersionTestUtils.LATEST_FORMAT_VERSION + "-IV0",
                                KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 3,
                                KafkaConfiguration.MIN_INSYNC_REPLICAS, 2
                        ))
                    .endKafka()
                .endSpec()
            .build();

        KafkaSpecChecker checker = generateChecker(kafka, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE);
        List<Condition> warnings = checker.run(false);
        assertThat(warnings, hasSize(0));
    }

    @Test
    public void checkMultipleWarnings() {
        KafkaSpecChecker checker = generateChecker(KAFKA, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE);
        List<Condition> warnings = checker.run(false);
        assertThat(warnings, hasSize(2));
    }

    @Test
    public void checkReplicationFactorAndMinInSyncReplicasSet() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of(
                                KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 3,
                                KafkaConfiguration.MIN_INSYNC_REPLICAS, 2
                        ))
                    .endKafka()
                .endSpec()
            .build();

        KafkaSpecChecker checker = generateChecker(kafka, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE);
        List<Condition> warnings = checker.run(false);
        assertThat(warnings, hasSize(0));
    }

    @Test
    public void checkReplicationFactorAndMinInSyncReplicasSetToOne() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of(
                                KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 1,
                                KafkaConfiguration.MIN_INSYNC_REPLICAS, 1
                        ))
                    .endKafka()
                .endSpec()
            .build();

        KafkaSpecChecker checker = generateChecker(kafka, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE);
        List<Condition> warnings = checker.run(false);
        assertThat(warnings, hasSize(0));
    }

    @Test
    public void checkReplicationFactorAndMinInSyncReplicasUnsetOnSingleNode() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withReplicas(1)
                    .endKafka()
                .endSpec()
            .build();

        KafkaSpecChecker checker = generateChecker(kafka, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE);
        List<Condition> warnings = checker.run(false);
        // One warning is generated, but not the one we are testing here
        assertThat(warnings, hasSize(1));
        assertThat(warnings.stream().anyMatch(w -> w.getMessage().contains(KafkaConfiguration.DEFAULT_REPLICATION_FACTOR)), is(false));
        assertThat(warnings.stream().anyMatch(w -> w.getMessage().contains(KafkaConfiguration.MIN_INSYNC_REPLICAS)), is(false));
    }

    @Test
    public void checkReplicationFactorAndMinInSyncReplicasNotSet() {
        KafkaSpecChecker checker = generateChecker(KAFKA, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE);
        List<Condition> warnings = checker.run(false);
        assertThat(warnings, hasSize(2));
        assertThat(warnings.stream().anyMatch(w -> w.getMessage().contains(KafkaConfiguration.DEFAULT_REPLICATION_FACTOR)), is(true));
        assertThat(warnings.stream().anyMatch(w -> w.getMessage().contains(KafkaConfiguration.MIN_INSYNC_REPLICAS)), is(true));
    }

    @Test
    public void checkKRaftMetadataConfigInZooKeeperMode() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of(
                                // Set to avoid unrelated warnings being raised here
                                KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 3,
                                KafkaConfiguration.MIN_INSYNC_REPLICAS, 2
                        ))
                        .withNewEphemeralStorage()
                            .withKraftMetadata(KRaftMetadataStorage.SHARED)
                        .endEphemeralStorage()
                    .endKafka()
                .endSpec()
            .build();

        KafkaSpecChecker checker = generateChecker(kafka, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE);

        List<Condition> warnings = checker.run(false);
        assertThat(warnings, hasSize(1));
        assertThat(warnings.get(0).getReason(), is("KRaftMetadataStorageConfiguredWithoutKRaft"));
        assertThat(warnings.get(0).getMessage(), is("The Kafka custom resource or one or more of the KafkaNodePool custom resources contain the kraftMetadata configuration. This configuration is supported only for KRaft-based Kafka clusters."));

        // Check Persistent storage
        PersistentClaimStorage persistentStorage = new PersistentClaimStorageBuilder()
                .withSize("100Gi")
                .withKraftMetadata(KRaftMetadataStorage.SHARED)
                .build();
        kafka.getSpec().getKafka().setStorage(persistentStorage);
        checker = generateChecker(kafka, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE);

        warnings = checker.run(false);
        assertThat(warnings, hasSize(1));
        assertThat(warnings.get(0).getReason(), is("KRaftMetadataStorageConfiguredWithoutKRaft"));
        assertThat(warnings.get(0).getMessage(), is("The Kafka custom resource or one or more of the KafkaNodePool custom resources contain the kraftMetadata configuration. This configuration is supported only for KRaft-based Kafka clusters."));

        // Check JBOD storage
        JbodStorage jbodStorage = new JbodStorageBuilder()
                .withVolumes(new PersistentClaimStorageBuilder()
                                .withId(0)
                                .withSize("100Gi")
                                .build(),
                        new PersistentClaimStorageBuilder()
                                .withId(1)
                                .withSize("100Gi")
                                .withKraftMetadata(KRaftMetadataStorage.SHARED)
                                .build())
                .build();
        kafka.getSpec().getKafka().setStorage(jbodStorage);
        checker = generateChecker(kafka, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE);

        warnings = checker.run(false);
        assertThat(warnings, hasSize(1));
        assertThat(warnings.get(0).getReason(), is("KRaftMetadataStorageConfiguredWithoutKRaft"));
        assertThat(warnings.get(0).getMessage(), is("The Kafka custom resource or one or more of the KafkaNodePool custom resources contain the kraftMetadata configuration. This configuration is supported only for KRaft-based Kafka clusters."));
    }

    @Test
    public void checkKRaftMetadataConfigNotUsedInZooKeeperMode() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withConfig(Map.of(
                                // Set to avoid unrelated warnings being raised here
                                KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 3,
                                KafkaConfiguration.MIN_INSYNC_REPLICAS, 2
                        ))
                    .endKafka()
                .endSpec()
            .build();
        KafkaSpecChecker checker = generateChecker(kafka, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE);

        List<Condition> warnings = checker.run(false);
        assertThat(warnings, hasSize(0));

        // Check Persistent storage
        PersistentClaimStorage persistentStorage = new PersistentClaimStorageBuilder()
                .withSize("100Gi")
                .build();
        kafka.getSpec().getKafka().setStorage(persistentStorage);
        checker = generateChecker(kafka, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE);

        warnings = checker.run(false);
        assertThat(warnings, hasSize(0));

        // Check JBOD storage
        JbodStorage jbodStorage = new JbodStorageBuilder()
                .withVolumes(new PersistentClaimStorageBuilder()
                                .withId(0)
                                .withSize("100Gi")
                                .build(),
                        new PersistentClaimStorageBuilder()
                                .withId(1)
                                .withSize("100Gi")
                                .build())
                .build();
        kafka.getSpec().getKafka().setStorage(jbodStorage);
        checker = generateChecker(kafka, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE);

        warnings = checker.run(false);
        assertThat(warnings, hasSize(0));
    }
}
