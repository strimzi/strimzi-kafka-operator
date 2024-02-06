/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.kafka.EphemeralStorage;
import io.strimzi.api.kafka.model.kafka.EphemeralStorageBuilder;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.nodepools.NodePoolUtils;
import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

public class KafkaSpecCheckerTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();

    private static final String NAMESPACE = "ns";
    private static final String NAME = "foo";
    private static final String IMAGE = "image";
    private static final int HEALTH_DELAY = 120;
    private static final int HEALTH_TIMEOUT = 30;

    private KafkaSpecChecker generateChecker(Kafka kafka, KafkaVersionChange versionChange) {
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafka, null, Map.of(), Map.of(), false, SHARED_ENV_PROVIDER);
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, pools, VERSIONS, versionChange, KafkaMetadataConfigurationState.ZK, null, SHARED_ENV_PROVIDER);

        return new KafkaSpecChecker(kafka.getSpec(), VERSIONS, kafkaCluster);
    }

    @Test
    public void checkEmptyWarnings() {
        Map<String, Object> kafkaOptions = new HashMap<>();
        kafkaOptions.put(KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 3);
        kafkaOptions.put(KafkaConfiguration.MIN_INSYNC_REPLICAS, 2);

        Kafka kafka = ResourceUtils.createKafka(NAMESPACE, NAME, 3, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT,
                null, kafkaOptions, emptyMap(),
                new EphemeralStorage(), new EphemeralStorage(), null, null, null, null);

        KafkaSpecChecker checker = generateChecker(kafka, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE);
        assertThat(checker.run(false), empty());
    }

    @Test
    public void checkKafkaStorage() {
        Kafka kafka = new KafkaBuilder(ResourceUtils.createKafka(NAMESPACE, NAME, 1, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT,
                null, emptyMap(), emptyMap(),
            new EphemeralStorage(), new EphemeralStorage(), null, null, null, null))
                .editSpec()
                    .editZookeeper()
                        .withReplicas(3)
                    .endZookeeper()
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
        Kafka kafka = new KafkaBuilder(ResourceUtils.createKafka(NAMESPACE, NAME, 1, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT,
                null, emptyMap(), emptyMap(),
            new JbodStorageBuilder().withVolumes(new EphemeralStorageBuilder().withId(1).build(),
                                                 new EphemeralStorageBuilder().withId(2).build()).build(),
            new EphemeralStorage(), null, null, null, null))
                .editSpec()
                    .editZookeeper()
                        .withReplicas(3)
                    .endZookeeper()
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
        Map<String, Object> kafkaOptions = new HashMap<>();
        kafkaOptions.put(KafkaConfiguration.LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION);
        kafkaOptions.put(KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 3);
        kafkaOptions.put(KafkaConfiguration.MIN_INSYNC_REPLICAS, 2);

        Kafka kafka = new KafkaBuilder(ResourceUtils.createKafka(NAMESPACE, NAME, 3, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT,
                null, kafkaOptions, emptyMap(),
            new EphemeralStorage(), new EphemeralStorage(), null, null, null, null))
                .editSpec()
                    .editKafka()
                        .withVersion(KafkaVersionTestUtils.LATEST_KAFKA_VERSION)
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
        Map<String, Object> kafkaOptions = new HashMap<>();
        kafkaOptions.put(KafkaConfiguration.LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION);
        kafkaOptions.put(KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 3);
        kafkaOptions.put(KafkaConfiguration.MIN_INSYNC_REPLICAS, 2);

        Kafka kafka = ResourceUtils.createKafka(NAMESPACE, NAME, 3, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT,
                null, kafkaOptions, emptyMap(),
            new EphemeralStorage(), new EphemeralStorage(), null, null, null, null);

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
        Map<String, Object> kafkaOptions = new HashMap<>();
        kafkaOptions.put(KafkaConfiguration.LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.LATEST_FORMAT_VERSION);
        kafkaOptions.put(KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 3);
        kafkaOptions.put(KafkaConfiguration.MIN_INSYNC_REPLICAS, 2);

        Kafka kafka = ResourceUtils.createKafka(NAMESPACE, NAME, 3, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT,
                null, kafkaOptions, emptyMap(),
            new EphemeralStorage(), new EphemeralStorage(), null, null, null, null);

        KafkaSpecChecker checker = generateChecker(kafka, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE);
        List<Condition> warnings = checker.run(false);
        assertThat(warnings, hasSize(0));
    }

    @Test
    public void checkLogMessageFormatWithRightLongVersion() {
        Map<String, Object> kafkaOptions = new HashMap<>();
        kafkaOptions.put(KafkaConfiguration.LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.LATEST_FORMAT_VERSION + "-IV0");
        kafkaOptions.put(KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 3);
        kafkaOptions.put(KafkaConfiguration.MIN_INSYNC_REPLICAS, 2);

        Kafka kafka = ResourceUtils.createKafka(NAMESPACE, NAME, 3, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT,
                null, kafkaOptions, emptyMap(),
            new EphemeralStorage(), new EphemeralStorage(), null, null, null, null);

        KafkaSpecChecker checker = generateChecker(kafka, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE);
        List<Condition> warnings = checker.run(false);
        assertThat(warnings, hasSize(0));
    }

    @Test
    public void checkInterBrokerProtocolVersion() {
        Map<String, Object> kafkaOptions = new HashMap<>();
        kafkaOptions.put(KafkaConfiguration.INTERBROKER_PROTOCOL_VERSION, KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION);
        kafkaOptions.put(KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 3);
        kafkaOptions.put(KafkaConfiguration.MIN_INSYNC_REPLICAS, 2);

        Kafka kafka = new KafkaBuilder(ResourceUtils.createKafka(NAMESPACE, NAME, 3, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT,
                null, kafkaOptions, emptyMap(),
            new EphemeralStorage(), new EphemeralStorage(), null, null, null, null))
                .editSpec()
                    .editKafka()
                        .withVersion(KafkaVersionTestUtils.LATEST_KAFKA_VERSION)
                        .withListeners(new GenericKafkaListenerBuilder().withName("plain").withPort(9092).withTls(false).withType(KafkaListenerType.INTERNAL).build())
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
        Map<String, Object> kafkaOptions = new HashMap<>();
        kafkaOptions.put(KafkaConfiguration.INTERBROKER_PROTOCOL_VERSION, KafkaVersionTestUtils.PREVIOUS_PROTOCOL_VERSION);
        kafkaOptions.put(KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 3);
        kafkaOptions.put(KafkaConfiguration.MIN_INSYNC_REPLICAS, 2);

        Kafka kafka = ResourceUtils.createKafka(NAMESPACE, NAME, 3, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT,
                null, kafkaOptions, emptyMap(),
            new EphemeralStorage(), new EphemeralStorage(), null, null, null, null);

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
        Map<String, Object> kafkaOptions = new HashMap<>();
        kafkaOptions.put(KafkaConfiguration.INTERBROKER_PROTOCOL_VERSION, KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION);
        kafkaOptions.put(KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 3);
        kafkaOptions.put(KafkaConfiguration.MIN_INSYNC_REPLICAS, 2);

        Kafka kafka = ResourceUtils.createKafka(NAMESPACE, NAME, 3, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT,
                null, kafkaOptions, emptyMap(),
            new EphemeralStorage(), new EphemeralStorage(), null, null, null, null);

        KafkaSpecChecker checker = generateChecker(kafka, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE);
        List<Condition> warnings = checker.run(false);
        assertThat(warnings, hasSize(0));
    }

    @Test
    public void checkInterBrokerProtocolWithCorrectLongVersion() {
        Map<String, Object> kafkaOptions = new HashMap<>();
        kafkaOptions.put(KafkaConfiguration.INTERBROKER_PROTOCOL_VERSION, KafkaVersionTestUtils.LATEST_PROTOCOL_VERSION + "-IV0");
        kafkaOptions.put(KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 3);
        kafkaOptions.put(KafkaConfiguration.MIN_INSYNC_REPLICAS, 2);

        Kafka kafka = ResourceUtils.createKafka(NAMESPACE, NAME, 3, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT,
                null, kafkaOptions, emptyMap(),
            new EphemeralStorage(), new EphemeralStorage(), null, null, null, null);

        KafkaSpecChecker checker = generateChecker(kafka, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE);
        List<Condition> warnings = checker.run(false);
        assertThat(warnings, hasSize(0));
    }

    @Test
    public void checkMultipleWarnings() {
        Kafka kafka = ResourceUtils.createKafka(NAMESPACE, NAME, 3, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT,
                null, emptyMap(), emptyMap(),
                new EphemeralStorage(), new EphemeralStorage(), null, null, null, null);

        KafkaSpecChecker checker = generateChecker(kafka, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE);
        List<Condition> warnings = checker.run(false);
        assertThat(warnings, hasSize(2));
    }

    @Test
    public void checkReplicationFactorAndMinInsyncReplicasSet() {
        Map<String, Object> kafkaOptions = new HashMap<>();
        kafkaOptions.put(KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 3);
        kafkaOptions.put(KafkaConfiguration.MIN_INSYNC_REPLICAS, 2);

        Kafka kafka = ResourceUtils.createKafka(NAMESPACE, NAME, 3, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT,
                null, kafkaOptions, emptyMap(),
                new EphemeralStorage(), new EphemeralStorage(), null, null, null, null);

        KafkaSpecChecker checker = generateChecker(kafka, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE);
        List<Condition> warnings = checker.run(false);
        assertThat(warnings, hasSize(0));
    }

    @Test
    public void checkReplicationFactorAndMinInsyncReplicasSetToOne() {
        Map<String, Object> kafkaOptions = new HashMap<>();
        kafkaOptions.put(KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 1);
        kafkaOptions.put(KafkaConfiguration.MIN_INSYNC_REPLICAS, 1);

        Kafka kafka = ResourceUtils.createKafka(NAMESPACE, NAME, 3, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT,
                null, kafkaOptions, emptyMap(),
                new EphemeralStorage(), new EphemeralStorage(), null, null, null, null);

        KafkaSpecChecker checker = generateChecker(kafka, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE);
        List<Condition> warnings = checker.run(false);
        assertThat(warnings, hasSize(0));
    }

    @Test
    public void checkReplicationFactorAndMinInsyncReplicasUnsetOnSingleNode() {
        Kafka kafka = ResourceUtils.createKafka(NAMESPACE, NAME, 1, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT,
                null, emptyMap(), emptyMap(),
                new EphemeralStorage(), new EphemeralStorage(), null, null, null, null);

        KafkaSpecChecker checker = generateChecker(kafka, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE);
        List<Condition> warnings = checker.run(false);
        // One warning is generated, but not the one we are testing here
        assertThat(warnings, hasSize(1));
        assertThat(warnings.stream().anyMatch(w -> w.getMessage().contains(KafkaConfiguration.DEFAULT_REPLICATION_FACTOR)), is(false));
        assertThat(warnings.stream().anyMatch(w -> w.getMessage().contains(KafkaConfiguration.MIN_INSYNC_REPLICAS)), is(false));
    }

    @Test
    public void checkReplicationFactorAndMinInsyncReplicasNotSet() {
        Kafka kafka = ResourceUtils.createKafka(NAMESPACE, NAME, 3, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT,
                null, emptyMap(), emptyMap(),
                new EphemeralStorage(), new EphemeralStorage(), null, null, null, null);

        KafkaSpecChecker checker = generateChecker(kafka, KafkaVersionTestUtils.DEFAULT_ZOOKEEPER_VERSION_CHANGE);
        List<Condition> warnings = checker.run(false);
        assertThat(warnings, hasSize(2));
        assertThat(warnings.stream().anyMatch(w -> w.getMessage().contains(KafkaConfiguration.DEFAULT_REPLICATION_FACTOR)), is(true));
        assertThat(warnings.stream().anyMatch(w -> w.getMessage().contains(KafkaConfiguration.MIN_INSYNC_REPLICAS)), is(true));
    }
}
