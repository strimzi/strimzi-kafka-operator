/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.storage.EphemeralStorage;
import io.strimzi.api.kafka.model.storage.EphemeralStorageBuilder;
import io.strimzi.api.kafka.model.storage.JbodStorageBuilder;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaConfiguration;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

public class KafkaSpecCheckerTest {

    private static final String NAMESPACE = "ns";
    private static final String NAME = "foo";
    private static final String IMAGE = "image";
    private static final int HEALTH_DELAY = 120;
    private static final int HEALTH_TIMEOUT = 30;

    private KafkaSpecChecker generateChecker(Kafka kafka) {
        KafkaVersion.Lookup versions = new KafkaVersion.Lookup(
                new StringReader(KafkaVersionTestUtils.getKafkaVersionYaml()),
                KafkaVersionTestUtils.getKafkaImageMap(),
                emptyMap(),
                emptyMap(),
                emptyMap(),
                emptyMap()) { };
        KafkaCluster kafkaCluster = KafkaCluster.fromCrd(kafka, versions);
        ZookeeperCluster zkCluster = ZookeeperCluster.fromCrd(kafka, versions);
        return new KafkaSpecChecker(kafka.getSpec(), kafkaCluster, zkCluster);
    }

    @Test
    public void checkEmptyWarnings() {
        Kafka kafka = ResourceUtils.createKafka(NAMESPACE, NAME, 3, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT);
        KafkaSpecChecker checker = generateChecker(kafka);
        assertThat(checker.run(), empty());
    }

    @Test
    public void checkKafkaStorage() {
        Kafka kafka = new KafkaBuilder(ResourceUtils.createKafka(NAMESPACE, NAME, 1, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT,
            Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
            new EphemeralStorage(), new EphemeralStorage(), null, null, null, null))
                .editSpec()
                    .editZookeeper()
                        .withReplicas(3)
                    .endZookeeper()
                .endSpec()
            .build();
        KafkaSpecChecker checker = generateChecker(kafka);
        List<Condition> warnings = checker.run();
        assertThat(warnings, hasSize(1));
        Condition warning = warnings.get(0);
        assertThat(warning.getReason(), is("KafkaStorage"));
        assertThat(warning.getStatus(), is("True"));
        assertThat(warning.getMessage(), is("A Kafka cluster with a single replica and ephemeral storage will lose topic messages after any restart or rolling update."));
    }

    @Test
    public void checkKafkaJbodStorage() {
        Kafka kafka = new KafkaBuilder(ResourceUtils.createKafka(NAMESPACE, NAME, 1, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT,
            Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
            new JbodStorageBuilder().withVolumes(new EphemeralStorageBuilder().withId(1).build(),
                                                 new EphemeralStorageBuilder().withId(2).build()).build(),
            new EphemeralStorage(), null, null, null, null))
                .editSpec()
                    .editZookeeper()
                        .withReplicas(3)
                    .endZookeeper()
                .endSpec()
            .build();
        KafkaSpecChecker checker = generateChecker(kafka);
        List<Condition> warnings = checker.run();
        assertThat(warnings, hasSize(1));
        Condition warning = warnings.get(0);
        assertThat(warning.getReason(), is("KafkaStorage"));
        assertThat(warning.getStatus(), is("True"));
        assertThat(warning.getMessage(), is("A Kafka cluster with a single replica and ephemeral storage will lose topic messages after any restart or rolling update."));
    }

    @Test
    public void checkZookeeperStorage() {
        Kafka kafka = new KafkaBuilder(ResourceUtils.createKafka(NAMESPACE, NAME, 3, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT,
            Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
            new EphemeralStorage(), new EphemeralStorage(), null, null, null, null))
                .editSpec()
                    .editZookeeper()
                        .withReplicas(1)
                    .endZookeeper()
                .endSpec()
            .build();
        KafkaSpecChecker checker = generateChecker(kafka);
        List<Condition> warnings = checker.run();
        assertThat(warnings, hasSize(1));
        Condition warning = warnings.get(0);
        assertThat(warning.getReason(), is("ZooKeeperStorage"));
        assertThat(warning.getStatus(), is("True"));
        assertThat(warning.getMessage(), is("A ZooKeeper cluster with a single replica and ephemeral storage will be in a defective state after any restart or rolling update. It is recommended that a minimum of three replicas are used."));
    }

    @Test
    public void checkZookeeperReplicas() {
        Kafka kafka = ResourceUtils.createKafka(NAMESPACE, NAME, 2, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT);
        KafkaSpecChecker checker = generateChecker(kafka);
        List<Condition> warnings = checker.run();
        assertThat(warnings, hasSize(1));
        Condition warning = warnings.get(0);
        assertThat(warning.getReason(), is("ZooKeeperReplicas"));
        assertThat(warning.getStatus(), is("True"));
        assertThat(warning.getMessage(), is("Running ZooKeeper with two nodes is not advisable as both replicas will be needed to avoid downtime. It is recommended that a minimum of three replicas are used."));
    }

    @Test
    public void checkZookeeperEvenReplicas() {
        Kafka kafka = ResourceUtils.createKafka(NAMESPACE, NAME, 4, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT);
        KafkaSpecChecker checker = generateChecker(kafka);
        List<Condition> warnings = checker.run();
        assertThat(warnings, hasSize(1));
        Condition warning = warnings.get(0);
        assertThat(warning.getReason(), is("ZooKeeperReplicas"));
        assertThat(warning.getStatus(), is("True"));
        assertThat(warning.getMessage(), is("Running ZooKeeper with an odd number of replicas is recommended."));
    }

    @Test
    public void checkKafkaVersion() {
        Map<String, Object> kafkaOptions = new HashMap<>();
        kafkaOptions.put(KafkaConfiguration.LOG_MESSAGE_FORMAT_VERSION, KafkaVersionTestUtils.PREVIOUS_FORMAT_VERSION);
        Kafka kafka = new KafkaBuilder(ResourceUtils.createKafka(NAMESPACE, NAME, 3, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT,
            Collections.emptyMap(), kafkaOptions, Collections.emptyMap(),
            new EphemeralStorage(), new EphemeralStorage(), null, null, null, null))
                .editSpec()
                    .editKafka()
                        .withVersion(KafkaVersionTestUtils.LATEST_KAFKA_VERSION)
                    .endKafka()
                .endSpec()
            .build();
        KafkaSpecChecker checker = generateChecker(kafka);
        List<Condition> warnings = checker.run();
        assertThat(warnings, hasSize(1));
        Condition warning = warnings.get(0);
        assertThat(warning.getReason(), is("KafkaLogMessageFormatVersion"));
        assertThat(warning.getStatus(), is("True"));
        assertThat(warning.getMessage(), is("log.message.format.version does not match the Kafka cluster version, which suggests that an upgrade is incomplete."));
    }

    @Test
    public void checkMultipleWarnings() {
        Kafka kafka = ResourceUtils.createKafka(NAMESPACE, NAME, 1, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT,
                Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
                new EphemeralStorage(), new EphemeralStorage(), null, null, null, null);
        KafkaSpecChecker checker = generateChecker(kafka);
        List<Condition> warnings = checker.run();
        assertThat(warnings, hasSize(2));
    }

    private Date dateSupplier() {
        return Date.from(LocalDateTime.of(2018, 11, 26, 9, 12, 0).atZone(ZoneId.of("GMT")).toInstant());
    }
}
