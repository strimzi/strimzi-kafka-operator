/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.kafka.EphemeralStorage;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
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

public class ZooKeeperSpecCheckerTest {
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();
    private static final String NAMESPACE = "ns";
    private static final String NAME = "foo";
    private static final String IMAGE = "image";
    private static final int HEALTH_DELAY = 120;
    private static final int HEALTH_TIMEOUT = 30;

    private ZooKeeperSpecChecker generateChecker(Kafka kafka) {
        KafkaVersion.Lookup versions = KafkaVersionTestUtils.getKafkaVersionLookup();
        ZookeeperCluster zkCluster = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, versions, SHARED_ENV_PROVIDER);
        return new ZooKeeperSpecChecker(zkCluster);
    }

    @Test
    public void checkEmptyWarnings() {
        Map<String, Object> kafkaOptions = new HashMap<>();
        kafkaOptions.put(KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 3);
        kafkaOptions.put(KafkaConfiguration.MIN_INSYNC_REPLICAS, 2);

        Kafka kafka = ResourceUtils.createKafka(NAMESPACE, NAME, 3, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT,
                null, kafkaOptions, emptyMap(),
                new EphemeralStorage(), new EphemeralStorage(), null, null, null, null);

        ZooKeeperSpecChecker checker = generateChecker(kafka);
        assertThat(checker.run(), empty());
    }

    @Test
    public void checkZookeeperStorage() {
        Map<String, Object> kafkaOptions = new HashMap<>();
        kafkaOptions.put(KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 3);
        kafkaOptions.put(KafkaConfiguration.MIN_INSYNC_REPLICAS, 2);

        Kafka kafka = new KafkaBuilder(ResourceUtils.createKafka(NAMESPACE, NAME, 3, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT,
                null, kafkaOptions, emptyMap(),
            new EphemeralStorage(), new EphemeralStorage(), null, null, null, null))
                .editSpec()
                    .editZookeeper()
                        .withReplicas(1)
                    .endZookeeper()
                .endSpec()
            .build();

        ZooKeeperSpecChecker checker = generateChecker(kafka);
        List<Condition> warnings = checker.run();
        assertThat(warnings, hasSize(1));
        Condition warning = warnings.get(0);
        assertThat(warning.getReason(), is("ZooKeeperStorage"));
        assertThat(warning.getStatus(), is("True"));
        assertThat(warning.getMessage(), is("A ZooKeeper cluster with a single replica and ephemeral storage will be in a defective state after any restart or rolling update. It is recommended that a minimum of three replicas are used."));
    }

    @Test
    public void checkZookeeperReplicas() {
        Map<String, Object> kafkaOptions = new HashMap<>();
        kafkaOptions.put(KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 2);
        kafkaOptions.put(KafkaConfiguration.MIN_INSYNC_REPLICAS, 1);

        Kafka kafka = ResourceUtils.createKafka(NAMESPACE, NAME, 2, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT,
                null, kafkaOptions, emptyMap(),
                new EphemeralStorage(), new EphemeralStorage(), null, null, null, null);

        ZooKeeperSpecChecker checker = generateChecker(kafka);
        List<Condition> warnings = checker.run();
        assertThat(warnings, hasSize(1));
        Condition warning = warnings.get(0);
        assertThat(warning.getReason(), is("ZooKeeperReplicas"));
        assertThat(warning.getStatus(), is("True"));
        assertThat(warning.getMessage(), is("Running ZooKeeper with two nodes is not advisable as both replicas will be needed to avoid downtime. It is recommended that a minimum of three replicas are used."));
    }

    @Test
    public void checkZookeeperEvenReplicas() {
        Map<String, Object> kafkaOptions = new HashMap<>();
        kafkaOptions.put(KafkaConfiguration.DEFAULT_REPLICATION_FACTOR, 3);
        kafkaOptions.put(KafkaConfiguration.MIN_INSYNC_REPLICAS, 2);

        Kafka kafka = ResourceUtils.createKafka(NAMESPACE, NAME, 4, IMAGE, HEALTH_DELAY, HEALTH_TIMEOUT,
                null, kafkaOptions, emptyMap(),
                new EphemeralStorage(), new EphemeralStorage(), null, null, null, null);

        ZooKeeperSpecChecker checker = generateChecker(kafka);
        List<Condition> warnings = checker.run();
        assertThat(warnings, hasSize(1));
        Condition warning = warnings.get(0);
        assertThat(warning.getReason(), is("ZooKeeperReplicas"));
        assertThat(warning.getStatus(), is("True"));
        assertThat(warning.getMessage(), is("Running ZooKeeper with an odd number of replicas is recommended."));
    }
}
