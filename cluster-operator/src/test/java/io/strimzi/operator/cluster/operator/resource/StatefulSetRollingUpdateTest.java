/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class StatefulSetRollingUpdateTest {
    public static final InlineLogging KAFKA_LOG_CONFIG = new InlineLogging();
    public static final InlineLogging ZOOKEEPER_LOG_CONFIG = new InlineLogging();

    static {
        KAFKA_LOG_CONFIG.setLoggers(singletonMap("zookeeper.root.logger", "OFF"));
        ZOOKEEPER_LOG_CONFIG.setLoggers(singletonMap("kafka.root.logger.level", "OFF"));
    }

    private StatefulSet currectSts;
    private StatefulSet desiredSts;

    @BeforeEach
    public void before() {
        KafkaVersion.Lookup versions = new KafkaVersion.Lookup(emptyMap(), emptyMap(), emptyMap(), emptyMap());
        currectSts = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, getResource(), versions).generateStatefulSet(true, null, null, null);
        desiredSts = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, getResource(), versions).generateStatefulSet(true, null, null, null);
    }

    private Kafka getResource() {
        String kafkaName = "foo";
        String kafkaNamespace = "test";
        int replicas = 3;
        String image = "bar";
        int healthDelay = 120;
        int healthTimeout = 30;

        return new KafkaBuilder(ResourceUtils.createKafka(kafkaNamespace, kafkaName,
                replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .editKafka()
                        .withNewPersistentClaimStorage()
                            .withSize("123")
                            .withStorageClass("foo")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                        .withLogging(KAFKA_LOG_CONFIG)
                    .endKafka()
                    .editZookeeper()
                        .withLogging(ZOOKEEPER_LOG_CONFIG)
                    .endZookeeper()
                .endSpec()
                .build();
    }

    private StatefulSetDiff createDiff() {
        return new StatefulSetDiff(Reconciliation.DUMMY_RECONCILIATION, currectSts, desiredSts);
    }

    @Test
    public void testNotNeedsRollingUpdateWhenIdentical() {
        assertThat(StatefulSetOperator.needsRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, createDiff()), is(false));
    }

    @Test
    public void testNotNeedsRollingUpdateWhenReplicasDecrease() {
        currectSts.getSpec().setReplicas(desiredSts.getSpec().getReplicas() + 1);
        assertThat(StatefulSetOperator.needsRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, createDiff()), is(false));
    }

    @Test
    public void testNeedsRollingUpdateWhenLabelsRemoved() {
        Map<String, String> labels = new HashMap<>(desiredSts.getMetadata().getLabels());
        labels.put("foo", "bar");
        currectSts.getMetadata().setLabels(labels);
        assertThat(StatefulSetOperator.needsRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, createDiff()), is(true));
    }

    @Test
    public void testNeedsRollingUpdateWhenImageChanges() {
        String newImage = currectSts.getSpec().getTemplate().getSpec().getContainers().get(0).getImage() + "-foo";
        currectSts.getSpec().getTemplate().getSpec().getContainers().get(0).setImage(newImage);
        assertThat(StatefulSetOperator.needsRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, createDiff()), is(true));
    }

    @Test
    public void testNeedsRollingUpdateWhenReadinessDelayChanges() {
        Integer newDelay = currectSts.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds() + 1;
        currectSts.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().setInitialDelaySeconds(newDelay);
        assertThat(StatefulSetOperator.needsRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, createDiff()), is(true));
    }

    @Test
    public void testNeedsRollingUpdateWhenReadinessTimeoutChanges() {
        Integer newTimeout = currectSts.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds() + 1;
        currectSts.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().setTimeoutSeconds(newTimeout);
        assertThat(StatefulSetOperator.needsRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, createDiff()), is(true));
    }

    @Test
    public void testNeedsRollingUpdateWhenNewEnvRemoved() {
        String envVar = "SOME_RANDOM_ENV";
        currectSts.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().add(new EnvVar(envVar,
                "foo", null));
        assertThat(StatefulSetOperator.needsRollingUpdate(Reconciliation.DUMMY_RECONCILIATION, createDiff()), is(true));
    }
}
