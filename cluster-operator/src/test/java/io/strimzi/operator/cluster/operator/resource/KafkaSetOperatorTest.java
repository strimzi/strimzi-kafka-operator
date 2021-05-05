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
import io.strimzi.test.annotations.ParallelTest;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class KafkaSetOperatorTest {

    public static final InlineLogging KAFKA_LOG_CONFIG = new InlineLogging();
    public static final InlineLogging ZOOKEEPER_LOG_CONFIG = new InlineLogging();
    static {
        KAFKA_LOG_CONFIG.setLoggers(singletonMap("zookeeper.root.logger", "OFF"));
        ZOOKEEPER_LOG_CONFIG.setLoggers(singletonMap("kafka.root.logger.level", "OFF"));
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

    private StatefulSetDiff createDiff(StatefulSet currentSts, StatefulSet desiredSts) {
        return new StatefulSetDiff(currentSts, desiredSts);
    }

    @ParallelTest
    public void testNotNeedsRollingUpdateWhenIdentical() {
        KafkaVersion.Lookup versions = new KafkaVersion.Lookup(emptyMap(), emptyMap(), emptyMap(), emptyMap(), emptyMap());

        assertThat(KafkaSetOperator.needsRollingUpdate(createDiff(
            KafkaCluster.fromCrd(getResource(), versions).generateStatefulSet(true, null, null),
            KafkaCluster.fromCrd(getResource(), versions).generateStatefulSet(true, null, null))),
            is(false));
    }

    @ParallelTest
    public void testNotNeedsRollingUpdateWhenReplicasDecrease() {
        KafkaVersion.Lookup versions = new KafkaVersion.Lookup(emptyMap(), emptyMap(), emptyMap(), emptyMap(), emptyMap());
        StatefulSet currentSts = KafkaCluster.fromCrd(getResource(), versions).generateStatefulSet(true, null, null);
        StatefulSet desiredSts = KafkaCluster.fromCrd(getResource(), versions).generateStatefulSet(true, null, null);

        currentSts.getSpec().setReplicas(desiredSts.getSpec().getReplicas() + 1);
        assertThat(KafkaSetOperator.needsRollingUpdate(createDiff(currentSts, desiredSts)), is(false));
    }

    @ParallelTest
    public void testNeedsRollingUpdateWhenLabelsRemoved() {
        KafkaVersion.Lookup versions = new KafkaVersion.Lookup(emptyMap(), emptyMap(), emptyMap(), emptyMap(), emptyMap());
        StatefulSet currentSts = KafkaCluster.fromCrd(getResource(), versions).generateStatefulSet(true, null, null);
        StatefulSet desiredSts = KafkaCluster.fromCrd(getResource(), versions).generateStatefulSet(true, null, null);

        Map<String, String> labels = new HashMap(desiredSts.getMetadata().getLabels());
        labels.put("foo", "bar");
        currentSts.getMetadata().setLabels(labels);
        assertThat(KafkaSetOperator.needsRollingUpdate(createDiff(currentSts, desiredSts)), is(true));
    }

    @ParallelTest
    public void testNeedsRollingUpdateWhenImageChanges() {
        KafkaVersion.Lookup versions = new KafkaVersion.Lookup(emptyMap(), emptyMap(), emptyMap(), emptyMap(), emptyMap());
        StatefulSet currentSts = KafkaCluster.fromCrd(getResource(), versions).generateStatefulSet(true, null, null);
        StatefulSet desiredSts = KafkaCluster.fromCrd(getResource(), versions).generateStatefulSet(true, null, null);

        String newImage = currentSts.getSpec().getTemplate().getSpec().getContainers().get(0).getImage() + "-foo";
        currentSts.getSpec().getTemplate().getSpec().getContainers().get(0).setImage(newImage);
        assertThat(KafkaSetOperator.needsRollingUpdate(createDiff(currentSts, desiredSts)), is(true));
    }

    @ParallelTest
    public void testNeedsRollingUpdateWhenReadinessDelayChanges() {
        KafkaVersion.Lookup versions = new KafkaVersion.Lookup(emptyMap(), emptyMap(), emptyMap(), emptyMap(), emptyMap());
        StatefulSet currentSts = KafkaCluster.fromCrd(getResource(), versions).generateStatefulSet(true, null, null);
        StatefulSet desiredSts = KafkaCluster.fromCrd(getResource(), versions).generateStatefulSet(true, null, null);

        Integer newDelay = currentSts.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds() + 1;
        currentSts.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().setInitialDelaySeconds(newDelay);
        assertThat(KafkaSetOperator.needsRollingUpdate(createDiff(currentSts, desiredSts)), is(true));
    }

    @ParallelTest
    public void testNeedsRollingUpdateWhenReadinessTimeoutChanges() {
        KafkaVersion.Lookup versions = new KafkaVersion.Lookup(emptyMap(), emptyMap(), emptyMap(), emptyMap(), emptyMap());
        StatefulSet currentSts = KafkaCluster.fromCrd(getResource(), versions).generateStatefulSet(true, null, null);
        StatefulSet desiredSts = KafkaCluster.fromCrd(getResource(), versions).generateStatefulSet(true, null, null);

        Integer newTimeout = currentSts.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds() + 1;
        currentSts.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().setTimeoutSeconds(newTimeout);
        assertThat(KafkaSetOperator.needsRollingUpdate(createDiff(currentSts, desiredSts)), is(true));
    }

    @ParallelTest
    public void testNeedsRollingUpdateWhenNewEnvRemoved() {
        KafkaVersion.Lookup versions = new KafkaVersion.Lookup(emptyMap(), emptyMap(), emptyMap(), emptyMap(), emptyMap());
        StatefulSet currentSts = KafkaCluster.fromCrd(getResource(), versions).generateStatefulSet(true, null, null);
        StatefulSet desiredSts = KafkaCluster.fromCrd(getResource(), versions).generateStatefulSet(true, null, null);

        String envVar = "SOME_RANDOM_ENV";
        currentSts.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().add(new EnvVar(envVar,
                "foo", null));
        assertThat(KafkaSetOperator.needsRollingUpdate(createDiff(currentSts, desiredSts)), is(true));
    }
}
