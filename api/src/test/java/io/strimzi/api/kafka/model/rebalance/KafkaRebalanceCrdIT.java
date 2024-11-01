/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.rebalance;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.model.AbstractCrdIT;
import io.strimzi.test.CrdUtils;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The purpose of this test is to confirm that we can create a
 * resource from the POJOs, serialize it and create the resource in K8S.
 * I.e. that such instance resources obtained from POJOs are valid according to the schema
 * validation done by K8S.
 */
public class KafkaRebalanceCrdIT extends AbstractCrdIT {
    public static final String NAMESPACE = "kafkarebalance-crd-it";

    @Test
    void testKafkaRebalanceIsNotScaling() {
        assertThrows(KubernetesClientException.class, () -> createScaleDelete(KafkaRebalance.class, "KafkaRebalance.yaml"));
    }

    @Test
    void testKafkaRebalanceMinimal() {
        createDeleteCustomResource("KafkaRebalance-minimal.yaml");
    }

    @Test
    void testKafkaRebalanceWithGoals() {
        createDeleteCustomResource("KafkaRebalance-with-goals.yaml");
    }

    @Test
    void testKafkaRebalanceWithGoalsSkipHardGoalCheck() {
        createDeleteCustomResource("KafkaRebalance-with-goals-skip-hard-goal-check.yaml");
    }

    @Test
    void testKafkaRebalanceWithPerformanceTuning() {
        createDeleteCustomResource("KafkaRebalance-performance-tuning.yaml");
    }

    @Test
    void testKafkaRebalanceWithExcludedTopics() {
        createDeleteCustomResource("KafkaRebalance-excluded-topics.yaml");
    }

    @Test
    void testKafkaRebalanceAddBroker() {
        createDeleteCustomResource("KafkaRebalance-add-brokers.yaml");
    }

    @Test
    void testKafkaRebalanceRemoveBroker() {
        createDeleteCustomResource("KafkaRebalance-remove-brokers.yaml");
    }

    @Test
    void testKafkaRebalanceRemoveDisks() {
        createDeleteCustomResource("KafkaRebalance-remove-disks.yaml");
    }

    @Test
    void testKafkaRebalanceRemoveDisksWithEmptyVolumes() {
        Throwable exception = assertThrows(
                KubernetesClientException.class,
                () -> createDeleteCustomResource("KafkaRebalance-remove-disks-with-empty-volumes.yaml"));

        assertThat(exception.getMessage(), containsString("spec.moveReplicasOffVolumes[0].volumeIds: Invalid value: 0: spec.moveReplicasOffVolumes[0].volumeIds in body should have at least 1 items."));
    }

    @Test
    void testKafkaRebalanceRemoveDisksWithEmptyBrokerAndVolumes() {
        Throwable exception = assertThrows(
                KubernetesClientException.class,
                () -> createDeleteCustomResource("KafkaRebalance-remove-disks-with-empty-broker-and-volumes.yaml"));

        assertThat(exception.getMessage(), containsString("spec.moveReplicasOffVolumes: Invalid value: 0: spec.moveReplicasOffVolumes in body should have at least 1 items."));
    }

    @Test
    void testKafkaRebalanceWrongMode() {
        Throwable exception = assertThrows(
                KubernetesClientException.class,
                () -> createDeleteCustomResource("KafkaRebalance-wrong-mode.yaml"));

        assertThat(exception.getMessage(), containsString("spec.mode: Unsupported value: \"wrong-mode\": supported values: \"full\", \"add-brokers\", \"remove-brokers\""));
    }

    @BeforeAll
    void setupEnvironment() {
        client = new KubernetesClientBuilder().withConfig(new ConfigBuilder().withNamespace(NAMESPACE).build()).build();
        CrdUtils.createCrd(client, KafkaRebalance.CRD_NAME, CrdUtils.CRD_KAFKA_REBALANCE);
        TestUtils.createNamespace(client, NAMESPACE);
    }

    @AfterAll
    void teardownEnvironment() {
        CrdUtils.deleteCrd(client, KafkaRebalance.CRD_NAME);
        TestUtils.deleteNamespace(client, NAMESPACE);
        client.close();
    }
}
