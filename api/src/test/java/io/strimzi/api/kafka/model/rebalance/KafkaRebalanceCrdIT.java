/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.rebalance;

import io.strimzi.api.kafka.model.AbstractCrdIT;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
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
        assertThrows(KubeClusterException.class, () -> createScaleDelete(KafkaRebalance.class, "KafkaRebalance.yaml"));
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
    void testKafkaRebalanceWrongMode() {
        Throwable exception = assertThrows(
                KubeClusterException.class,
                () -> createDeleteCustomResource("KafkaRebalance-wrong-mode.yaml"));

        assertThat(exception.getMessage(), containsString("spec.mode: Unsupported value: \"wrong-mode\": supported values: \"full\", \"add-brokers\", \"remove-brokers\""));
    }

    @BeforeAll
    void setupEnvironment() {
        cluster.createCustomResources(TestUtils.CRD_KAFKA_REBALANCE);
        cluster.waitForCustomResourceDefinition("kafkarebalances.kafka.strimzi.io");
        cluster.createNamespace(NAMESPACE);

        try {
            Thread.sleep(1_000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @AfterAll
    void teardownEnvironment() {
        cluster.deleteCustomResources();
        cluster.deleteNamespaces();
    }
}
