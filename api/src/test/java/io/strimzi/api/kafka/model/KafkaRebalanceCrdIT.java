/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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

    @BeforeAll
    void setupEnvironment() {
        cluster.createNamespace(NAMESPACE);
        cluster.createCustomResources(TestUtils.CRD_KAFKA_REBALANCE);
        waitForCrd("crd", "kafkarebalances.kafka.strimzi.io");
    }

    @AfterAll
    void teardownEnvironment() {
        cluster.deleteCustomResources();
        cluster.deleteNamespaces();
    }
}
