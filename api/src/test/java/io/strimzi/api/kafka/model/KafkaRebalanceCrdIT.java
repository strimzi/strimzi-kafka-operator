/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The purpose of this test is to confirm that we can create a
 * resource from the POJOs, serialize it and create the resource in K8S.
 * I.e. that such instance resources obtained from POJOs are valid according to the schema
 * validation done by K8S.
 */
public class KafkaRebalanceCrdIT extends AbstractCrdIT {

    public static final String NAMESPACE = "kafkarebalance-crd-it";

    @Test
    void testKafkaRebalanceMinimal() {
        createDelete(KafkaRebalance.class, "KafkaRebalance-minimal.yaml");
    }

    @Test
    void testKafkaRebalanceWithGoals() {
        createDelete(KafkaRebalance.class, "KafkaRebalance-with-goals.yaml");
    }

    @Test
    void testKafkaRebalanceWithGoalsSkipHardGoalCheck() {
        createDelete(KafkaRebalance.class, "KafkaRebalance-with-goals-skip-hard-goal-check.yaml");
    }

    @BeforeAll
    void setupEnvironment() {
        cluster.createNamespace(NAMESPACE);
        cluster.createCustomResources(TestUtils.CRD_KAFKA_REBALANCE);
    }

    @AfterAll
    void teardownEnvironment() {
        cluster.deleteCustomResources();
        cluster.deleteNamespaces();
    }
}
