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
public class KafkaClusterRebalanceCrdIT extends AbstractCrdIT {

    public static final String NAMESPACE = "kafkaclusterrebalance-crd-it";

    @Test
    void testKafkaClusterRebalanceMinimal() {
        createDelete(KafkaClusterRebalance.class, "KafkaClusterRebalance-minimal.yaml");
    }

    @Test
    void testKafkaClusterRebalanceWithGoals() {
        createDelete(KafkaClusterRebalance.class, "KafkaClusterRebalance-with-goals.yaml");
    }

    @Test
    void testKafkaClusterRebalanceWithGoalsVerbose() {
        createDelete(KafkaClusterRebalance.class, "KafkaClusterRebalance-with-goals-verbose.yaml");
    }

    @BeforeAll
    void setupEnvironment() {
        cluster.createNamespace(NAMESPACE);
        cluster.createCustomResources(TestUtils.CRD_KAFKA_CLUSTER_REBALANCE);
    }

    @AfterAll
    void teardownEnvironment() {
        cluster.deleteCustomResources();
        cluster.deleteNamespaces();
    }
}
