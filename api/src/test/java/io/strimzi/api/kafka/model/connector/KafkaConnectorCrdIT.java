/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connector;

import io.strimzi.api.kafka.model.AbstractCrdIT;
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
public class KafkaConnectorCrdIT extends AbstractCrdIT {
    public static final String NAMESPACE = "kafkaconnector-crd-it";

    @Test
    void testKafkaConnector() {
        createDeleteCustomResource("KafkaConnector.yaml");
    }

    @Test
    void testKafkaConnectorScaling() {
        createScaleDelete(KafkaConnector.class, "KafkaConnector.yaml");
    }

    @BeforeAll
    void setupEnvironment() {
        cluster.createCustomResources(TestUtils.CRD_KAFKA_CONNECTOR);
        cluster.waitForCustomResourceDefinition("kafkaconnectors.kafka.strimzi.io");
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
