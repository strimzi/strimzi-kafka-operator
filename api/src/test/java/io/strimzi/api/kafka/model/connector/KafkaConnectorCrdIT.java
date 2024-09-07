/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connector;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.strimzi.api.kafka.model.AbstractCrdIT;
import io.strimzi.test.CrdUtils;
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
        client = new KubernetesClientBuilder().withConfig(new ConfigBuilder().withNamespace(NAMESPACE).build()).build();
        CrdUtils.createCrd(client, KafkaConnector.CRD_NAME, CrdUtils.CRD_KAFKA_CONNECTOR);
        TestUtils.createNamespace(client, NAMESPACE);
    }

    @AfterAll
    void teardownEnvironment() {
        CrdUtils.deleteCrd(client, KafkaConnector.CRD_NAME);
        TestUtils.deleteNamespace(client, NAMESPACE);
        client.close();
    }
}
