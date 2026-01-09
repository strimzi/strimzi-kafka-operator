/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connector;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.model.AbstractCrdIT;
import io.strimzi.test.CrdUtils;
import io.strimzi.test.TestUtils;
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
public class KafkaConnectorCrdIT extends AbstractCrdIT {
    public static final String NAMESPACE = "kafkaconnector-crd-it";


    private KafkaConnectorCrdIT() {
    }
    @Test
    void testKafkaConnector() {
        createDeleteCustomResource("KafkaConnector.yaml");
    }

    @Test
    void testKafkaConnectorv1() {
        createDeleteCustomResource("KafkaConnector-v1.yaml");
    }

    @Test
    void testKafkaConnectorScaling() {
        createScaleDelete(KafkaConnector.class, "KafkaConnector.yaml");
    }

    @Test
    void testKafkaConnectorV1NoSpec() {
        Throwable exception = assertThrows(
                KubernetesClientException.class,
                () -> createDeleteCustomResource("KafkaConnector-v1-no-spec.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "spec");
    }

    @BeforeAll
    void setupEnvironment() {
        client = new KubernetesClientBuilder().withConfig(new ConfigBuilder().withNamespace(NAMESPACE).build()).build();
        CrdUtils.createCrd(client, CrdUtils.CRD_KAFKA_CONNECTOR_NAME, CrdUtils.CRD_KAFKA_CONNECTOR);
        TestUtils.createNamespace(client, NAMESPACE);
    }

    @AfterAll
    void teardownEnvironment() {
        CrdUtils.deleteCrd(client, CrdUtils.CRD_KAFKA_CONNECTOR_NAME);
        TestUtils.deleteNamespace(client, NAMESPACE);
        client.close();
    }
}
