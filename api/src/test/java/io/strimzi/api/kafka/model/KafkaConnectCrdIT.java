/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.test.Namespace;
import io.strimzi.test.Resources;
import io.strimzi.test.StrimziRunner;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterException;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

/**
 * The purpose of this test is to confirm that we can create a
 * resource from the POJOs, serialize it and create the resource in K8S.
 * I.e. that such instance resources obtained from POJOs are valid according to the schema
 * validation done by K8S.
 */
@RunWith(StrimziRunner.class)
@Namespace(KafkaConnectCrdIT.NAMESPACE)
@Resources(value = TestUtils.CRD_KAFKA_CONNECT, asAdmin = true)
public class KafkaConnectCrdIT extends AbstractCrdIT {
    public static final String NAMESPACE = "kafkaconnect-crd-it";

    @Test
    public void testKafka() {
        createDelete(KafkaConnectAssembly.class, "KafkaConnectAssembly.yaml");
    }

    @Test
    public void testKafkaMinimal() {
        createDelete(KafkaConnectAssembly.class, "KafkaConnectAssembly-minimal.yaml");
    }

    @Test
    public void testKafkaWithExtraProperty() {
        createDelete(KafkaConnectAssembly.class, "KafkaConnectAssembly-with-extra-property.yaml");
    }

    @Test
    public void testKafkaWithMissingRequired() {
        try {
            createDelete(KafkaConnectAssembly.class, "KafkaConnectAssembly-with-missing-required-property.yaml");
        } catch (KubeClusterException.InvalidResource e) {
            assertTrue(e.getMessage(), e.getMessage().contains("spec.config in body is required"));
        }
    }

    @Test
    public void testKafkaWithInvalidResourceMemory() {
        try {
            createDelete(KafkaConnectAssembly.class, "KafkaConnectAssembly-with-invalid-resource-memory.yaml");
        } catch (KubeClusterException.InvalidResource e) {
            assertTrue(e.getMessage().contains("spec.replicas in body must be of type integer: \"string\""));
        }
    }

}
