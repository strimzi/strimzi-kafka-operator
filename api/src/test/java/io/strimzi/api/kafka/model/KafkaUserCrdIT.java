/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertTrue;

/**
 * The purpose of this test is to confirm that we can create a
 * resource from the POJOs, serialize it and create the resource in K8S.
 * I.e. that such instance resources obtained from POJOs are valid according to the schema
 * validation done by K8S.
 */
public class KafkaUserCrdIT extends AbstractCrdIT {
    public static final String NAMESPACE = "kafkausercrd-it";

    @Test
    void testKafkaUser() {
        createDelete(KafkaUser.class, "KafkaUser.yaml");
    }

    @Test
    void testKafkaUserMinimal() {
        createDelete(KafkaUser.class, "KafkaUser-minimal.yaml");
    }

    @Test
    void testKafkaUserWithExtraProperty() {
        createDelete(KafkaUser.class, "KafkaUser-with-extra-property.yaml");
    }

    @Test
    void testKafkaUserWithMissingRequired() {
        try {
            createDelete(KafkaUser.class, "KafkaUser-with-missing-required.yaml");
        } catch (KubeClusterException.InvalidResource e) {
            assertTrue(e.getMessage().contains("spec.authentication in body is required"));
        }
    }

    @BeforeAll
    void setupEnvironment() {
        createNamespace(NAMESPACE);
        createCustomResources(TestUtils.CRD_KAFKA_USER);
    }

    @AfterAll
    void teardownEnvironment() {
        deleteCustomResources();
        deleteNamespaces();
    }
}
