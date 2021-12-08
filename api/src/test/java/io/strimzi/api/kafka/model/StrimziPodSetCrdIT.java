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
public class StrimziPodSetCrdIT extends AbstractCrdIT {
    public static final String NAMESPACE = "strimzipodset-crd-it";

    @Test
    void testStrimziPodSetMinimal() {
        createDeleteCustomResource("StrimziPodSet.yaml");
    }

    @Test
    void testStrimziPodSettWithMissingRequired() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> createDeleteCustomResource("StrimziPodSet-with-missing-required-property.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "pods", "selector");
    }

    @BeforeAll
    void setupEnvironment() throws InterruptedException {
        cluster.createCustomResources(TestUtils.CRD_STRIMZI_POD_SET);
        cluster.waitForCustomResourceDefinition("strimzipodsets.core.strimzi.io");
        cluster.createNamespace(NAMESPACE);
    }

    @AfterAll
    void teardownEnvironment() {
        cluster.deleteCustomResources();
        cluster.deleteNamespaces();
    }
}

