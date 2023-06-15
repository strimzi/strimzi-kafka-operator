/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.nodepool;

import io.strimzi.api.kafka.model.AbstractCrdIT;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * This class tests the encoding and decoding of the KafkaNodePool custom resource
 */
public class KafkaNodePoolCrdIT extends AbstractCrdIT {
    public static final String NAMESPACE = "kafkanodepool-crd-it";

    @Test
    void testKafkaNodePool() {
        createDeleteCustomResource("KafkaNodePool.yaml");
    }

    @Test
    void testKafkaNodePoolScaling() {
        createScaleDelete(KafkaNodePool.class, "KafkaNodePool.yaml");
    }

    @Test
    public void testKafkaWithInvalidRole() {
        Throwable exception = assertThrows(
                KubeClusterException.class,
                () -> createDeleteCustomResource("KafkaNodePool-with-invalid-role.yaml"));

        assertThat(exception.getMessage(), anyOf(
                containsStringIgnoringCase("spec.roles[0] in body should be one of [controller broker]"),
                containsStringIgnoringCase("spec.roles: Unsupported value: \"helper\": supported values: \"controller\", \"broker\""),
                containsStringIgnoringCase("spec.roles[0]: Unsupported value: \"helper\": supported values: \"controller\", \"broker\"")));
    }

    @BeforeAll
    void setupEnvironment() {
        cluster.createCustomResources(TestUtils.CRD_KAFKA_NODE_POOL);
        cluster.waitForCustomResourceDefinition("kafkanodepools.kafka.strimzi.io");
        cluster.createNamespace(NAMESPACE);
    }

    @AfterAll
    void teardownEnvironment() {
        cluster.deleteCustomResources();
        cluster.deleteNamespaces();
    }
}
