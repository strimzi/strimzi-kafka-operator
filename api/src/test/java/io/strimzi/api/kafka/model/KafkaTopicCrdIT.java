/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The purpose of this test is to confirm that we can create a
 * resource from the POJOs, serialize it and create the resource in K8S.
 * I.e. that such instance resources obtained from POJOs are valid according to the schema
 * validation done by K8S.
 */
public class KafkaTopicCrdIT extends AbstractCrdIT {

    public static final String NAMESPACE = "topiccrd-it";

    @Test
    void testKafkaTopicV1alpha1() {
        assumeKube1_11Plus();
        createDelete(KafkaTopic.class, "KafkaTopicV1alpha1.yaml");
    }
    @Test
    void testKafkaTopicV1beta1() {
        createDelete(KafkaTopic.class, "KafkaTopicV1beta1.yaml");
    }

    @Test
    void testKafkaTopicMinimal() {
        createDelete(KafkaTopic.class, "KafkaTopic-minimal.yaml");
    }

    @Test
    void testKafkaTopicWithExtraProperty() {
        createDelete(KafkaTopic.class, "KafkaTopic-with-extra-property.yaml");
    }

    @Test
    void testKafkaTopicWithMissingProperty() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> {
                createDelete(KafkaTopic.class, "KafkaTopic-with-missing-required-property.yaml");
            });

        String expectedMsg1a = "spec.partitions in body is required";
        String expectedMsg1b = "spec.replicas in body is required";
        String expectedMsg2a = "spec.partitions: Required value";
        String expectedMsg2b = "spec.replicas: Required value";
        assertThat(exception.getMessage(), anyOf(
                allOf(
                        containsStringIgnoringCase(expectedMsg1a),
                        containsStringIgnoringCase(expectedMsg1b)),
                allOf(
                        containsStringIgnoringCase(expectedMsg2a),
                        containsStringIgnoringCase(expectedMsg2b))
                ));
    }

    @BeforeAll
    void setupEnvironment() {
        createNamespace(NAMESPACE);
        createCustomResources(TestUtils.CRD_TOPIC);
    }

    @AfterAll
    void teardownEnvironment() {
        deleteCustomResources();
        deleteNamespaces();
    }
}
