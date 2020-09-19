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
public class KafkaTopicCrdIT extends AbstractCrdIT {

    public static final String NAMESPACE = "topiccrd-it";

    @Test
    void testKafkaTopicV1alpha1() {
        assumeKube1_11Plus();
        createDelete(KafkaTopic.class, "KafkaTopicV1alpha1.yaml");
    }

    @Test
    void testKafkaTopicIsNotScaling() {
        assertThrows(KubeClusterException.class, () -> createScaleDelete(KafkaTopic.class, "KafkaTopic.yaml"));
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
            () -> createDelete(KafkaTopic.class, "KafkaTopic-with-missing-required-property.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "spec.partitions", "spec.replicas");
    }

    @BeforeAll
    void setupEnvironment() {
        cluster.createNamespace(NAMESPACE);
        cluster.createCustomResources(TestUtils.CRD_TOPIC);
        waitForCrd("crd", "kafkatopics.kafka.strimzi.io");
    }

    @AfterAll
    void teardownEnvironment() {
        cluster.deleteCustomResources();
        cluster.deleteNamespaces();
    }
}
