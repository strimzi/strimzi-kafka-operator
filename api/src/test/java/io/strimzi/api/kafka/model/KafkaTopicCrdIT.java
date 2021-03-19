/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
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
        createDeleteCustomResource("KafkaTopicV1alpha1.yaml");
    }

    @Test
    void testKafkaTopicIsNotScaling() {
        assertThrows(KubeClusterException.class, () -> createScaleDelete(KafkaTopic.class, "KafkaTopic.yaml"));
    }

    @Test
    void testKafkaTopicV1beta1() {
        createDeleteCustomResource("KafkaTopicV1beta1.yaml");
    }

    @Test
    void testKafkaTopicMinimal() {
        createDeleteCustomResource("KafkaTopic-minimal.yaml");
    }

    @Disabled("See https://github.com/strimzi/strimzi-kafka-operator/issues/4606")
    @Test
    void testCreateKafkaTopicWithExtraProperty() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> createDeleteCustomResource("KafkaTopic-with-extra-property.yaml"));

        assertThat(exception.getMessage(), containsString("unknown field \"foo\""));
    }

    @Test
    void testKafkaTopicWithMissingProperty() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> createDeleteCustomResource("KafkaTopic-with-missing-required-property.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "partitions", "replicas");
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
