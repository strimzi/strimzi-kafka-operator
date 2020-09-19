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

import static org.hamcrest.CoreMatchers.containsStringIgnoringCase;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The purpose of this test is to confirm that we can create a
 * resource from the POJOs, serialize it and create the resource in K8S.
 * I.e. that such instance resources obtained from POJOs are valid according to the schema
 * validation done by K8S.
 */
public class KafkaConnectCrdIT extends AbstractCrdIT {
    public static final String NAMESPACE = "kafkaconnect-crd-it";

    @Test
    void testKafkaConnectV1alpha1() {
        assumeKube1_11Plus();
        createDelete(KafkaConnect.class, "KafkaConnectV1alpha1.yaml");
    }

    @Test
    void testKafkaConnectScaling() {
        createScaleDelete(KafkaConnect.class, "KafkaConnect.yaml");
    }

    @Test
    void testKafkaConnectV1beta1() {
        createDelete(KafkaConnect.class, "KafkaConnectV1beta1.yaml");
    }

    @Test
    void testKafkaConnectMinimal() {
        createDelete(KafkaConnect.class, "KafkaConnect-minimal.yaml");
    }

    @Test
    void testKafkaConnectWithExtraProperty() {
        createDelete(KafkaConnect.class, "KafkaConnect-with-extra-property.yaml");
    }

    @Test
    void testKafkaConnectWithMissingRequired() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> createDelete(KafkaConnect.class, "KafkaConnect-with-missing-required-property.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "spec.bootstrapServers");
    }

    @Test
    void testKafkaConnectWithInvalidReplicas() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> createDelete(KafkaConnect.class, "KafkaConnect-with-invalid-replicas.yaml"));

        assertThat(exception.getMessage(),
                containsStringIgnoringCase("spec.replicas in body must be of type integer: \"string\""));
    }

    @Test
    void testKafkaConnectWithTls() {
        createDelete(KafkaConnect.class, "KafkaConnect-with-tls.yaml");
    }

    @Test
    void testKafkaConnectWithTlsAuth() {
        createDelete(KafkaConnect.class, "KafkaConnect-with-tls-auth.yaml");
    }

    @Test
    void testKafkaConnectWithTlsAuthWithMissingRequired() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> createDelete(KafkaConnect.class, "KafkaConnect-with-tls-auth-with-missing-required.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "spec.authentication.certificateAndKey.certificate", "spec.authentication.certificateAndKey.key");
    }

    @Test
    void testKafkaConnectWithScramSha512Auth() {
        createDelete(KafkaConnect.class, "KafkaConnect-with-scram-sha-512-auth.yaml");
    }

    @Test
    public void testKafkaConnectWithTemplate() {
        createDelete(KafkaConnect.class, "KafkaConnect-with-template.yaml");
    }

    @Test
    public void testKafkaConnectWithExternalConfiguration() {
        createDelete(KafkaConnect.class, "KafkaConnect-with-external-configuration.yaml");
    }

    @Test
    public void testKafkaConnectWithInvalidExternalConfiguration() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> createDelete(KafkaConnect.class, "KafkaConnect-with-invalid-external-configuration.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "spec.externalConfiguration.env.valueFrom");
    }

    @BeforeAll
    void setupEnvironment() {
        cluster.createNamespace(NAMESPACE);
        cluster.createCustomResources(TestUtils.CRD_KAFKA_CONNECT);
        waitForCrd("crd", "kafkaconnects.kafka.strimzi.io");
    }

    @AfterAll
    void teardownEnvironment() {
        cluster.deleteCustomResources();
        cluster.deleteNamespaces();
    }
}
