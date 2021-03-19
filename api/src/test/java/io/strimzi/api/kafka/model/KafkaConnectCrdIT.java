/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.containsStringIgnoringCase;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
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
    void testKafkaConnectScaling() {
        createScaleDelete(KafkaConnect.class, "KafkaConnect.yaml");
    }

    @Test
    void testKafkaConnectMinimal() {
        createDeleteCustomResource("KafkaConnect-minimal.yaml");
    }

    @Disabled("See https://github.com/strimzi/strimzi-kafka-operator/issues/4606")
    @Test
    void testCreateKafkaConnectWithExtraProperty() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> createDeleteCustomResource("KafkaConnect-with-extra-property.yaml"));

        assertThat(exception.getMessage(), containsString("unknown field \"extra\""));
    }

    @Test
    void testKafkaConnectWithMissingRequired() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> createDeleteCustomResource("KafkaConnect-with-missing-required-property.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "spec.bootstrapServers");
    }

    @Test
    void testKafkaConnectWithInvalidReplicas() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> createDeleteCustomResource("KafkaConnect-with-invalid-replicas.yaml"));

        assertThat(exception.getMessage(),
                containsStringIgnoringCase("spec.replicas: Invalid value: \"string\": spec.replicas in body must be of type integer: \"string\""));
    }

    @Test
    void testKafkaConnectWithTls() {
        createDeleteCustomResource("KafkaConnect-with-tls.yaml");
    }

    @Test
    void testKafkaConnectWithTlsAuth() {
        createDeleteCustomResource("KafkaConnect-with-tls-auth.yaml");
    }

    @Test
    @Disabled
    void testLoadKafkaConnectWithTlsAuthWithMissingRequired() {
        Throwable exception = assertThrows(
            RuntimeException.class,
            () -> loadCustomResourceToYaml(KafkaConnect.class, "KafkaConnect-with-tls-auth-with-missing-required.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "certificate", "key");
    }

    @Test
    void testCreateKafkaConnectWithTlsAuthWithMissingRequired() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> createDeleteCustomResource("KafkaConnect-with-tls-auth-with-missing-required.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "certificate", "key");
    }

    @Test
    void testKafkaConnectWithScramSha512Auth() {
        createDeleteCustomResource("KafkaConnect-with-scram-sha-512-auth.yaml");
    }

    @Test
    public void testKafkaConnectWithTemplate() {
        createDeleteCustomResource("KafkaConnect-with-template.yaml");
    }

    @Test
    public void testKafkaConnectWithExternalConfiguration() {
        createDeleteCustomResource("KafkaConnect-with-external-configuration.yaml");
    }

    @Test
    void testKafkaConnectWithInvalidExternalConfiguration() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> createDeleteCustomResource("KafkaConnect-with-invalid-external-configuration.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "valueFrom");
    }

    @BeforeEach
    void setup() {
        cluster.createCustomResources(TestUtils.CRD_KAFKA_CONNECT);
        waitForCrd("crd", "kafkaconnects.kafka.strimzi.io");
    }

    @AfterEach
    void teardown() {
        cluster.deleteCustomResources();
    }

    @BeforeAll
    void setupEnvironment() {
        cluster.createNamespace(NAMESPACE);
    }

    @AfterAll
    void teardownEnvironment() {
        cluster.deleteNamespaces();
    }
}
