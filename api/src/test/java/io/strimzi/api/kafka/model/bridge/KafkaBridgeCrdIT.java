/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.bridge;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.model.AbstractCrdIT;
import io.strimzi.test.CrdUtils;
import io.strimzi.test.ReadWriteUtils;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsStringIgnoringCase;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The purpose of this test is to confirm that we can create a
 * resource from the POJOs, serialize it and create the resource in K8S.
 * I.e. that such instance resources obtained from POJOs are valid according to the schema
 * validation done by K8S.
 */
public class KafkaBridgeCrdIT extends AbstractCrdIT {
    public static final String NAMESPACE = "kafkabridge-crd-it";

    @Test
    void testKafkaBridgeScaling() {
        createScaleDelete(KafkaBridge.class, "KafkaBridge.yaml");
    }

    @Test
    void testKafkaBridgeMinimal() {
        createDeleteCustomResource("KafkaBridge-minimal.yaml");
    }

    @Test
    void testKafkaBridgeWithMissingRequired() {
        Throwable exception = assertThrows(
                KubernetesClientException.class,
                () -> createDeleteCustomResource("KafkaBridge-with-missing-required-property.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "bootstrapServers");
    }

    @Test
    void testKafkaBridgeWithTls() {
        createDeleteCustomResource("KafkaBridge-with-tls.yaml");
    }

    @Test
    void testKafkaBridgeWithTlsAuth() {
        createDeleteCustomResource("KafkaBridge-with-tls-auth.yaml");
    }

    @Test
    void testKafkaBridgeWithTlsAuthWithMissingRequired() {
        Throwable exception = assertThrows(
                KubernetesClientException.class,
                () -> createDeleteCustomResource("KafkaBridge-with-tls-auth-with-missing-required.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "spec.authentication.certificateAndKey.certificate",
                "spec.authentication.certificateAndKey.key");
    }

    @Test
    void testKafkaBridgeWithScramSha512Auth() {
        createDeleteCustomResource("KafkaBridge-with-scram-sha-512-auth.yaml");
    }

    @Test
    void testKafkaBridgeWithTemplate() {
        createDeleteCustomResource("KafkaBridge-with-template.yaml");
    }

    @Test
    void testKafkaBridgeWithOpenTelemetryTracing() {
        createDeleteCustomResource("KafkaBridge-with-opentelemetry-tracing.yaml");
    }

    @Test
    void testLoadKafkaBridgeWithWrongTracingType() {
        Throwable exception = assertThrows(
            RuntimeException.class,
            () -> ReadWriteUtils.readObjectFromYamlFileInResources("KafkaBridge-with-wrong-tracing-type.yaml", KafkaBridge.class));

        assertThat(exception.getMessage(), allOf(
                containsStringIgnoringCase("Could not resolve type id 'wrongtype'"),
                containsStringIgnoringCase("known type ids = [jaeger, opentelemetry]"))
        );
    }

    @Test
    void testCreateKafkaBridgeWithWrongTracingType() {
        Throwable exception = assertThrows(
                KubernetesClientException.class,
                () -> createDeleteCustomResource("KafkaBridge-with-wrong-tracing-type.yaml"));

        assertThat(exception.getMessage(), containsStringIgnoringCase("spec.tracing.type: Unsupported value: \"wrongtype\": supported values: \"jaeger\", \"opentelemetry\""));
    }

    @Test
    void testKafkaBridgeWithMissingTracingType() {
        Throwable exception = assertThrows(
                KubernetesClientException.class,
                () -> createDeleteCustomResource("KafkaBridge-with-missing-tracing-type.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "type");
    }

    @Test
    void testKafkaBridgeWithMetrics() {
        createDeleteCustomResource("KafkaBridge-with-metrics.yaml");
    }

    @BeforeAll
    void setupEnvironment() {
        client = new KubernetesClientBuilder().withConfig(new ConfigBuilder().withNamespace(NAMESPACE).build()).build();
        CrdUtils.createCrd(client, CrdUtils.CRD_KAFKA_BRIDGE_NAME, CrdUtils.CRD_KAFKA_BRIDGE);
        TestUtils.createNamespace(client, NAMESPACE);
    }

    @AfterAll
    void teardownEnvironment() {
        CrdUtils.deleteCrd(client, CrdUtils.CRD_KAFKA_BRIDGE_NAME);
        TestUtils.deleteNamespace(client, NAMESPACE);
        client.close();
    }
}

