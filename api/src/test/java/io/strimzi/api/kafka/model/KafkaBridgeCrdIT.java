/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.anyOf;
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
public class KafkaBridgeCrdIT extends AbstractCrdIT {

    private static final Logger LOGGER = LogManager.getLogger(KafkaBridgeCrdIT.class);

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
            KubeClusterException.class,
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
            KubeClusterException.InvalidResource.class,
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
    void testKafkaBridgeWithJaegerTracing() {
        createDeleteCustomResource("KafkaBridge-with-jaeger-tracing.yaml");
    }

    @Test
    void testLoadKafkaBridgeWithWrongTracingType() {
        Throwable exception = assertThrows(
            RuntimeException.class,
            () -> loadCustomResourceToYaml(KafkaBridge.class, "KafkaBridge-with-wrong-tracing-type.yaml"));

        assertThat(exception.getMessage(), allOf(
                containsStringIgnoringCase("Could not resolve type id 'wrongtype'"),
                containsStringIgnoringCase("known type ids = [jaeger]")));
    }

    @Test
    void testCreateKafkaBridgeWithWrongTracingType() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> createDeleteCustomResource("KafkaBridge-with-wrong-tracing-type.yaml"));

        assertThat(exception.getMessage(), anyOf(
                containsStringIgnoringCase("spec.tracing.type in body should be one of [jaeger]"),
                containsStringIgnoringCase("spec.tracing.type: Unsupported value: \"wrongtype\": supported values: \"jaeger\"")));
    }

    @Disabled("See https://github.com/strimzi/strimzi-kafka-operator/issues/4606")
    @Test
    void testCreateKafkaBridgeWithExtraProperty() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> createDeleteCustomResource("KafkaBridge-with-extra-property.yaml"));

        assertThat(exception.getMessage(), containsString("unknown field \"extra\""));
    }

    @Test
    void testKafkaBridgeWithMissingTracingType() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> createDeleteCustomResource("KafkaBridge-with-missing-tracing-type.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "type");
    }

    @Test
    void testKafkaBridgeWithMetrics() {
        createDeleteCustomResource("KafkaBridge-with-metrics.yaml");
    }

    @BeforeAll
    void setupEnvironment() throws InterruptedException {
        cluster.createNamespace(NAMESPACE);
        cluster.createCustomResources(TestUtils.CRD_KAFKA_BRIDGE);
        waitForCrd("crd", "kafkabridges.kafka.strimzi.io");
    }

    @AfterAll
    void teardownEnvironment() {
        cluster.deleteCustomResources();
        cluster.deleteNamespaces();
    }
}

