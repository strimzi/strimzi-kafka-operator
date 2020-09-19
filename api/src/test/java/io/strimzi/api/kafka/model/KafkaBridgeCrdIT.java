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

import static org.hamcrest.CoreMatchers.anyOf;
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
    void testKafkaBridgeV1alpha1() {
        assumeKube1_11Plus();
        createDelete(KafkaBridge.class, "KafkaBridgeV1alpha1.yaml");
    }

    @Test
    void testKafkaBridgeScaling() {
        createScaleDelete(KafkaBridge.class, "KafkaBridge.yaml");
    }

    @Test
    void testKafkaBridgeMinimal() {
        createDelete(KafkaBridge.class, "KafkaBridge-minimal.yaml");
    }

    @Test
    void testKafkaBridgeWithExtraProperty() {
        createDelete(KafkaMirrorMaker.class, "KafkaBridge-with-extra-property.yaml");
    }

    @Test
    void testKafkaBridgeWithMissingRequired() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> createDelete(KafkaBridge.class, "KafkaBridge-with-missing-required-property.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "spec.bootstrapServers");
    }

    @Test
    void testKafkaBridgeWithTls() {
        createDelete(KafkaBridge.class, "KafkaBridge-with-tls.yaml");
    }

    @Test
    void testKafkaBridgeWithTlsAuth() {
        createDelete(KafkaBridge.class, "KafkaBridge-with-tls-auth.yaml");
    }

    @Test
    void testKafkaBridgeWithTlsAuthWithMissingRequired() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> createDelete(KafkaBridge.class, "KafkaBridge-with-tls-auth-with-missing-required.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "spec.authentication.certificateAndKey.certificate",
                "spec.authentication.certificateAndKey.key");
    }

    @Test
    void testKafkaBridgeWithScramSha512Auth() {
        createDelete(KafkaBridge.class, "KafkaBridge-with-scram-sha-512-auth.yaml");
    }

    @Test
    void testKafkaBridgeWithTemplate() {
        createDelete(KafkaBridge.class, "KafkaBridge-with-template.yaml");
    }

    @Test
    void testKafkaBridgeWithJaegerTracing() {
        createDelete(KafkaBridge.class, "KafkaBridge-with-jaeger-tracing.yaml");
    }

    @Test
    void testKafkaBridgeWithWrongTracingType() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> createDelete(KafkaBridge.class, "KafkaBridge-with-wrong-tracing-type.yaml"));

        assertThat(exception.getMessage(), anyOf(
                containsStringIgnoringCase("spec.tracing.type in body should be one of [jaeger]"),
                containsStringIgnoringCase("spec.tracing.type: Unsupported value: \"wrongtype\": supported values: \"jaeger\"")));
    }

    @Test
    void testKafkaBridgeWithMissingTracingType() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> createDelete(KafkaBridge.class, "KafkaBridge-with-missing-tracing-type.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "spec.tracing.type");
    }

    @Test
    void testKafkaBridgeWithMetrics() {
        createDelete(KafkaBridge.class, "KafkaBridge-with-metrics.yaml");
    }

    @BeforeAll
    void setupEnvironment() {
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

