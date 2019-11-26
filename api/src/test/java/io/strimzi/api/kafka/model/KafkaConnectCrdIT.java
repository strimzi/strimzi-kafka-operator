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

import static org.hamcrest.CoreMatchers.containsStringIgnoringCase;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.anyOf;
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
            () -> {
                createDelete(KafkaConnect.class, "KafkaConnect-with-missing-required-property.yaml");
            });

        String expectedMsg1 = "spec.bootstrapServers in body is required";
        String expectedMsg2 = "spec.bootstrapServers: Required value";

        assertThat(exception.getMessage(), anyOf(
                containsStringIgnoringCase(expectedMsg1), containsStringIgnoringCase(expectedMsg2)));
    }

    @Test
    void testKafkaConnectWithInvalidReplicas() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> {
                createDelete(KafkaConnect.class, "KafkaConnect-with-invalid-replicas.yaml");
            });

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
            () -> {
                createDelete(KafkaConnect.class, "KafkaConnect-with-tls-auth-with-missing-required.yaml");
            });

        String expectedMsg1a = "spec.authentication.certificateAndKey.certificate in body is required";
        String expectedMsg1b = "spec.authentication.certificateAndKey.key in body is required";

        String expectedMsg2a = "spec.authentication.certificateAndKey.certificate: Required value";
        String expectedMsg2b = "spec.authentication.certificateAndKey.key: Required value";

        assertThat(exception.getMessage(), anyOf(
                allOf(containsStringIgnoringCase(expectedMsg1a), containsStringIgnoringCase(expectedMsg1b)),
                allOf(containsStringIgnoringCase(expectedMsg2a), containsStringIgnoringCase(expectedMsg2b))));
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
            () -> {
                createDelete(KafkaConnect.class, "KafkaConnect-with-invalid-external-configuration.yaml");
            });

        String expectedMsg1 = "spec.externalConfiguration.env.valueFrom in body is required";
        String expectedMsg2 = "spec.externalConfiguration.env.valueFrom: Required value";

        assertThat(exception.getMessage(), anyOf(
                containsStringIgnoringCase(expectedMsg1), containsStringIgnoringCase(expectedMsg2)));
    }

    @BeforeAll
    void setupEnvironment() {
        createNamespace(NAMESPACE);
        createCustomResources(TestUtils.CRD_KAFKA_CONNECT);
    }

    @AfterAll
    void teardownEnvironment() {
        deleteCustomResources();
        deleteNamespaces();
    }
}
