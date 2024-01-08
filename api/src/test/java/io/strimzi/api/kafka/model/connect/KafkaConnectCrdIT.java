/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connect;

import io.strimzi.api.kafka.model.AbstractCrdIT;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

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

    @Test
    void testKafkaConnectWithExtraProperty() {
        // oc tool does not fail with extra properties, it shows only a warning. So this test does not pass on OpenShift
        assumeKube();

        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> createDeleteCustomResource("KafkaConnect-with-extra-property.yaml"));

        assertThat(exception.getMessage(), containsString("unknown field \"extra\""));
    }

    @Test
    void testKafkaConnectWithMissingRequired() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> createDeleteCustomResource("KafkaConnect-with-missing-required-property.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "spec.bootstrapServers");
    }

    @Test
    void testKafkaConnectWithInvalidReplicas() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> createDeleteCustomResource("KafkaConnect-with-invalid-replicas.yaml"));

        assertThat(exception.getMessage(),
                anyOf(
                        containsStringIgnoringCase("spec.replicas: Invalid value: \"string\": spec.replicas in body must be of type integer: \"string\""),
                        containsStringIgnoringCase("invalid type for io.strimzi.kafka.v1beta2.KafkaConnect.spec.replicas: got \"string\", expected \"integer\"")
                ));
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

    @BeforeAll
    void setupEnvironment() {
        cluster.createCustomResources(TestUtils.CRD_KAFKA_CONNECT);
        cluster.waitForCustomResourceDefinition("kafkaconnects.kafka.strimzi.io");
        cluster.createNamespace(NAMESPACE);

        try {
            Thread.sleep(1_000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @AfterAll
    void teardownEnvironment() {
        cluster.deleteNamespaces();
        cluster.deleteCustomResources();
    }
}
