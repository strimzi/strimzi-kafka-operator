/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connect;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.model.AbstractCrdIT;
import io.strimzi.test.CrdUtils;
import io.strimzi.test.TestUtils;
import org.hamcrest.CoreMatchers;
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
public class KafkaConnectCrdIT extends AbstractCrdIT {
    public static final String NAMESPACE = "kafkaconnect-crd-it";

    @Test
    void testKafkaConnectScaling() {
        createScaleDelete(KafkaConnect.class, "KafkaConnect.yaml");
    }

    @Test
    void testKafkaConnectV1() {
        createDeleteCustomResource("KafkaConnect-v1.yaml");
    }

    @Test
    void testKafkaConnectMinimal() {
        createDeleteCustomResource("KafkaConnect-minimal.yaml");
    }

    @Test
    void testKafkaConnectWithMissingRequired() {
        Throwable exception = assertThrows(
                KubernetesClientException.class,
                () -> createDeleteCustomResource("KafkaConnect-with-missing-required-property.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "spec.bootstrapServers");
    }

    @Test
    void testKafkaConnectWithInvalidReplicas() {
        Throwable exception = assertThrows(
                KubernetesClientException.class,
                () -> createDeleteCustomResource("KafkaConnect-with-invalid-replicas.yaml"));

        assertThat(exception.getMessage(), containsStringIgnoringCase("spec.replicas: Invalid value: \"string\": spec.replicas in body must be of type integer: \"string\""));
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
    void testCreateKafkaConnectWithTlsAuthWithMissingRequired() {
        Throwable exception = assertThrows(
                KubernetesClientException.class,
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
                KubernetesClientException.class,
                () -> createDeleteCustomResource("KafkaConnect-with-invalid-external-configuration.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "valueFrom");
    }

    @Test
    public void testKafkaConnectWithDnsConfig() {
        createDeleteCustomResource("KafkaConnect-with-dnsConfig.yaml");
    }

    @Test
    void testKafkaConnectV1NoSpec() {
        Throwable exception = assertThrows(
                KubernetesClientException.class,
                () -> createDeleteCustomResource("KafkaConnect-v1-no-spec.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "spec");
    }

    @Test
    void testKafkaConnectV1NoReplicasGroupEtc() {
        Throwable exception = assertThrows(
                KubernetesClientException.class,
                () -> createDeleteCustomResource("KafkaConnect-v1-no-replicas-group-etc.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "replicas", "groupId", "configStorageTopic", "statusStorageTopic", "offsetStorageTopic");
    }

    @Test
    public void testKafkaConnectV1WrongAuth() {
        Throwable exception = assertThrows(
                KubernetesClientException.class,
                () -> createDeleteCustomResource("KafkaConnect-v1-wrong-auth.yaml"));

        assertThat(exception.getMessage(), allOf(
                CoreMatchers.containsStringIgnoringCase("Unsupported value: \"oauth\""),
                CoreMatchers.containsStringIgnoringCase("supported values: \"tls\", \"scram-sha-256\", \"scram-sha-512\", \"plain\", \"custom\""))
        );
    }

    @Test
    public void testKafkaConnectV1WrongTracing() {
        Throwable exception = assertThrows(
                KubernetesClientException.class,
                () -> createDeleteCustomResource("KafkaConnect-v1-wrong-tracing.yaml"));

        assertThat(exception.getMessage(), allOf(
                CoreMatchers.containsStringIgnoringCase("Unsupported value: \"jaeger\""),
                CoreMatchers.containsStringIgnoringCase("supported values: \"opentelemetry\""))
        );
    }

    @BeforeAll
    void setupEnvironment() {
        client = new KubernetesClientBuilder().withConfig(new ConfigBuilder().withNamespace(NAMESPACE).build()).build();
        CrdUtils.createCrd(client, CrdUtils.CRD_KAFKA_CONNECT_NAME, CrdUtils.CRD_KAFKA_CONNECT);
        TestUtils.createNamespace(client, NAMESPACE);
    }

    @AfterAll
    void teardownEnvironment() {
        CrdUtils.deleteCrd(client, CrdUtils.CRD_KAFKA_CONNECT_NAME);
        TestUtils.deleteNamespace(client, NAMESPACE);
        client.close();
    }
}
