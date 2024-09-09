/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.mirrormaker2;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.model.AbstractCrdIT;
import io.strimzi.test.CrdUtils;
import io.strimzi.test.TestUtils;
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
public class KafkaMirrorMaker2CrdIT extends AbstractCrdIT {
    public static final String NAMESPACE = "kafkamirrormaker2-crd-it";

    @Test
    void testKafkaMirrorMaker2Scaling() {
        createScaleDelete(KafkaMirrorMaker2.class, "KafkaMirrorMaker2.yaml");
    }

    @Test
    void testKafkaMirrorMaker2Minimal() {
        createDeleteCustomResource("KafkaMirrorMaker2-minimal.yaml");
    }

    @Test
    void testKafkaMirrorMaker2WithMissingRequired() {
        Throwable exception = assertThrows(
                KubernetesClientException.class,
                () -> createDeleteCustomResource("KafkaMirrorMaker2-with-missing-required-property.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "connectCluster", "clusters.alias", "sourceCluster", "targetCluster");
    }

    @Test
    void testKafkaMirrorMaker2WithInvalidReplicas() {
        Throwable exception = assertThrows(
                KubernetesClientException.class,
                () -> createDeleteCustomResource("KafkaMirrorMaker2-with-invalid-replicas.yaml"));

        assertThat(exception.getMessage(), containsStringIgnoringCase("Invalid value: \"string\": spec.replicas in body must be of type integer: \"string\""));
    }

    @Test
    void testKafkaMirrorMaker2WithTls() {
        createDeleteCustomResource("KafkaMirrorMaker2-with-tls.yaml");
    }

    @Test
    void testKafkaMirrorMaker2WithTlsAuth() {
        createDeleteCustomResource("KafkaMirrorMaker2-with-tls-auth.yaml");
    }

    @Test
    void testKafkaMirrorMaker2WithTlsAuthWithMissingRequired() {
        Throwable exception = assertThrows(
                KubernetesClientException.class,
                () -> createDeleteCustomResource("KafkaMirrorMaker2-with-tls-auth-with-missing-required.yaml"));
        
        assertMissingRequiredPropertiesMessage(exception.getMessage(), "certificate", "key");
    }

    @Test
    void testKafkaMirrorMaker2WithScramSha512Auth() {
        createDeleteCustomResource("KafkaMirrorMaker2-with-scram-sha-512-auth.yaml");
    }

    @Test
    public void testKafkaMirrorMaker2WithTemplate() {
        createDeleteCustomResource("KafkaMirrorMaker2-with-template.yaml");
    }

    @Test
    public void testKafkaMirrorMaker2WithExternalConfiguration() {
        createDeleteCustomResource("KafkaMirrorMaker2-with-external-configuration.yaml");
    }

    @Test
    public void testKafkaMirrorMaker2WithInvalidExternalConfiguration() {
        Throwable exception = assertThrows(
                KubernetesClientException.class,
                () -> createDeleteCustomResource("KafkaMirrorMaker2-with-invalid-external-configuration.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "valueFrom");
    }

    @BeforeAll
    void setupEnvironment() {
        client = new KubernetesClientBuilder().withConfig(new ConfigBuilder().withNamespace(NAMESPACE).build()).build();
        CrdUtils.createCrd(client, KafkaMirrorMaker2.CRD_NAME, CrdUtils.CRD_KAFKA_MIRROR_MAKER_2);
        TestUtils.createNamespace(client, NAMESPACE);
    }

    @AfterAll
    void teardownEnvironment() {
        CrdUtils.deleteCrd(client, KafkaMirrorMaker2.CRD_NAME);
        TestUtils.deleteNamespace(client, NAMESPACE);
        client.close();
    }
}
