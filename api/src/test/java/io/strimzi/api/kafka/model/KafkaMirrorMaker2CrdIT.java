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

    @Disabled("See https://github.com/strimzi/strimzi-kafka-operator/issues/4606")
    @Test
    void testCreateKafkaMirrorMaker2WithExtraProperty() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> createDeleteCustomResource("KafkaMirrorMaker2-with-extra-property.yaml"));

        assertThat(exception.getMessage(), containsString("unknown field \"extra\""));
    }

    @Test
    void testKafkaMirrorMaker2WithMissingRequired() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> createDeleteCustomResource("KafkaMirrorMaker2-with-missing-required-property.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "connectCluster", "clusters.alias", "sourceCluster", "targetCluster");
    }

    @Test
    void testKafkaMirrorMaker2WithInvalidReplicas() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> createDeleteCustomResource("KafkaMirrorMaker2-with-invalid-replicas.yaml"));

        assertThat(exception.getMessage(),
                containsStringIgnoringCase("Invalid value: \"string\": spec.replicas in body must be of type integer: \"string\""));
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
            KubeClusterException.class,
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
            KubeClusterException.class,
            () -> createDeleteCustomResource("KafkaMirrorMaker2-with-invalid-external-configuration.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "valueFrom");
    }

    @BeforeAll
    void setupEnvironment() {
        cluster.createNamespace(NAMESPACE);
        cluster.createCustomResources(TestUtils.CRD_KAFKA_MIRROR_MAKER_2);
        waitForCrd("crd", "kafkamirrormaker2s.kafka.strimzi.io");
    }

    @AfterAll
    void teardownEnvironment() {
        cluster.deleteCustomResources();
        cluster.deleteNamespaces();
    }
}
