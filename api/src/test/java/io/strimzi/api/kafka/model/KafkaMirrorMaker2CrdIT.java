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
public class KafkaMirrorMaker2CrdIT extends AbstractCrdIT {
    public static final String NAMESPACE = "kafkamirrormaker2-crd-it";

    @Test
    void testKafkaMirrorMaker2V1alpha1() {
        assumeKube1_11Plus();
        createDelete(KafkaMirrorMaker2.class, "KafkaMirrorMaker2V1alpha1.yaml");
    }

    @Test
    void testKafkaMirrorMaker2Scaling() {
        createScaleDelete(KafkaMirrorMaker2.class, "KafkaMirrorMaker2.yaml");
    }

    @Test
    void testKafkaMirrorMaker2Minimal() {
        createDelete(KafkaMirrorMaker2.class, "KafkaMirrorMaker2-minimal.yaml");
    }

    @Test
    void testKafkaMirrorMaker2WithExtraProperty() {
        createDelete(KafkaMirrorMaker2.class, "KafkaMirrorMaker2-with-extra-property.yaml");
    }

    @Test
    void testKafkaMirrorMaker2WithMissingRequired() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> createDelete(KafkaMirrorMaker2.class, "KafkaMirrorMaker2-with-missing-required-property.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "spec.connectCluster", "spec.clusters.alias", "spec.mirrors.sourceCluster", "spec.mirrors.targetCluster");
    }

    @Test
    void testKafkaMirrorMaker2WithInvalidReplicas() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> createDelete(KafkaMirrorMaker2.class, "KafkaMirrorMaker2-with-invalid-replicas.yaml"));

        assertThat(exception.getMessage(),
                containsStringIgnoringCase("spec.replicas in body must be of type integer: \"string\""));
    }

    @Test
    void testKafkaMirrorMaker2WithTls() {
        createDelete(KafkaMirrorMaker2.class, "KafkaMirrorMaker2-with-tls.yaml");
    }

    @Test
    void testKafkaMirrorMaker2WithTlsAuth() {
        createDelete(KafkaMirrorMaker2.class, "KafkaMirrorMaker2-with-tls-auth.yaml");
    }

    @Test
    void testKafkaMirrorMaker2WithTlsAuthWithMissingRequired() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> createDelete(KafkaMirrorMaker2.class, "KafkaMirrorMaker2-with-tls-auth-with-missing-required.yaml"));
        
        assertMissingRequiredPropertiesMessage(exception.getMessage(), "spec.clusters.authentication.certificateAndKey.certificate", "spec.clusters.authentication.certificateAndKey.key");
    }

    @Test
    void testKafkaMirrorMaker2WithScramSha512Auth() {
        createDelete(KafkaMirrorMaker2.class, "KafkaMirrorMaker2-with-scram-sha-512-auth.yaml");
    }

    @Test
    public void testKafkaMirrorMaker2WithTemplate() {
        createDelete(KafkaMirrorMaker2.class, "KafkaMirrorMaker2-with-template.yaml");
    }

    @Test
    public void testKafkaMirrorMaker2WithExternalConfiguration() {
        createDelete(KafkaMirrorMaker2.class, "KafkaMirrorMaker2-with-external-configuration.yaml");
    }

    @Test
    public void testKafkaMirrorMaker2WithInvalidExternalConfiguration() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> createDelete(KafkaMirrorMaker2.class, "KafkaMirrorMaker2-with-invalid-external-configuration.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "spec.externalConfiguration.env.valueFrom");
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
