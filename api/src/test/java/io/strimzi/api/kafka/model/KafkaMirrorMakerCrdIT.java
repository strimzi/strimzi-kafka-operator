/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The purpose of this test is to confirm that we can create a
 * resource from the POJOs, serialize it and create the resource in K8S.
 * I.e. that such instance resources obtained from POJOs are valid according to the schema
 * validation done by K8S.
 */
public class KafkaMirrorMakerCrdIT extends AbstractCrdIT {

    public static final String NAMESPACE = "kafkamirrormaker-crd-it";

    @Test
    void testKafkaMirrorMakerV1alpha1() {
        assumeKube1_11Plus();
        createDelete(KafkaMirrorMaker.class, "KafkaMirrorMakerV1alpha1.yaml");
    }

    @Test
    void testKafkaMirrorMakerScaling() {
        createScaleDelete(KafkaMirrorMaker.class, "KafkaMirrorMaker.yaml");
    }

    @Test
    void testKafkaMirrorMakerV1beta1() {
        createDelete(KafkaMirrorMaker.class, "KafkaMirrorMakerV1beta1.yaml");
    }

    @Test
    void testKafkaMirrorMakerMinimal() {
        createDelete(KafkaMirrorMaker.class, "KafkaMirrorMaker-minimal.yaml");
    }

    @Test
    void testKafkaMirrorMakerWithExtraProperty() {
        createDelete(KafkaMirrorMaker.class, "KafkaMirrorMaker-with-extra-property.yaml");
    }

    @Test
    void testKafkaMirrorMakerWithMissingRequired() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> {
                createDelete(KafkaMirrorMaker.class, "KafkaMirrorMaker-with-missing-required-property.yaml");
            });

        assertMissingRequiredPropertiesMessage(exception.getMessage(),
                "spec.consumer.bootstrapServers",
                "spec.producer",
                "spec.whitelist");
    }

    @Test
    void testKafkaMirrorMakerWithTls() {
        createDelete(KafkaMirrorMaker.class, "KafkaMirrorMaker-with-tls.yaml");
    }

    @Test
    void testKafkaMirrorMakerWithTlsAuth() {
        createDelete(KafkaMirrorMaker.class, "KafkaMirrorMaker-with-tls-auth.yaml");
    }

    @Test
    void testKafkaMirrorMakerWithTlsAuthWithMissingRequired() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> createDelete(KafkaMirrorMaker.class, "KafkaMirrorMaker-with-tls-auth-with-missing-required.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(),
                "spec.producer.authentication.certificateAndKey.certificate",
                "spec.producer.authentication.certificateAndKey.key",
                "spec.whitelist");
    }

    @Test
    void testKafkaMirrorMakerWithScramSha512Auth() {
        createDelete(KafkaMirrorMaker.class, "KafkaMirrorMaker-with-scram-sha-512-auth.yaml");
    }

    @Test
    void testKafkaMirrorMakerWithTemplate() {
        createDelete(KafkaMirrorMaker.class, "KafkaMirrorMaker-with-template.yaml");
    }

    @Test
    void testKafkaMirrorMakerWithCommitAndAbort() {
        createDelete(KafkaMirrorMaker.class, "KafkaMirrorMaker-with-commit-and-abort.yaml");
    }

    @BeforeAll
    void setupEnvironment() {
        cluster.createNamespace(NAMESPACE);
        cluster.createCustomResources(TestUtils.CRD_KAFKA_MIRROR_MAKER);
        waitForCrd("crd", "kafkamirrormakers.kafka.strimzi.io");
    }

    @AfterAll
    void teardownEnvironment() {
        cluster.deleteCustomResources();
        cluster.deleteNamespaces();
    }
}

