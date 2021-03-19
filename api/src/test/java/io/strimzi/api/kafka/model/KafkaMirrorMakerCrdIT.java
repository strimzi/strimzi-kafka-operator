/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
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
    void testKafkaMirrorMakerScaling() {
        createScaleDelete(KafkaMirrorMaker.class, "KafkaMirrorMaker.yaml");
    }

    @Test
    void testKafkaMirrorMakerMinimal() {
        createDeleteCustomResource("KafkaMirrorMaker-minimal.yaml");
    }

    @Disabled("See https://github.com/strimzi/strimzi-kafka-operator/issues/4606")
    @Test
    void testCreateKafkaMirrorMakerWithExtraProperty() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> createDeleteCustomResource("KafkaMirrorMaker-with-extra-property.yaml"));

        assertThat(exception.getMessage(), containsString("unknown field \"extra\""));
    }

    @Test
    void testKafkaMirrorMakerWithMissingRequired() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> {
                createDeleteCustomResource("KafkaMirrorMaker-with-missing-required-property.yaml");
            });

        assertMissingRequiredPropertiesMessage(exception.getMessage(),
                "bootstrapServers",
                "producer",
                "whitelist");
    }

    @Test
    void testKafkaMirrorMakerWithTls() {
        createDeleteCustomResource("KafkaMirrorMaker-with-tls.yaml");
    }

    @Test
    void testKafkaMirrorMakerWithTlsAuth() {
        createDeleteCustomResource("KafkaMirrorMaker-with-tls-auth.yaml");
    }

    @Test
    void testKafkaMirrorMakerWithTlsAuthWithMissingRequired() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> createDeleteCustomResource("KafkaMirrorMaker-with-tls-auth-with-missing-required.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(),
                "spec.producer.authentication.certificateAndKey.certificate",
                "spec.producer.authentication.certificateAndKey.key",
                "spec.whitelist");
    }

    @Test
    void testKafkaMirrorMakerWithScramSha512Auth() {
        createDeleteCustomResource("KafkaMirrorMaker-with-scram-sha-512-auth.yaml");
    }

    @Test
    void testKafkaMirrorMakerWithTemplate() {
        createDeleteCustomResource("KafkaMirrorMaker-with-template.yaml");
    }

    @Test
    void testKafkaMirrorMakerWithCommitAndAbort() {
        createDeleteCustomResource("KafkaMirrorMaker-with-commit-and-abort.yaml");
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

