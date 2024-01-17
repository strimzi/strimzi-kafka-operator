/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.mirrormaker;

import io.strimzi.api.kafka.model.AbstractCrdIT;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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

    @SuppressWarnings("deprecation")
    @Test
    void testKafkaMirrorMakerScaling() {
        createScaleDelete(KafkaMirrorMaker.class, "KafkaMirrorMaker.yaml");
    }

    @Test
    void testKafkaMirrorMakerMinimal() {
        createDeleteCustomResource("KafkaMirrorMaker-minimal.yaml");
    }

    @Test
    void testKafkaMirrorMakerWithExtraProperty() {
        // oc tool does not fail with extra properties, it shows only a warning. So this test does not pass on OpenShift
        assumeKube();

        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> createDeleteCustomResource("KafkaMirrorMaker-with-extra-property.yaml"));

        assertThat(exception.getMessage(), containsString("unknown field \"extra\""));
    }

    @Test
    void testKafkaMirrorMakerWithMissingRequired() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> createDeleteCustomResource("KafkaMirrorMaker-with-missing-required-property.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(),
                "bootstrapServers",
                "producer");
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
            KubeClusterException.class,
            () -> createDeleteCustomResource("KafkaMirrorMaker-with-tls-auth-with-missing-required.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(),
                "spec.producer.authentication.certificateAndKey.certificate",
                "spec.producer.authentication.certificateAndKey.key");
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
        cluster.createCustomResources(TestUtils.CRD_KAFKA_MIRROR_MAKER);
        cluster.waitForCustomResourceDefinition("kafkamirrormakers.kafka.strimzi.io");
        cluster.createNamespace(NAMESPACE);

        try {
            Thread.sleep(1_000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @AfterAll
    void teardownEnvironment() {
        cluster.deleteCustomResources();
        cluster.deleteNamespaces();
    }
}

