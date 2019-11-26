/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterException;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.allOf;
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
public class KafkaMirrorMakerCrdIT extends AbstractCrdIT {

    public static final String NAMESPACE = "kafkamirrormaker-crd-it";

    @Test
    void testKafkaMirrorMakerV1alpha1() {
        assumeKube1_11Plus();
        createDelete(KafkaMirrorMaker.class, "KafkaMirrorMakerV1alpha1.yaml");
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

        String expectedMsg1a = "spec.consumer.bootstrapServers in body is required";
        String expectedMsg1b = "spec.producer in body is required";
        String expectedMsg1c = "spec.whitelist in body is required";

        String expectedMsg2a = "spec.consumer.bootstrapServers: Required value";
        String expectedMSg2b = "spec.whitelist: Required value";
        String expectedMsg2c = "spec.producer: Required value";

        assertThat(exception.getMessage(), anyOf(
                allOf(
                        containsStringIgnoringCase(expectedMsg1a),
                        containsStringIgnoringCase(expectedMsg1b),
                        containsStringIgnoringCase(expectedMsg1c)),
                allOf(
                        containsStringIgnoringCase(expectedMsg2a),
                        containsStringIgnoringCase(expectedMSg2b),
                        containsStringIgnoringCase(expectedMsg2c))
                ));
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
            () -> {
                createDelete(KafkaMirrorMaker.class, "KafkaMirrorMaker-with-tls-auth-with-missing-required.yaml");
            });

        String expectedMsg1a = "spec.producer.authentication.certificateAndKey.certificate in body is required";
        String expectedMsg1b = "spec.producer.authentication.certificateAndKey.key in body is required";

        String expectedMsg2a = "spec.producer.authentication.certificateAndKey.certificate: Required value";
        String expectedMSg2b = "spec.producer.authentication.certificateAndKey.key: Required value";
        String expectedMsg2c = "spec.whitelist: Required value";

        assertThat(exception.getMessage(), anyOf(
                allOf(
                        containsStringIgnoringCase(expectedMsg1a),
                        containsStringIgnoringCase(expectedMsg1b)),
                allOf(
                        containsStringIgnoringCase(expectedMsg2a),
                        containsStringIgnoringCase(expectedMSg2b),
                        containsStringIgnoringCase(expectedMsg2c))
        ));
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
        createNamespace(NAMESPACE);
        createCustomResources(TestUtils.CRD_KAFKA_MIRROR_MAKER);
    }

    @AfterAll
    void teardownEnvironment() {
        deleteCustomResources();
        deleteNamespaces();
    }
}

