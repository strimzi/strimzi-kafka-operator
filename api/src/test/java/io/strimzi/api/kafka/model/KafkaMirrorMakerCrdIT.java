/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterException;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertTrue;

/**
 * The purpose of this test is to confirm that we can create a
 * resource from the POJOs, serialize it and create the resource in K8S.
 * I.e. that such instance resources obtained from POJOs are valid according to the schema
 * validation done by K8S.
 */
public class KafkaMirrorMakerCrdIT extends AbstractCrdIT {

    public static final String NAMESPACE = "kafkamirrormaker-crd-it";

    @Test
    void testKafkaMirrorMaker() {
        createDelete(KafkaMirrorMaker.class, "KafkaMirrorMaker.yaml");
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
        try {
            createDelete(KafkaMirrorMaker.class, "KafkaMirrorMaker-with-missing-required-property.yaml");
        } catch (KubeClusterException.InvalidResource e) {
            assertTrue(e.getMessage(), e.getMessage().contains("spec.consumer.bootstrapServers in body is required"));
            assertTrue(e.getMessage(), e.getMessage().contains("spec.producer in body is required"));
            assertTrue(e.getMessage(), e.getMessage().contains("spec.whitelist in body is required"));
        }
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
        try {
            createDelete(KafkaMirrorMaker.class, "KafkaMirrorMaker-with-tls-auth-with-missing-required.yaml");
        } catch (KubeClusterException.InvalidResource e) {
            assertTrue(e.getMessage().contains("spec.producer.authentication.certificateAndKey.certificate in body is required"));
            assertTrue(e.getMessage().contains("spec.producer.authentication.certificateAndKey.key in body is required"));
        }
    }

    @Test
    void testKafkaMirrorMakerWithScramSha512Auth() {
        createDelete(KafkaMirrorMaker.class, "KafkaMirrorMaker-with-scram-sha-512-auth.yaml");
    }

    @Test
    void testKafkaMirrorMakerWithTemplate() {
        createDelete(KafkaMirrorMaker.class, "KafkaMirrorMaker-with-template.yaml");
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

