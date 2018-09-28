/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.test.Namespace;
import io.strimzi.test.Resources;
import io.strimzi.test.StrimziRunner;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterException;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

/**
 * The purpose of this test is to confirm that we can create a
 * resource from the POJOs, serialize it and create the resource in K8S.
 * I.e. that such instance resources obtained from POJOs are valid according to the schema
 * validation done by K8S.
 */
@RunWith(StrimziRunner.class)
@Namespace(KafkaMirrorMakerCrdIT.NAMESPACE)
@Resources(value = TestUtils.CRD_KAFKA_MIRROR_MAKER, asAdmin = true)
public class KafkaMirrorMakerCrdIT extends AbstractCrdIT {

    public static final String NAMESPACE = "kafkamirrormaker-crd-it";

    @Test
    public void testKafkaMirrorMaker() {
        createDelete(KafkaMirrorMaker.class, "KafkaMirrorMaker.yaml");
    }

    @Test
    public void testKafkaMirrorMakerMinimal() {
        createDelete(KafkaMirrorMaker.class, "KafkaMirrorMaker-minimal.yaml");
    }

    @Test
    public void testKafkaMirrorMakerWithExtraProperty() {
        createDelete(KafkaMirrorMaker.class, "KafkaMirrorMaker-with-extra-property.yaml");
    }

    @Test
    public void testKafkaMirrorMakerWithMissingRequired() {
        try {
            createDelete(KafkaMirrorMaker.class, "KafkaMirrorMaker-with-missing-required-property.yaml");
        } catch (KubeClusterException.InvalidResource e) {
            assertTrue(e.getMessage(), e.getMessage().contains("spec.consumer.bootstrapServers in body is required"));
            assertTrue(e.getMessage(), e.getMessage().contains("spec.producer in body is required"));
            assertTrue(e.getMessage(), e.getMessage().contains("spec.whitelist in body is required"));
        }
    }

    @Test
    public void testKafkaMirrorMakerWithTls() {
        createDelete(KafkaMirrorMaker.class, "KafkaMirrorMaker-with-tls.yaml");
    }

    @Test
    public void testKafkaMirrorMakerWithTlsAuth() {
        createDelete(KafkaMirrorMaker.class, "KafkaMirrorMaker-with-tls-auth.yaml");
    }

    @Test
    public void testKafkaWithTlsAuthWithMissingRequired() {
        try {
            createDelete(KafkaMirrorMaker.class, "KafkaMirrorMaker-with-tls-auth-with-missing-required.yaml");
        } catch (KubeClusterException.InvalidResource e) {
            assertTrue(e.getMessage().contains("spec.producer.authentication.certificateAndKey.certificate in body is required"));
            assertTrue(e.getMessage().contains("spec.producer.authentication.certificateAndKey.key in body is required"));
        }
    }

    @Test
    public void testKafkaWithScramSha512Auth() {
        createDelete(KafkaMirrorMaker.class, "KafkaMirrorMaker-with-scram-sha-512-auth.yaml");
    }
}

