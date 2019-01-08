/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.test.Annotations.Namespace;
import io.strimzi.test.Annotations.Resources;
import io.strimzi.test.Extensions.StrimziExtension;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.Assert.assertTrue;

/**
 * The purpose of this test is to confirm that we can create a
 * resource from the POJOs, serialize it and create the resource in K8S.
 * I.e. that such instance resources obtained from POJOs are valid according to the schema
 * validation done by K8S.
 */
@ExtendWith(StrimziExtension.class)
@Namespace(KafkaUserCrdIT.NAMESPACE)
@Resources(value = TestUtils.CRD_KAFKA_USER, asAdmin = true)
public class KafkaUserCrdIT extends AbstractCrdIT {
    public static final String NAMESPACE = "kafkausercrd-it";

    @Test
    void testKafkaUser() {
        createDelete(KafkaUser.class, "KafkaUser.yaml");
    }

    @Test
    void testKafkaUserMinimal() {
        createDelete(KafkaUser.class, "KafkaUser-minimal.yaml");
    }

    @Test
    void testKafkaUserWithExtraProperty() {
        createDelete(KafkaUser.class, "KafkaUser-with-extra-property.yaml");
    }

    @Test
    void testKafkaUserWithMissingRequired() {
        try {
            createDelete(KafkaUser.class, "KafkaUser-with-missing-required.yaml");
        } catch (KubeClusterException.InvalidResource e) {
            assertTrue(e.getMessage().contains("spec.authentication in body is required"));
        }
    }
}
