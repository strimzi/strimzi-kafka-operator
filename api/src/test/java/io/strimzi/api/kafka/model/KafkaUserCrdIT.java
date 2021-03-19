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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The purpose of this test is to confirm that we can create a
 * resource from the POJOs, serialize it and create the resource in K8S.
 * I.e. that such instance resources obtained from POJOs are valid according to the schema
 * validation done by K8S.
 */
public class KafkaUserCrdIT extends AbstractCrdIT {
    public static final String NAMESPACE = "kafkausercrd-it";

    @Test
    void testKafkaUserV1alpha1() {
        createDeleteCustomResource("KafkaUserV1alpha1.yaml");
    }

    @Test
    void testKafkaUserIsNotScaling() {
        assertThrows(KubeClusterException.class, () -> createScaleDelete(KafkaUser.class, "KafkaUser.yaml"));
    }

    @Test
    void testKafkaUserV1beta1() {
        createDeleteCustomResource("KafkaUserV1beta1.yaml");
    }

    @Test
    void testKafkaUserMinimal() {
        createDeleteCustomResource("KafkaUser-minimal.yaml");
    }

    @Disabled("See https://github.com/strimzi/strimzi-kafka-operator/issues/4606")
    @Test
    void testCreateKafkaUserWithExtraProperty() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> createDeleteCustomResource("KafkaUser-with-extra-property.yaml"));

        assertThat(exception.getMessage(), containsString("unknown field \"thisPropertyIsNotInTheSchema\""));
    }

    @BeforeAll
    void setupEnvironment() {
        cluster.createNamespace(NAMESPACE);
        cluster.createCustomResources(TestUtils.CRD_KAFKA_USER);
        waitForCrd("crd", "kafkausers.kafka.strimzi.io");
    }

    @AfterAll
    void teardownEnvironment() {
        cluster.deleteCustomResources();
        cluster.deleteNamespaces();
    }
}
