/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.user;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.model.AbstractCrdIT;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
        assertThrows(KubernetesClientException.class, () -> createScaleDelete(KafkaUser.class, "KafkaUser.yaml"));
    }

    @Test
    void testKafkaUserV1beta1() {
        createDeleteCustomResource("KafkaUserV1beta1.yaml");
    }

    @Test
    void testKafkaUserMinimal() {
        createDeleteCustomResource("KafkaUser-minimal.yaml");
    }

    @BeforeAll
    void setupEnvironment() {
        client = new KubernetesClientBuilder().withConfig(new ConfigBuilder().withNamespace(NAMESPACE).build()).build();
        TestUtils.createCrd(client, KafkaUser.CRD_NAME, TestUtils.CRD_KAFKA_USER);
        TestUtils.createNamespace(client, NAMESPACE);
    }

    @AfterAll
    void teardownEnvironment() {
        TestUtils.deleteCrd(client, KafkaUser.CRD_NAME);
        TestUtils.deleteNamespace(client, NAMESPACE);
        client.close();
    }
}
