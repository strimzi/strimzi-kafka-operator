/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.topic;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.model.AbstractCrdIT;
import io.strimzi.test.CrdUtils;
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
public class KafkaTopicCrdIT extends AbstractCrdIT {
    public static final String NAMESPACE = "topiccrd-it";

    @Test
    void testKafkaTopicV1alpha1() {
        createDeleteCustomResource("KafkaTopicV1alpha1.yaml");
    }

    @Test
    void testKafkaTopicIsNotScaling() {
        assertThrows(KubernetesClientException.class, () -> createScaleDelete(KafkaTopic.class, "KafkaTopic.yaml"));
    }

    @Test
    void testKafkaTopicV1beta1() {
        createDeleteCustomResource("KafkaTopicV1beta1.yaml");
    }

    @Test
    void testKafkaTopicMinimal() {
        createDeleteCustomResource("KafkaTopic-minimal.yaml");
    }

    @BeforeAll
    void setupEnvironment() {
        client = new KubernetesClientBuilder().withConfig(new ConfigBuilder().withNamespace(NAMESPACE).build()).build();
        CrdUtils.createCrd(client, CrdUtils.CRD_KAFKA_TOPIC_NAME, CrdUtils.CRD_TOPIC);
        TestUtils.createNamespace(client, NAMESPACE);
    }

    @AfterAll
    void teardownEnvironment() {
        CrdUtils.deleteCrd(client, CrdUtils.CRD_KAFKA_TOPIC_NAME);
        TestUtils.deleteNamespace(client, NAMESPACE);
        client.close();
    }
}
