/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.test.CrdUtils;
import io.strimzi.test.ReadWriteUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.interfaces.TestSeparator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

/**
 * Installs every Strimzi production CRD once, then exercises each CRD's {@code -minimal} and {@code -regular}
 * YAML fixture against a real Kubernetes API server. Reuses the same fixtures as the {@link CrdTest} unit-level
 * round-trip tests, so the IT and the unit test share input data.
 *
 * CEL validation rules reachable on production CRDs are exercised separately in {@link CelValidationIT}. Generic
 * CRD schema behaviours (required fields, enum types, polymorphic discriminators, {@code minimum}/{@code minItems},
 * {@code oneOf}, the {@code scale} subresource) are exercised once against a synthetic CRD in
 * {@code io.strimzi.crdgenerator.CrdGeneratorIT} in the crd-generator module.
 */
public class CrdIT implements TestSeparator {
    private static final String NAMESPACE = "crd-it";

    private static KubernetesClient client;

    @BeforeAll
    static void beforeAll() {
        client = new KubernetesClientBuilder().withConfig(new ConfigBuilder().withNamespace(NAMESPACE).build()).build();
        TestUtils.createNamespace(client, NAMESPACE);
        CrdUtils.CRDS.forEach((name, path) -> CrdUtils.createCrd(client, name, path));
    }

    @AfterAll
    static void afterAll() {
        CrdUtils.CRDS.keySet().forEach(name -> CrdUtils.deleteCrd(client, name));
        TestUtils.deleteNamespace(client, NAMESPACE);
        client.close();
    }

    static Stream<Arguments> resources() {
        return Stream.of(
                resourcePair(KafkaBridge.class, "KafkaBridge"),
                resourcePair(KafkaConnect.class, "KafkaConnect"),
                resourcePair(KafkaConnector.class, "KafkaConnector"),
                resourcePair(Kafka.class, "Kafka"),
                resourcePair(KafkaMirrorMaker2.class, "KafkaMirrorMaker2"),
                resourcePair(KafkaNodePool.class, "KafkaNodePool"),
                resourcePair(StrimziPodSet.class, "StrimziPodSet"),
                resourcePair(KafkaRebalance.class, "KafkaRebalance"),
                resourcePair(KafkaTopic.class, "KafkaTopic"),
                resourcePair(KafkaUser.class, "KafkaUser")
        ).flatMap(s -> s);
    }

    private static Stream<Arguments> resourcePair(Class<? extends CustomResource> crdClass, String baseName) {
        return Stream.of(
                Arguments.of(crdClass, baseName + "-minimal.yaml"),
                Arguments.of(crdClass, baseName + "-regular.yaml")
        );
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("resources")
    void testCreateDelete(Class<? extends CustomResource> crdClass, String yamlResource) {
        String content = ReadWriteUtils.readFileFromResources(crdClass, yamlResource);
        CrdUtils.createDeleteCustomResource(client, content);
    }
}
