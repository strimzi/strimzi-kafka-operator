/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Exercises every CEL validation rule reachable on a Strimzi CRD against a real Kubernetes API server, by
 * submitting a deliberately-invalid custom resource and asserting that the rule's message surfaces in the rejection.
 *
 * Each parameter row in {@link #celRules()} corresponds to one CEL rule. The fixtures used are intentionally
 * minimal: they contain only the fields required to reach (and violate) the rule under test.
 */
public class CelValidationIT implements TestSeparator {
    private static final String NAMESPACE = "cel-it";

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

    static Stream<Arguments> celRules() {
        final String topologyKeyRequired = "topologyKey property is required";
        final String envVarNameRequired = "envVarName property is required";
        final String valueFromRequired = "valueFrom property is required";
        final String ccStrimziTypeNotSupported = "value type not supported";

        return Stream.of(
                Arguments.of(Kafka.class, "Kafka-cel-rack-topology-label-missing-key.yaml", topologyKeyRequired),
                Arguments.of(Kafka.class, "Kafka-cel-rack-environment-variable-missing-name.yaml", envVarNameRequired),
                Arguments.of(Kafka.class, "Kafka-cel-metrics-jmx-missing-valueFrom.yaml", valueFromRequired),
                Arguments.of(Kafka.class, "Kafka-cel-cc-metrics-jmx-missing-valueFrom.yaml", valueFromRequired),
                Arguments.of(Kafka.class, "Kafka-cel-cc-metrics-strimzi-rejected.yaml", ccStrimziTypeNotSupported),

                Arguments.of(KafkaConnect.class, "KafkaConnect-cel-rack-topology-label-missing-key.yaml", topologyKeyRequired),
                Arguments.of(KafkaConnect.class, "KafkaConnect-cel-rack-environment-variable-missing-name.yaml", envVarNameRequired),
                Arguments.of(KafkaConnect.class, "KafkaConnect-cel-metrics-jmx-missing-valueFrom.yaml", valueFromRequired),

                Arguments.of(KafkaBridge.class, "KafkaBridge-cel-rack-topology-label-missing-key.yaml", topologyKeyRequired),
                Arguments.of(KafkaBridge.class, "KafkaBridge-cel-rack-environment-variable-missing-name.yaml", envVarNameRequired),
                Arguments.of(KafkaBridge.class, "KafkaBridge-cel-metrics-jmx-missing-valueFrom.yaml", valueFromRequired),

                Arguments.of(KafkaMirrorMaker2.class, "KafkaMirrorMaker2-cel-rack-topology-label-missing-key.yaml", topologyKeyRequired),
                Arguments.of(KafkaMirrorMaker2.class, "KafkaMirrorMaker2-cel-rack-environment-variable-missing-name.yaml", envVarNameRequired),
                Arguments.of(KafkaMirrorMaker2.class, "KafkaMirrorMaker2-cel-metrics-jmx-missing-valueFrom.yaml", valueFromRequired)
        );
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("celRules")
    void testCelRule(Class<? extends CustomResource> crdClass, String yamlResource, String expectedMessage) {
        String content = ReadWriteUtils.readFileFromResources(crdClass, yamlResource);
        KubernetesClientException exception = assertThrows(
                KubernetesClientException.class,
                () -> CrdUtils.createDeleteCustomResource(client, content));
        assertThat(exception.getMessage(), containsStringIgnoringCase(expectedMessage));
    }
}
