/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.fabric8.kubernetes.client.CustomResource;
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
import io.strimzi.test.ReadWriteUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Round-trips the {@code -minimal} and {@code -regular} YAML fixtures that {@link CrdIT} also installs into a
 * Kubernetes cluster, so the unit-level serialization tests and the cluster-level CRD acceptance tests share input
 * data. Each input fixture has a hand-maintained {@code .out.yaml} sibling that records the canonical Jackson output;
 * if the serialization changes (new field, new default, reordered {@code @JsonPropertyOrder}, ...), update the
 * {@code .out.yaml} fixture by hand.
 */
public class CrdTest {
    static Stream<Arguments> roundTripFixtures() {
        return Stream.of(
                fixturePair(KafkaBridge.class, "KafkaBridge"),
                fixturePair(KafkaConnect.class, "KafkaConnect"),
                fixturePair(KafkaConnector.class, "KafkaConnector"),
                fixturePair(Kafka.class, "Kafka"),
                fixturePair(KafkaMirrorMaker2.class, "KafkaMirrorMaker2"),
                fixturePair(KafkaNodePool.class, "KafkaNodePool"),
                fixturePair(StrimziPodSet.class, "StrimziPodSet"),
                fixturePair(KafkaRebalance.class, "KafkaRebalance"),
                fixturePair(KafkaTopic.class, "KafkaTopic"),
                fixturePair(KafkaUser.class, "KafkaUser")
        ).flatMap(s -> s);
    }

    @SuppressWarnings("rawtypes") // The CustomResource class it not types
    private static Stream<Arguments> fixturePair(Class<? extends CustomResource> crdClass, String baseName) {
        return Stream.of(
                Arguments.of(crdClass, baseName + "-minimal.yaml", baseName + "-minimal.out.yaml"),
                Arguments.of(crdClass, baseName + "-regular.yaml", baseName + "-regular.out.yaml")
        );
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("roundTripFixtures")
    @SuppressWarnings("rawtypes") // The CustomResource class it not types
    <R extends CustomResource> void roundTrip(Class<R> crdClass, String inputResource, String expectedResource) {
        // We read the source YAML as Java object, serialize it to YAML again, and compare it with expected serialized YAML
        // This checks that we can read the YAML and serialize it again
        R fromYaml = ReadWriteUtils.readObjectFromYamlFileInResources(inputResource, crdClass);
        String toYaml = ReadWriteUtils.writeObjectToYamlString(fromYaml);
        String expectedYaml = ReadWriteUtils.readFileFromResources(crdClass, expectedResource);
        assertThat(toYaml.trim(), is(expectedYaml.trim()));

        // We try to re-read the YAML we just serialized and check that it is still the same
        // This checks that the serialzed YAML is still valid and stable
        R rereadFromYaml = ReadWriteUtils.readObjectFromYamlString(toYaml, crdClass);
        assertThat(ReadWriteUtils.writeObjectToYamlString(rereadFromYaml).trim(), is(expectedYaml.trim()));
        assertThat(fromYaml, is(rereadFromYaml));
    }
}
