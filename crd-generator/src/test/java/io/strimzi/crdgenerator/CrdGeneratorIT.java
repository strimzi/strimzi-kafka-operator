/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.test.CrdUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.interfaces.TestSeparator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Generates the CRD YAML for {@link ExampleIntegrationCrd} via {@link CrdGenerator}, installs it into a real
 * Kubernetes API server, and verifies that the schema-level behaviours actually take effect: required fields,
 * enum values, polymorphic discriminators, {@code minimum}, {@code minItems}, {@code oneOf}, CEL rules, and the
 * {@code scale} subresource.
 */
public class CrdGeneratorIT implements TestSeparator {
    private static final String NAMESPACE = "crd-generator-it";
    private static final String CRD_NAME = "exampleintegrations.it.crdgenerator.strimzi.io";
    private static final String API_VERSION = "it.crdgenerator.strimzi.io/v1";
    private static final String KIND = "ExampleIntegration";

    private static KubernetesClient client;

    @BeforeAll
    static void beforeAll() throws IOException {
        client = new KubernetesClientBuilder().withConfig(new ConfigBuilder().withNamespace(NAMESPACE).build()).build();
        TestUtils.createNamespace(client, NAMESPACE);
        CrdUtils.createCrdFromYaml(client, CRD_NAME, generateCrdYaml());
    }

    @AfterAll
    static void afterAll() {
        CrdUtils.deleteCrd(client, CRD_NAME);
        TestUtils.deleteNamespace(client, NAMESPACE);
        client.close();
    }

    @Test
    void testValidResourceIsAccepted() {
        CrdUtils.createDeleteCustomResource(client, """
                apiVersion: it.crdgenerator.strimzi.io/v1
                kind: ExampleIntegration
                metadata:
                  name: valid
                spec:
                  requiredString: hello
                  enumProperty: FOO
                  numericProperty: 10
                  listProperty:
                    - one
                  polymorphic:
                    type: left
                    leftValue: a
                  nested:
                    requiredField: present
                  cel:
                    value: abcdef
                  either: x
                """);
    }

    static Stream<Arguments> rejectedResources() {
        return Stream.of(
                Arguments.of("missing required string",
                        """
                        apiVersion: it.crdgenerator.strimzi.io/v1
                        kind: ExampleIntegration
                        metadata:
                          name: missing-required
                        spec:
                          either: x
                        """,
                        "requiredString"),
                Arguments.of("missing required nested field",
                        """
                        apiVersion: it.crdgenerator.strimzi.io/v1
                        kind: ExampleIntegration
                        metadata:
                          name: missing-nested
                        spec:
                          requiredString: hello
                          either: x
                          nested: {}
                        """,
                        "requiredField"),
                Arguments.of("invalid enum value",
                        """
                        apiVersion: it.crdgenerator.strimzi.io/v1
                        kind: ExampleIntegration
                        metadata:
                          name: invalid-enum
                        spec:
                          requiredString: hello
                          either: x
                          enumProperty: NOPE
                        """,
                        "Unsupported value"),
                Arguments.of("invalid polymorphic discriminator",
                        """
                        apiVersion: it.crdgenerator.strimzi.io/v1
                        kind: ExampleIntegration
                        metadata:
                          name: invalid-polymorphic
                        spec:
                          requiredString: hello
                          either: x
                          polymorphic:
                            type: middle
                        """,
                        "Unsupported value"),
                Arguments.of("numeric value below minimum",
                        """
                        apiVersion: it.crdgenerator.strimzi.io/v1
                        kind: ExampleIntegration
                        metadata:
                          name: below-minimum
                        spec:
                          requiredString: hello
                          either: x
                          numericProperty: 1
                        """,
                        "should be greater than or equal to 5"),
                Arguments.of("list below minItems",
                        """
                        apiVersion: it.crdgenerator.strimzi.io/v1
                        kind: ExampleIntegration
                        metadata:
                          name: empty-list
                        spec:
                          requiredString: hello
                          either: x
                          listProperty: []
                        """,
                        "should have at least 1 items"),
                Arguments.of("oneOf neither alternative set",
                        """
                        apiVersion: it.crdgenerator.strimzi.io/v1
                        kind: ExampleIntegration
                        metadata:
                          name: oneof-neither
                        spec:
                          requiredString: hello
                        """,
                        "Invalid value"),
                Arguments.of("oneOf both alternatives set",
                        """
                        apiVersion: it.crdgenerator.strimzi.io/v1
                        kind: ExampleIntegration
                        metadata:
                          name: oneof-both
                        spec:
                          requiredString: hello
                          either: x
                          or: y
                        """,
                        "Invalid value"),
                Arguments.of("CEL rule violation",
                        """
                        apiVersion: it.crdgenerator.strimzi.io/v1
                        kind: ExampleIntegration
                        metadata:
                          name: cel-violation
                        spec:
                          requiredString: hello
                          either: x
                          cel:
                            value: abc
                        """,
                        "value needs to be at least 5 characters long")
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("rejectedResources")
    void testRejected(String scenario, String yaml, String expectedMessage) {
        KubernetesClientException exception = assertThrows(KubernetesClientException.class,
                () -> CrdUtils.createDeleteCustomResource(client, yaml));
        assertThat(exception.getMessage(), containsStringIgnoringCase(expectedMessage));
    }

    @Test
    void testScaleSubresource() {
        String yaml = """
                apiVersion: it.crdgenerator.strimzi.io/v1
                kind: ExampleIntegration
                metadata:
                  name: scalable
                spec:
                  requiredString: hello
                  either: x
                  replicas: 1
                """;

        try {
            client.resource(yaml).create();

            client.genericKubernetesResources(API_VERSION, KIND).withName("scalable").scale(7);

            GenericKubernetesResource updated = client.genericKubernetesResources(API_VERSION, KIND).withName("scalable").get();
            @SuppressWarnings("unchecked")
            Number replicas = (Number) ((Map<String, Object>) updated.get("spec")).get("replicas");
            assertEquals(7, replicas.intValue());
        } finally {
            client.genericKubernetesResources(API_VERSION, KIND).withName("scalable").delete();
        }
    }

    private static String generateCrdYaml() throws IOException {
        CrdGenerator generator = new CrdGenerator(KubeVersion.V1_16_PLUS, ApiVersion.V1, CrdGenerator.YAML_MAPPER,
                emptyMap(), err -> { }, emptyList(), null, null,
                new CrdGenerator.NoneConversionStrategy(), null);
        StringWriter writer = new StringWriter();
        generator.generate(ExampleIntegrationCrd.class, writer);
        return writer.toString();
    }
}
