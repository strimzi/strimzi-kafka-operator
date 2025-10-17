/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.cli;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.strimzi.kafka.api.conversion.v1.converter.conversions.MultipartConversions;
import io.strimzi.test.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ConvertResourceCommandSampleResourcesIT {
    private static final String NAMESPACE = "api-conversion";

    private static KubernetesClient client;

    @BeforeEach
    public void beforeEach()    {
        MultipartConversions.remove();
    }

    @BeforeAll
    static void setupEnvironment() {
        client = new KubernetesClientBuilder().withConfig(new ConfigBuilder().withNamespace(NAMESPACE).build()).build();
        ConversionTestUtils.createV1beta2Crds(client);
        TestUtils.createNamespace(client, NAMESPACE);
    }

    @AfterAll
    static void teardownEnvironment() {
        ConversionTestUtils.deleteAllCrds(client);
        TestUtils.deleteNamespace(client, NAMESPACE);
        client.close();
    }

    @ParameterizedTest
    @MethodSource("correctSamples")
    public void testConvertCorrectSampled(String testFile) {
        convertSampleResource(testFile, 0, null);
    }

    private static Stream<Arguments> correctSamples()    {
        return Stream.of(
                Arguments.of("bridge-metrics.yaml"),
                Arguments.of("bridge-no-replicas.yaml"),
                Arguments.of("bridge-jaeger-tracing.yaml"),
                Arguments.of("bridge-opentelemetry-tracing.yaml"),
                Arguments.of("topic-v1alpha1.yaml"),
                Arguments.of("topic-v1beta1.yaml"),
                Arguments.of("topic-v1beta2.yaml"),
                Arguments.of("user-v1alpha1.yaml"),
                Arguments.of("user-v1beta1.yaml"),
                Arguments.of("user-v1beta2.yaml"),
                Arguments.of("user-up-to-date.yaml"),
                Arguments.of("user-empty-acls.yaml"),
                Arguments.of("user-op-conflict.yaml"),
                Arguments.of("rebalance-v1beta2.yaml"),
                Arguments.of("strimzipodset-v1beta2.yaml"),
                Arguments.of("connector-v1beta2.yaml"),
                Arguments.of("connector-up-to-date.yaml"),
                Arguments.of("connector-conflict.yaml"),
                Arguments.of("connector-unpaused.yaml"),
                Arguments.of("nodepool-v1beta2.yaml"),
                Arguments.of("nodepool-no-jbod.yaml"),
                Arguments.of("nodepool-no-overrides.yaml"),
                Arguments.of("connect-v1beta2.yaml"),
                Arguments.of("connect-up-to-date.yaml"),
                Arguments.of("mm2-v1beta2.yaml"),
                Arguments.of("mm2-up-to-date.yaml"),
                Arguments.of("mm2-pause.yaml"),
                Arguments.of("mm2-connect-config.yaml"),
                Arguments.of("mm2-many-mirrors.yaml"),
                Arguments.of("kafka-v1beta2.yaml"),
                Arguments.of("kafka-all-out.yaml"),
                Arguments.of("kafka-up-to-date.yaml")
        );
    }

    @ParameterizedTest
    @MethodSource("invalidSamples")
    public void testInvalidSamples(String inputFile, String expectedError) {
        convertSampleResource(inputFile, 1, expectedError);
    }

    private static Stream<Arguments> invalidSamples()    {
        return Stream.of(
                Arguments.of("topic-no-spec.yaml", "The .spec section is required and has to be added manually to KafkaTopic kind resources. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("bridge-no-spec.yaml", "The .spec section is required and has to be added manually to KafkaBridge kind resources. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("bridge-oauth.yaml", "The OAuth authentication is removed in the v1 API version. Use the Custom authentication instead. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("user-no-spec.yaml", "The .spec section is required and has to be added manually to KafkaUser kind resources. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("rebalance-no-spec.yaml", "The .spec section is required and has to be added manually to KafkaRebalance kind resources. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("strimzipodset-no-spec.yaml", "The .spec section is required and has to be added manually to StrimziPodSet kind resources. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("connector-no-spec.yaml", "The .spec section is required and has to be added manually to KafkaConnector kind resources. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("nodepool-no-spec.yaml", "The .spec section is required and has to be added manually to KafkaNodePool kind resources. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("connect-no-spec.yaml", "The .spec section is required and has to be added manually to KafkaConnect kind resources. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("connect-external-configuration.yaml", "The External Configuration is removed in the v1 API version. Use the .spec.template section instead. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("connect-oauth.yaml", "The OAuth authentication is removed in the v1 API version. Use the Custom authentication instead. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("mm2-no-spec.yaml", "The .spec section is required and has to be added manually to KafkaMirrorMaker2 kind resources. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("mm2-external-configuration.yaml", "The External Configuration is removed in the v1 API version. Use the .spec.template section instead. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("mm2-oauth.yaml", "The OAuth authentication is removed in the v1 API version. Use the Custom authentication instead. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("mm2-no-clusters.yaml", "KafkaMirrorMaker2 resource seems to be missing the target cluster definition in both the new and old format. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("mm2-no-connect-cluster.yaml", ".spec.connectCluster not found in .spec.clusters. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("mm2-no-source-cluster.yaml", ".spec.mirrors[].sourceCluster not found in .spec.clusters. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("mm2-target-not-connect-cluster.yaml", "KafkaMirrorMaker2 resource has different Connect and Target clusters. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("kafka-no-spec.yaml", "The .spec section is required and has to be added manually to Kafka kind resources. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("kafka-oauth-authn.yaml", "The OAuth authentication is removed in the v1 API version. Use the Custom authentication instead. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("kafka-keycloak-authz.yaml", "The Keycloak authorization is removed in the v1 API version. Use the Custom authorization instead. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("kafka-opa-authz.yaml", "The Open Policy Agent authorization is removed in the v1 API version. Use the Custom authorization instead. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("kafka-custom-authn-secrets.yaml", "Adding secrets in custom authentication is removed in the v1 API version. Use the additional volumes feature in the template section instead. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("kafka-resources.yaml", "The resources configuration in .spec.kafka.resources is removed in the v1 API version. Use the KafkaNodePool resource instead to configure resources. Please fix the resource manually and re-run the conversion tool.")
        );
    }

    private void convertSampleResource(String testFile, int expectedExitCode, String expectedError)    {
        try {
            client.load(getClass().getResourceAsStream(testFile)).create();

            CommandLine cmd = new CommandLine(new EntryCommand());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            cmd.setOut(pw);
            cmd.setErr(pw);

            int exitCode = cmd.execute("convert-resource", "--namespace", NAMESPACE);

            assertThat("Checking exit code for conversion of " + testFile + " is " + expectedExitCode, exitCode, is(expectedExitCode));

            if (expectedError != null) {
                assertThat("Checking error for conversion of " + testFile, sw.toString(), containsString(expectedError));
            }
        } finally {
            client.load(getClass().getResourceAsStream(testFile)).delete();
        }
    }
}
