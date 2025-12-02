/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.cli;

import io.strimzi.kafka.api.conversion.v1.converter.conversions.MultipartConversions;
import io.strimzi.kafka.api.conversion.v1.utils.IoUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import picocli.CommandLine;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class ConvertFileCommandTest {
    @BeforeEach
    public void beforeEach()    {
        MultipartConversions.remove();
    }

    private void compareConvertFileToStandardOutput(String inputFileName, String outputFileName) {
        CommandLine cmd = new CommandLine(new EntryCommand());
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        cmd.setOut(pw);
        cmd.setErr(pw);

        String inputFilePath = getClass().getResource(inputFileName).getPath();
        String expectedOutput = IoUtils.toString(getClass().getResourceAsStream(outputFileName));

        int exitCode = cmd.execute("convert-file", "--file", inputFilePath);

        assertThat(sw.toString().trim(), is(expectedOutput.trim()));
        assertThat(exitCode, is(0));

        pw.close();
    }

    private void checkConflictError(String inputFileName, String expectedError) {
        CommandLine cmd = new CommandLine(new EntryCommand());
        StringWriter sw = new StringWriter();
        cmd.setOut(new PrintWriter(sw));
        cmd.setErr(new PrintWriter(sw));

        String inputFilePath = getClass().getResource(inputFileName).getPath();

        int exitCode = cmd.execute("convert-file", "--file", inputFilePath);

        assertThat(exitCode, is(not(0)));
        assertThat(sw.toString(), containsString(expectedError));
    }

    private void compareConversionInPlace(String inputFileName, String outputFileName) throws IOException {
        CommandLine cmd = new CommandLine(new EntryCommand());
        StringWriter sw = new StringWriter();
        cmd.setOut(new PrintWriter(sw));

        Path tempFile = null;

        try {
            tempFile = Files.createTempFile("strimzi-api-conversion-", ".yaml");

            String source = IoUtils.toString(getClass().getResourceAsStream(inputFileName));
            String expectedOutput = IoUtils.toString(getClass().getResourceAsStream(outputFileName));

            Files.writeString(tempFile, source);

            int exitCode = cmd.execute("convert-file", "--file", tempFile.toString(), "--in-place");

            String actualOutput = Files.readString(tempFile, StandardCharsets.UTF_8);

            assertThat(exitCode, is(0));
            assertThat(actualOutput.trim(), is(expectedOutput));
        } finally {
            if (tempFile != null) {
                Files.deleteIfExists(tempFile);
            }
        }
    }

    private void compareConversionToOutputFile(String inputFileName, String outputFileName) throws IOException {
        CommandLine cmd = new CommandLine(new EntryCommand());
        StringWriter sw = new StringWriter();
        cmd.setOut(new PrintWriter(sw));

        Path tempInputFile = null;
        Path tempOutputFile = null;

        try {
            tempInputFile = Files.createTempFile("strimzi-api-conversion-", ".yaml");
            tempOutputFile = Files.createTempFile("strimzi-api-conversion-", ".out");

            String source = IoUtils.toString(getClass().getResourceAsStream(inputFileName));
            String expectedOutput = IoUtils.toString(getClass().getResourceAsStream(outputFileName));

            Files.writeString(tempInputFile, source);

            int exitCode = cmd.execute("convert-file", "--file", tempInputFile.toString(), "--output", tempOutputFile.toString());

            String actualOutput = Files.readString(tempOutputFile, StandardCharsets.UTF_8);

            assertThat(exitCode, is(0));
            assertThat(actualOutput.trim(), is(expectedOutput));
        } finally {
            if (tempInputFile != null) {
                Files.deleteIfExists(tempInputFile);
            }

            if (tempOutputFile != null) {
                Files.deleteIfExists(tempOutputFile);
            }
        }
    }

    ////////////////////
    // * Tests of stdout conversions
    ////////////////////

    @ParameterizedTest
    @MethodSource("correctSamples")
    public void testConvertCorrectSampled(String inputFile, String outputFile) {
        compareConvertFileToStandardOutput(inputFile, outputFile);
    }

    private static Stream<Arguments> correctSamples()    {
        return Stream.of(
                Arguments.of("bridge-metrics.yaml", "bridge-metrics.out"),
                Arguments.of("bridge-no-replicas.yaml", "bridge-no-replicas.out"),
                Arguments.of("bridge-jaeger-tracing.yaml", "bridge-jaeger-tracing.out"),
                Arguments.of("bridge-opentelemetry-tracing.yaml", "bridge-opentelemetry-tracing.out"),
                Arguments.of("topic-v1alpha1.yaml", "topic-v1alpha1.out"),
                Arguments.of("topic-v1beta1.yaml", "topic-v1beta1.out"),
                Arguments.of("topic-v1beta2.yaml", "topic-v1beta2.out"),
                Arguments.of("user-v1alpha1.yaml", "user-v1alpha1.out"),
                Arguments.of("user-v1beta1.yaml", "user-v1beta1.out"),
                Arguments.of("user-v1beta2.yaml", "user-v1beta2.out"),
                Arguments.of("user-up-to-date.yaml", "user-up-to-date.out"),
                Arguments.of("user-empty-acls.yaml", "user-empty-acls.out"),
                Arguments.of("user-op-conflict.yaml", "user-op-conflict.out"),
                Arguments.of("rebalance-v1beta2.yaml", "rebalance-v1beta2.out"),
                Arguments.of("strimzipodset-v1beta2.yaml", "strimzipodset-v1beta2.out"),
                Arguments.of("connector-v1beta2.yaml", "connector-v1beta2.out"),
                Arguments.of("connector-up-to-date.yaml", "connector-up-to-date.out"),
                Arguments.of("connector-conflict.yaml", "connector-conflict.out"),
                Arguments.of("connector-unpaused.yaml", "connector-unpaused.out"),
                Arguments.of("nodepool-v1beta2.yaml", "nodepool-v1beta2.out"),
                Arguments.of("nodepool-no-jbod.yaml", "nodepool-no-jbod.out"),
                Arguments.of("nodepool-no-overrides.yaml", "nodepool-no-overrides.out"),
                Arguments.of("nodepool-no-overrides-no-jbod.yaml", "nodepool-no-overrides-no-jbod.out"),
                Arguments.of("nodepool-ephemeral.yaml", "nodepool-ephemeral.out"),
                Arguments.of("connect-v1beta2.yaml", "connect-v1beta2.out"),
                Arguments.of("connect-up-to-date.yaml", "connect-up-to-date.out"),
                Arguments.of("mm2-v1beta2.yaml", "mm2-v1beta2.out"),
                Arguments.of("mm2-up-to-date.yaml", "mm2-up-to-date.out"),
                Arguments.of("mm2-pause.yaml", "mm2-pause.out"),
                Arguments.of("mm2-connect-config.yaml", "mm2-connect-config.out"),
                Arguments.of("mm2-many-mirrors.yaml", "mm2-many-mirrors.out"),
                Arguments.of("kafka-v1beta2.yaml", "kafka-v1beta2.out"),
                Arguments.of("kafka-all-out.yaml", "kafka-all-out.out"),
                Arguments.of("kafka-up-to-date.yaml", "kafka-up-to-date.out")
        );
    }

    @ParameterizedTest
    @MethodSource("invalidSamples")
    public void testInvalidSamples(String inputFile, String expectedError) {
        checkConflictError(inputFile, expectedError);
    }

    private static Stream<Arguments> invalidSamples()    {
        return Stream.of(
                Arguments.of("topic-no-spec.yaml", "io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException: The .spec section is required and has to be added manually to KafkaTopic kind resources. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("bridge-no-spec.yaml", "io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException: The .spec section is required and has to be added manually to KafkaBridge kind resources. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("bridge-oauth.yaml", "io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException: The OAuth authentication is removed in the v1 API version. Use the Custom authentication instead. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("user-no-spec.yaml", "io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException: The .spec section is required and has to be added manually to KafkaUser kind resources. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("rebalance-no-spec.yaml", "io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException: The .spec section is required and has to be added manually to KafkaRebalance kind resources. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("strimzipodset-no-spec.yaml", "io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException: The .spec section is required and has to be added manually to StrimziPodSet kind resources. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("connector-no-spec.yaml", "io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException: The .spec section is required and has to be added manually to KafkaConnector kind resources. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("nodepool-no-spec.yaml", "io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException: The .spec section is required and has to be added manually to KafkaNodePool kind resources. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("connect-no-spec.yaml", "io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException: The .spec section is required and has to be added manually to KafkaConnect kind resources. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("connect-external-configuration.yaml", "io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException: The External Configuration is removed in the v1 API version. Use the .spec.template section instead. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("connect-oauth.yaml", "io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException: The OAuth authentication is removed in the v1 API version. Use the Custom authentication instead. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("mm2-no-spec.yaml", "io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException: The .spec section is required and has to be added manually to KafkaMirrorMaker2 kind resources. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("mm2-external-configuration.yaml", "io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException: The External Configuration is removed in the v1 API version. Use the .spec.template section instead. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("mm2-oauth.yaml", "io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException: The OAuth authentication is removed in the v1 API version. Use the Custom authentication instead. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("mm2-no-clusters.yaml", "io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException: KafkaMirrorMaker2 resource seems to be missing the target cluster definition in both the new and old format. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("mm2-no-connect-cluster.yaml", "io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException: .spec.connectCluster not found in .spec.clusters. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("mm2-no-source-cluster.yaml", "io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException: .spec.mirrors[].sourceCluster not found in .spec.clusters. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("mm2-target-not-connect-cluster.yaml", "io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException: KafkaMirrorMaker2 resource has different Connect and Target clusters. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("kafka-no-spec.yaml", "io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException: The .spec section is required and has to be added manually to Kafka kind resources. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("kafka-oauth-authn.yaml", "io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException: The OAuth authentication is removed in the v1 API version. Use the Custom authentication instead. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("kafka-keycloak-authz.yaml", "io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException: The Keycloak authorization is removed in the v1 API version. Use the Custom authorization instead. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("kafka-opa-authz.yaml", "io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException: The Open Policy Agent authorization is removed in the v1 API version. Use the Custom authorization instead. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("kafka-custom-authn-secrets.yaml", "io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException: Adding secrets in custom authentication is removed in the v1 API version. Use the additional volumes feature in the template section instead. Please fix the resource manually and re-run the conversion tool."),
                Arguments.of("kafka-resources.yaml", "io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException: The resources configuration in .spec.kafka.resources is removed in the v1 API version. Use the KafkaNodePool resource instead to configure resources. Please fix the resource manually and re-run the conversion tool.")
        );
    }

    ////////////////////
    // Tests of in-place conversions
    ////////////////////

    @Test
    public void testKafkaInPlace() throws IOException {
        compareConversionInPlace("kafka-all-out.yaml", "kafka-all-out.out");
    }

    @Test
    public void testMultipleStrimziResourcesInPlace() {
        compareConvertFileToStandardOutput("multiple-strimzi-resources.yaml", "multiple-strimzi-resources.out");
    }

    @Test
    public void testMultipleDifferentResourcesInPlace() {
        compareConvertFileToStandardOutput("multiple-resources.yaml", "multiple-resources.out");
    }

    ////////////////////
    // Tests of conversions to output files
    ////////////////////

    @Test
    public void testKafkaToOutputFile() throws IOException {
        compareConversionToOutputFile("kafka-all-out.yaml", "kafka-all-out.out");
    }

    @Test
    public void testMultipleStrimziResourcesToOutputFile() {
        compareConvertFileToStandardOutput("multiple-strimzi-resources.yaml", "multiple-strimzi-resources.out");
    }

    @Test
    public void testMultipleDifferentResourcesToOutputFile() {
        compareConvertFileToStandardOutput("multiple-resources.yaml", "multiple-resources.out");
    }

    ////////////////////
    // Tests of invalid commands, options etc.
    ////////////////////

    @Test
    public void testInPlaceAndOutputArgumentsError() {
        CommandLine cmd = new CommandLine(new EntryCommand());
        StringWriter sw = new StringWriter();
        cmd.setOut(new PrintWriter(sw));
        cmd.setErr(new PrintWriter(sw));

        int exitCode = cmd.execute("convert-file", "--file", "some-file.yaml", "--in-place", "--output", "some-other-file.yaml");

        assertThat(exitCode, is(2));
        assertThat(sw.toString(), containsString("--output=<outputFile>, --in-place are mutually exclusive"));
    }

    @Test
    public void testNoFileArgumentsError() {
        CommandLine cmd = new CommandLine(new EntryCommand());
        StringWriter sw = new StringWriter();
        cmd.setOut(new PrintWriter(sw));
        cmd.setErr(new PrintWriter(sw));

        int exitCode = cmd.execute("convert-file", "--output", "some-other-file.yaml");

        assertThat(exitCode, is(2));
        assertThat(sw.toString(), containsString("Missing required option: '--file=<inputFile>'"));
    }

    @Test
    public void testFileDoesNotExistError() {
        CommandLine cmd = new CommandLine(new EntryCommand());
        StringWriter sw = new StringWriter();
        cmd.setOut(new PrintWriter(sw));
        cmd.setErr(new PrintWriter(sw));

        int exitCode = cmd.execute("convert-file", "--file", "i-definitely-do-not-exist-on-this-disk.yaml");

        assertThat(exitCode, is(1));
        assertThat(sw.toString(), containsString("java.io.FileNotFoundException: i-definitely-do-not-exist-on-this-disk.yaml"));
    }
}
