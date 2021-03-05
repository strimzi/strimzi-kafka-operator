/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.cli;

import io.strimzi.kafka.api.conversion.converter.MultipartConversions;
import io.strimzi.kafka.api.conversion.utils.IoUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

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
        String expectedOutput = IoUtil.toString(getClass().getResourceAsStream(outputFileName));

        int exitCode = cmd.execute("convert-file", "--file", inputFilePath);

        assertThat(exitCode, is(0));
        assertThat(sw.toString().trim(), is(expectedOutput.trim()));

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

            String source = IoUtil.toString(getClass().getResourceAsStream(inputFileName));
            String expectedOutput = IoUtil.toString(getClass().getResourceAsStream(outputFileName));

            Files.write(tempFile, source.getBytes(StandardCharsets.UTF_8));

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

            String source = IoUtil.toString(getClass().getResourceAsStream(inputFileName));
            String expectedOutput = IoUtil.toString(getClass().getResourceAsStream(outputFileName));

            Files.write(tempInputFile, source.getBytes(StandardCharsets.UTF_8));

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

    /**************************************************
     * Tests of stdout conversions
     **************************************************/

    @Test
    public void testMultipleStrimziResources() {
        compareConvertFileToStandardOutput("multiple-strimzi-resources.yaml", "multiple-strimzi-resources.out");
    }

    @Test
    public void testMultipleDifferentResources() {
        compareConvertFileToStandardOutput("multiple-resources.yaml", "multiple-resources.out");
    }

    @Test
    public void testKafka() {
        compareConvertFileToStandardOutput("kafka-complex.yaml", "kafka-complex.out");
    }

    @Test
    public void testKafkaV1Alpha1() {
        compareConvertFileToStandardOutput("kafka-v1alpha1.yaml", "kafka-v1alpha1.out");
    }

    @Test
    public void testKafkaV1Beta1() {
        compareConvertFileToStandardOutput("kafka-v1beta1.yaml", "kafka-v1beta1.out");
    }

    @Test
    public void testKafkaV1Beta2() {
        compareConvertFileToStandardOutput("kafka-v1beta2.yaml", "kafka-v1beta2.out");
    }

    @Test
    public void testKafkaTlsSidecar() {
        compareConvertFileToStandardOutput("kafka-tls-sidecar.yaml", "kafka-tls-sidecar.out");
    }

    @Test
    public void testKafkaAffinityTolerations() {
        compareConvertFileToStandardOutput("kafka-affinity-tolerations.yaml", "kafka-affinity-tolerations.out");
    }

    @Test
    public void testKafkaMetrics() {
        compareConvertFileToStandardOutput("kafka-metrics.yaml", "kafka-metrics.out");
    }

    @Test
    public void testKafkaLogging() {
        compareConvertFileToStandardOutput("kafka-logging.yaml", "kafka-logging.out");
    }

    @Test
    public void testKafkaListeners() {
        compareConvertFileToStandardOutput("kafka-listeners.yaml", "kafka-listeners.out");
    }

    @Test
    public void testKafkaTopicOperatorSimple() {
        compareConvertFileToStandardOutput("kafka-tosimple.yaml", "kafka-tosimple.out");
    }

    @Test
    public void testKafkaTopicOperatorFull() {
        compareConvertFileToStandardOutput("kafka-tofull.yaml", "kafka-tofull.out");
    }

    @Test
    public void testKafkaDifferentSourceRanges() {
        checkConflictError("kafka-different-ranges.yaml", "KafkaClusterSpec's ExternalBootstrapService and PerPodService (fields externalTrafficPolicy and/or loadBalancerSourceRanges) are not equal and cannot be converted automatically! Please resolve the issue manually and run the API conversion tool again.");
    }

    @Test
    public void testKafkaDifferentExternalTrafficPolicies() {
        checkConflictError("kafka-different-policies.yaml", "KafkaClusterSpec's ExternalBootstrapService and PerPodService (fields externalTrafficPolicy and/or loadBalancerSourceRanges) are not equal and cannot be converted automatically! Please resolve the issue manually and run the API conversion tool again.");
    }

    @Test
    public void testKafkaTopicOperatorTwice() {
        checkConflictError("kafka-to-twice.yaml", "Cannot move /spec/topicOperator to /spec/entityOperator/topicOperator. The target path already exists. Please resolve the issue manually and run the API conversion tool again.");
    }

    @Test
    public void testKafkaTopicOperatorTwoTlsSidecars() {
        checkConflictError("kafka-to-two-tls-sidecars.yaml", "Cannot move /spec/topicOperator/tlsSidecar to /spec/entityOperator/tlsSidecar. The target path already exists. Please resolve the issue manually and run the API conversion tool again");
    }

    @Test
    public void testKafkaTolerationConflict() {
        checkConflictError("kafka-toleration-conflict.yaml", "Cannot move /spec/kafka/tolerations to /spec/kafka/template/pod/tolerations. The target path already exists. Please resolve the issue manually and run the API conversion tool again.");
    }

    @Test
    public void testBridgeV1Alpha1() {
        compareConvertFileToStandardOutput("bridge-v1alpha1.yaml", "bridge-v1alpha1.out");
    }

    @Test
    public void testBridgeLogging() {
        compareConvertFileToStandardOutput("bridge-logging.yaml", "bridge-logging.out");
    }

    @Test
    public void testMirrorMakerV1Alpha1() {
        compareConvertFileToStandardOutput("mm-v1alpha1.yaml", "mm-v1alpha1.out");
    }

    @Test
    public void testMirrorMakerV1Beta1() {
        compareConvertFileToStandardOutput("mm-v1beta1.yaml", "mm-v1beta1.out");
    }

    @Test
    public void testMirrorMakerAffinityTolerations() {
        compareConvertFileToStandardOutput("mm-affinity-tolerations.yaml", "mm-affinity-tolerations.out");
    }

    @Test
    public void testMirrorMakerLogging() {
        compareConvertFileToStandardOutput("mm-logging.yaml", "mm-logging.out");
    }

    @Test
    public void testMirrorMakerMetrics() {
        compareConvertFileToStandardOutput("mm-metrics.yaml", "mm-metrics.out");
    }

    @Test
    public void testMirrorMaker2V1Alpha1() {
        compareConvertFileToStandardOutput("mm2-v1alpha1.yaml", "mm2-v1alpha1.out");
    }

    @Test
    public void testMirrorMaker2V1AffinityTolerations() {
        compareConvertFileToStandardOutput("mm2-affinity-tolerations.yaml", "mm2-affinity-tolerations.out");
    }

    @Test
    public void testMirrorMaker2Logging() {
        compareConvertFileToStandardOutput("mm2-logging.yaml", "mm2-logging.out");
    }

    @Test
    public void testMirrorMaker2Metrics() {
        compareConvertFileToStandardOutput("mm2-metrics.yaml", "mm2-metrics.out");
    }

    @Test
    public void testConnectV1Alpha1() {
        compareConvertFileToStandardOutput("connect-v1alpha1.yaml", "connect-v1alpha1.out");
    }

    @Test
    public void testConnectV1Beta1() {
        compareConvertFileToStandardOutput("connect-v1beta1.yaml", "connect-v1beta1.out");
    }

    @Test
    public void testConnectAffinityTolerations() {
        compareConvertFileToStandardOutput("connect-affinity-tolerations.yaml", "connect-affinity-tolerations.out");
    }

    @Test
    public void testConnectLogging() {
        compareConvertFileToStandardOutput("connect-logging.yaml", "connect-logging.out");
    }

    @Test
    public void testConnectMetrics() {
        compareConvertFileToStandardOutput("connect-metrics.yaml", "connect-metrics.out");
    }

    @Test
    public void testConnectS2IV1Alpha1() {
        compareConvertFileToStandardOutput("connect-s2i-v1alpha1.yaml", "connect-s2i-v1alpha1.out");
    }

    @Test
    public void testConnectS2IV1Beta1() {
        compareConvertFileToStandardOutput("connect-s2i-v1beta1.yaml", "connect-s2i-v1beta1.out");
    }

    @Test
    public void testConnectS2IAffinityTolerations() {
        compareConvertFileToStandardOutput("connect-s2i-affinity-tolerations.yaml", "connect-s2i-affinity-tolerations.out");
    }

    @Test
    public void testConnectS2ILogging() {
        compareConvertFileToStandardOutput("connect-s2i-logging.yaml", "connect-s2i-logging.out");
    }

    @Test
    public void testConnectS2IMetrics() {
        compareConvertFileToStandardOutput("connect-s2i-metrics.yaml", "connect-s2i-metrics.out");
    }

    @Test
    public void testTopicV1Alpha1() {
        compareConvertFileToStandardOutput("topic-v1alpha1.yaml", "topic-v1alpha1.out");
    }

    @Test
    public void testTopicV1Beta1() {
        compareConvertFileToStandardOutput("topic-v1beta1.yaml", "topic-v1beta1.out");
    }

    @Test
    public void testUserV1Alpha1() {
        compareConvertFileToStandardOutput("user-v1alpha1.yaml", "user-v1alpha1.out");
    }

    @Test
    public void testUserV1Beta1() {
        compareConvertFileToStandardOutput("user-v1beta1.yaml", "user-v1beta1.out");
    }

    @Test
    public void testRebalanceV1Alpha1() {
        compareConvertFileToStandardOutput("rebalance-v1alpha1.yaml", "rebalance-v1alpha1.out");
    }

    @Test
    public void testConnectorV1Alpha1() {
        compareConvertFileToStandardOutput("connector-v1alpha1.yaml", "connector-v1alpha1.out");
    }

    /**************************************************
     * Tests of in-place conversions
     **************************************************/

    @Test
    public void testKafkaInPlace() throws IOException {
        compareConversionInPlace("kafka-complex.yaml", "kafka-complex.out");
    }

    @Test
    public void testMultipleStrimziResourcesInPlace() {
        compareConvertFileToStandardOutput("multiple-strimzi-resources.yaml", "multiple-strimzi-resources.out");
    }

    @Test
    public void testMultipleDifferentResourcesInPlace() {
        compareConvertFileToStandardOutput("multiple-resources.yaml", "multiple-resources.out");
    }

    /**************************************************
     * Tests of conversions to output files
     **************************************************/

    @Test
    public void testKafkaToOutputFile() throws IOException {
        compareConversionToOutputFile("kafka-complex.yaml", "kafka-complex.out");
    }

    @Test
    public void testMultipleStrimziResourcesToOutputFile() {
        compareConvertFileToStandardOutput("multiple-strimzi-resources.yaml", "multiple-strimzi-resources.out");
    }

    @Test
    public void testMultipleDifferentResourcesToOutputFile() {
        compareConvertFileToStandardOutput("multiple-resources.yaml", "multiple-resources.out");
    }

    /**************************************************
     * Tests of invalid commands, options etc.
     **************************************************/

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
