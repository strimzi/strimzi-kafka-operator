/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.report;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.systemtest.performance.PerformanceConstants;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Map;

/**
 * Reports performance metrics from tests related to Strimzi Kafka operators.
 * <p>
 * This class provides functionality to log performance data gathered during performance tests,
 * including operation durations, Kafka configuration, and custom performance metrics defined
 * in {@link PerformanceConstants}. The performance data is logged to files organized by test
 * use case and configuration parameters such as max batch size and max linger time.
 * <p>
 * Key Features:
 * <ul>
 *     <li>Logs detailed performance metrics and Kafka configurations in a structured format.</li>
 *     <li>Supports logging of complex data types, including long arrays and maps, with special
 *     handling for serialization.</li>
 *     <li>Organizes logs by test use case and configuration parameters, facilitating easier
 *     analysis of performance under different conditions.</li>
 * </ul>
 */
public class PerformanceReporter {
    private static final Logger LOGGER = LogManager.getLogger(PerformanceReporter.class);

    public static void logPerformanceData(
            Map<String, Object> performanceAttributes,
            String useCase,
            TestStorage testStorage,
            TemporalAccessor date,
            String baseDir
    ) throws IOException {
        // Dynamically build the performance data string from the map
        StringBuilder testPerformanceDataBuilder = new StringBuilder();
        performanceAttributes.forEach((key, value) -> {
            // Special handling for complex objects like long arrays or maps
            if (value instanceof long[] times) {
                for (int i = 0; i < times.length; i++) {
                    testPerformanceDataBuilder.append(String.format("%s Round %d: %d ms%n", key, i + 1, times[i]));
                }
            } else if (value instanceof Map) {
                // This placeholder assumes you'll implement or call a method to nicely serialize or format this map
                testPerformanceDataBuilder.append(String.format("%s: [Complex Data]%n", key));
            } else {
                testPerformanceDataBuilder.append(String.format("%s: %s%n", key, value.toString()));
            }
        });

        // Serialize Kafka configuration and add to performance data
        final String kafkaConfig = serializeKafkaConfiguration(testStorage.getNamespaceName(), testStorage.getClusterName());
        testPerformanceDataBuilder.append(String.format("Kafka Configuration:%n%s", kafkaConfig));

        final String testPerformanceData = testPerformanceDataBuilder.toString();

        // Retrieve necessary data for logging file path
        final String maxBatchSize = performanceAttributes.getOrDefault(PerformanceConstants.MAX_BATCH_SIZE, "").toString();
        final String maxBatchLingerMs = performanceAttributes.getOrDefault(PerformanceConstants.MAX_BATCH_LINGER_MS, "").toString();

        final boolean clientsEnabled = !performanceAttributes.get(PerformanceConstants.NUMBER_OF_CLIENT_INSTANCES).equals("0");

        // Prepare log file path
        Path performanceLogFile = prepareLogFile(date, baseDir, useCase, maxBatchSize, maxBatchLingerMs, clientsEnabled);

        // Write performance data to file
        writePerformanceMetricsToFile(testPerformanceData, performanceLogFile);

        // Assuming serializeMetricsHistory handles serialization of the metrics history within the map
        Map<Long, Map<String, List<Double>>> metricsHistory = (Map<Long, Map<String, List<Double>>>) performanceAttributes.get(PerformanceConstants.METRICS_HISTORY);
        if (metricsHistory != null) {
            serializeMetricsHistory(metricsHistory, date, useCase, baseDir, maxBatchSize, maxBatchLingerMs, clientsEnabled);
        }
    }

    public static Path prepareLogFile(TemporalAccessor date, String baseDir, String useCaseName, String maxBatchSize, String maxBatchLingerMs, boolean clientsEnabled) throws IOException {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");
        dateTimeFormatter = dateTimeFormatter.withZone(ZoneId.of("GMT"));
        String currentDate = dateTimeFormatter.format(date);

        Path logDirPath = Path.of(baseDir).resolve(currentDate);

        if (Files.notExists(logDirPath)) {
            Files.createDirectories(logDirPath);
        }

        // Use the useCaseName to create a directory specific to the current test case (Alice or Bob)
        Path useCasePathDir = logDirPath.resolve(useCaseName + "/max-batch-size-" + maxBatchSize + "-max-linger-time-" + maxBatchLingerMs + "with-clients-" + clientsEnabled);

        if (Files.notExists(useCasePathDir)) {
            Files.createDirectories(useCasePathDir);
        }

        return useCasePathDir.resolve("test-performance-metrics.txt");
    }

    public static void writePerformanceMetricsToFile(String performanceData, Path filePath) throws IOException {
        Files.write(filePath, performanceData.getBytes(StandardCharsets.UTF_8));
        LOGGER.info("Test performance data written to file: {}", filePath);
    }

    public static String serializeKafkaConfiguration(String namespace, String clusterName) {
        Kafka kafkaResource = KafkaResource.kafkaClient().inNamespace(namespace).withName(clusterName).get();

        // Check if the Kafka resource has deployed
        if (kafkaResource != null && kafkaResource.getSpec() != null) {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory()); // for JSON, just use ObjectMapper()
            try {
                // Serialize the Kafka spec part of the Kafka CR to YAML
                return mapper.writeValueAsString(kafkaResource.getSpec());
            } catch (Exception e) {
                LOGGER.error("Failed to serialize Kafka Spec configuration", e);
                return null;
            }
        } else {
            return "Kafka Spec not deployed or Kafka resource not found.";
        }
    }

    public static void serializeMetricsHistory(Map<Long, Map<String, List<Double>>> metricsHistory,
                                               TemporalAccessor date, String useCase, String baseDir,
                                               String maxBatchSize, String maxBatchLingerMs, boolean clientsEnabled) throws IOException {
        final Path alicePathDir = prepareUseCaseDirectory(date, baseDir, useCase, maxBatchSize, maxBatchLingerMs, clientsEnabled);
        final String newLine = System.lineSeparator();

        metricsHistory.forEach((time, metrics) -> {
            // Including milliseconds in the time format
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss-SSS");
            dateTimeFormatter = dateTimeFormatter.withZone(ZoneId.systemDefault());
            final String formattedTime = dateTimeFormatter.format(Instant.ofEpochMilli(time));

            metrics.forEach((metricName, values) -> {
                String content = String.format("Timestamp: %s, Values: %s%s", formattedTime, values, newLine);

                // Using just the metric name for the file name
                String fileName = metricName + ".txt";
                Path metricFilePath = alicePathDir.resolve(fileName);

                // Appending to the file if it exists, or creating a new one if it doesn't
                try (BufferedWriter writer = Files.newBufferedWriter(metricFilePath,
                        StandardCharsets.UTF_8,
                        Files.exists(metricFilePath) ? StandardOpenOption.APPEND : StandardOpenOption.CREATE)) {
                    writer.write(content);
                } catch (IOException e) {
                    LOGGER.error("Failed to write metrics to file", e);
                }
            });
        });
    }

    private static Path prepareUseCaseDirectory(TemporalAccessor date, String baseDir, String useCase,
                                              String maxBatchSize, String maxBatchLingerMs, boolean clientsEnabled) throws IOException {
        // Reusing the existing 'prepareLogFile' method for directory preparation logic
        final Path logFilePath = prepareLogFile(date, baseDir, useCase, maxBatchSize, maxBatchLingerMs, clientsEnabled);
        final Path useCasePathDir = logFilePath.getParent();

        // Check if the parent directory path is not null
        if (useCasePathDir == null) {
            throw new IOException("Unable to determine the parent directory for the log file path: " + logFilePath);
        }

        if (Files.notExists(useCasePathDir)) {
            Files.createDirectories(useCasePathDir);
        }

        return useCasePathDir;
    }
}
