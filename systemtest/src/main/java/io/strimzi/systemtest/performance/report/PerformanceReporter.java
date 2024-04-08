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

    /**
     * Logs performance metrics for a given test case.
     * <p>
     * This method processes and logs detailed performance metrics including operation durations,
     * Kafka configurations, and custom metrics. The performance data is logged to a file specified by
     * the use case and additional configuration parameters. This method handles serialization of
     * complex data types and organizes logs for easier analysis.
     *
     * @param performanceAttributes     A map containing the performance metrics and attributes to log.
     * @param useCase                   The name of the test use case (scenario) being logged.
     * @param testStorage               An object containing details about the test storage, including namespace and cluster name.
     * @param date                      The date and time of the test execution, used in naming the log file.
     * @param baseDir                   The base directory where the log file will be saved.
     * @throws IOException              If an I/O error occurs during writing to the log file.
     */
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

        final boolean clientsEnabled = !performanceAttributes.get(PerformanceConstants.NUMBER_OF_CLIENT_INSTANCES).equals(0);

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

    /**
     * Prepares the log file path for the performance metrics based on the test execution details.
     * <p>
     * This method generates a directory structure based on the date, use case, and configuration parameters
     * such as max batch size and linger time. It ensures that the directories exist before returning the path
     * to the log file within this structure.
     *
     * @param date              The date and time of the test execution.
     * @param baseDir           The base directory for the logs.
     * @param useCaseName       The name of the use case being logged.
     * @param maxBatchSize      The max batch size configuration for the test.
     * @param maxBatchLingerMs  The max linger time configuration for the test.
     * @param clientsEnabled    A flag indicating whether client instances were used in the test.
     * @return                  The path to the log file where performance metrics will be saved.
     * @throws IOException      If an I/O error occurs during the directory creation process.
     */
    public static Path prepareLogFile(TemporalAccessor date, String baseDir, String useCaseName, String maxBatchSize, String maxBatchLingerMs, boolean clientsEnabled) throws IOException {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");
        dateTimeFormatter = dateTimeFormatter.withZone(ZoneId.systemDefault());
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

        return useCasePathDir.resolve(PerformanceConstants.PERFORMANCE_METRICS_FILE_NAME + ".txt");
    }

    /**
     * Writes the given performance data to a file at the specified path.
     * <p>
     * This method saves the performance metrics data to a file, encoding the text using UTF-8. It logs
     * an informational message upon successful writing.
     *
     * @param performanceData   The performance data to be written to the file.
     * @param filePath          The path to the file where the data will be saved.
     * @throws IOException      If an I/O error occurs during the writing process.
     */
    public static void writePerformanceMetricsToFile(String performanceData, Path filePath) throws IOException {
        Files.write(filePath, performanceData.getBytes(StandardCharsets.UTF_8));
        LOGGER.info("Test performance data written to file: {}", filePath);
    }

    /**
     * Serializes the Kafka configuration for the specified cluster and namespace.
     * <p>
     * This method retrieves the Kafka custom resource for the given cluster and namespace, then
     * serializes its configuration to a YAML string. It handles exceptions by logging errors and
     * returning a null or error message.
     *
     * @param namespace         The namespace of the Kafka cluster.
     * @param clusterName       The name of the Kafka cluster.
     * @return                  A string containing the serialized YAML representation of the Kafka configuration, or an error message if the configuration cannot be serialized.
     */
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

    /**
     * Serializes the history of metrics for the test execution and writes it to files.
     * <p>
     * This method processes a map containing metrics history, organizing the data by timestamp and metric name.
     * For each metric, it generates a file named after the metric and appends the serialized data. The method
     * ensures that data for each metric is saved in a coherent and readable format.
     *
     * @param metricsHistory        A map containing the history of metrics collected during the test.
     * @param date                  The date and time of the test execution.
     * @param useCase               The name of the use case being analyzed.
     * @param baseDir               The base directory for storing metrics history files.
     * @param maxBatchSize          The max batch size configuration for the test.
     * @param maxBatchLingerMs      The max linger time configuration for the test.
     * @param clientsEnabled        A flag indicating whether client instances were used in the test.
     * @throws IOException          If an I/O error occurs during the file writing process.
     */
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

    /**
     * Prepares a directory for storing use case-specific logs and metrics.
     * <p>
     * This method leverages the logic in `prepareLogFile` to create a directory structure tailored
     * for the given use case and its configurations. It returns the path to the directory intended
     * for storing related logs and metrics files.
     *
     * @param date              The date and time of the test execution.
     * @param baseDir           The base directory for logs and metrics.
     * @param useCase           The name of the use case.
     * @param maxBatchSize      The max batch size used in the test.
     * @param maxBatchLingerMs  The max linger time used in the test.
     * @param clientsEnabled    Indicates if client instances were enabled during the test.
     * @return                  The path to the directory where use case-specific files should be stored.
     * @throws IOException      If an I/O error occurs during the directory preparation process.
     */
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
