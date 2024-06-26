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
 * Provides a foundational structure for logging performance data across various Strimzi components.
 * <p>
 * This abstract class outlines the procedure for collecting, organizing, and writing performance metrics
 * to files. It is designed to be extended by specific component reporters (e.g., TopicOperatorPerformanceReporter)
 * which can provide detailed implementations for component-specific configurations and logging needs.
 */
public abstract class BasePerformanceReporter {

    private static final Logger LOGGER = LogManager.getLogger(BasePerformanceReporter.class);

    /**
     * Resolves the directory path for logging use case-specific performance data.
     * <p>
     * Implementations of this method should determine the structure and naming convention of the
     * directory based on the component's specific requirements and the provided attributes.
     *
     * @param performanceLogDir             The base directory for performance logs.
     * @param useCaseName                   The name of the testing use case.
     * @param performanceAttributes         A map of performance attributes relevant to the specific use case.
     * @return                              The resolved directory path for the use case-specific performance data.
     */
    protected abstract Path resolveComponentUseCasePathDir(Path performanceLogDir, String useCaseName, Map<String, Object> performanceAttributes);

    /**
     * Logs performance metrics for a given test case.
     * <p>
     * This method processes and logs detailed performance metrics including operation durations,
     * Kafka configurations, and custom metrics. The performance data is logged to a file specified by
     * the use case and additional configuration parameters. This method handles serialization of
     * complex data types and organizes logs for easier analysis.
     *
     * @param testStorage               An object containing details about the test storage, including namespace and cluster name.
     * @param performanceAttributes     A map containing the performance metrics and attributes to log.
     * @param useCase                   The name of the test use case (scenario) being logged.
     * @param date                      The date and time of the test execution, used in naming the log file.
     * @param baseDir                   The base directory where the log file will be saved.
     * @throws IOException              If an I/O error occurs during writing to the log file.
     */
    public void logPerformanceData(
        TestStorage testStorage,
        Map<String, Object> performanceAttributes,
        String useCase,
        TemporalAccessor date,
        String baseDir
    ) throws IOException {
        // Dynamically build the performance data string from the map
        final StringBuilder testPerformanceDataBuilder = new StringBuilder();
        performanceAttributes.forEach((key, value) -> {
            // Special handling for complex objects like long arrays or maps
            if (value instanceof long[] times) {
                for (int i = 0; i < times.length; i++) {
                    testPerformanceDataBuilder.append(String.format("%s Round %d= %d ms%n", key, i + 1, times[i]));
                }
            } else if (value instanceof Map) {
                // This placeholder assumes you'll implement or call a method to nicely serialize or format this map
                // skip...
            } else {
                testPerformanceDataBuilder.append(String.format("%s= %s%n", key, value.toString()));
            }
        });

        // Serialize Kafka configuration and add to performance data
        final String kafkaConfig = serializeKafkaConfiguration(testStorage.getNamespaceName(), testStorage.getClusterName());
        testPerformanceDataBuilder.append(String.format(PerformanceConstants.KAFKA_IN_CONFIGURATION + "=%n%s", kafkaConfig));

        final String testPerformanceData = testPerformanceDataBuilder.toString();

        // Prepare log file path
        Path performanceLogFilePath = prepareLogFile(date, baseDir, useCase, performanceAttributes);

        // Write performance data to file
        writePerformanceMetricsToFile(testPerformanceData, performanceLogFilePath);

        // Assuming serializeMetricsHistory handles serialization of the metrics history within the map
        @SuppressWarnings("unchecked")
        Map<Long, Map<String, List<Double>>> metricsHistory = (Map<Long, Map<String, List<Double>>>) performanceAttributes.get(PerformanceConstants.METRICS_HISTORY);
        if (metricsHistory != null) {
            serializeMetricsHistory(metricsHistory, performanceLogFilePath);
        }
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
    private void writePerformanceMetricsToFile(String performanceData, Path filePath) throws IOException {
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
    private String serializeKafkaConfiguration(String namespace, String clusterName) {
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
     * Prepares the log file path for the performance metrics based on the test execution details.
     * <p>
     * This method generates a directory structure based on the date, use case, and configuration parameters
     * such as max batch size and linger time. It ensures that the directories exist before returning the path
     * to the log file within this structure.
     *
     * @param date                  The date and time of the test execution.
     * @param baseDir               The base directory for the logs.
     * @param useCaseName           The name of the use case being logged.
     * @param performanceAttributes The map of all performance attributes in specific test scenario
     * @return                      The path to the log file where performance metrics will be saved.
     * @throws IOException          If an I/O error occurs during the directory creation process.
     */
    private Path prepareLogFile(TemporalAccessor date, String baseDir, String useCaseName, Map<String, Object> performanceAttributes) throws IOException {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");
        dateTimeFormatter = dateTimeFormatter.withZone(ZoneId.systemDefault());
        final String currentDate = dateTimeFormatter.format(date);

        final Path logDirPath = Path.of(baseDir).resolve(currentDate);

        if (Files.notExists(logDirPath)) {
            Files.createDirectories(logDirPath);
        }

        final Path componentUseCasePathDir = this.resolveComponentUseCasePathDir(logDirPath, useCaseName, performanceAttributes);

        if (Files.notExists(componentUseCasePathDir)) {
            Files.createDirectories(componentUseCasePathDir);
        }

        return componentUseCasePathDir.resolve(PerformanceConstants.PERFORMANCE_METRICS_FILE_NAME + ".txt");
    }

    /**
     * Prepares a directory for storing use case-specific logs and metrics.
     * <p>
     * This method leverages the logic in `prepareLogFile` to create a directory structure tailored
     * for the given use case and its configurations. It returns the path to the directory intended
     * for storing related logs and metrics files.
     *
     * @return                  The path to the directory where use case-specific files should be stored.
     * @throws IOException      If an I/O error occurs during the directory preparation process.
     */
    private Path prepareUseCaseDirectory(Path performanceLogPath) throws IOException {
        final Path useCasePathDir = performanceLogPath.getParent();

        // Check if the parent directory path is not null
        if (useCasePathDir == null) {
            throw new IOException("Unable to determine the parent directory for the log file path: " + performanceLogPath);
        }

        if (Files.notExists(useCasePathDir)) {
            Files.createDirectories(useCasePathDir);
        }

        return useCasePathDir;
    }

    /**
     * Serializes the history of metrics for the test execution and writes it to files.
     * <p>
     * This method processes a map containing metrics history, organizing the data by timestamp and metric name.
     * For each metric, it generates a file named after the metric and appends the serialized data. The method
     * ensures that data for each metric is saved in a coherent and readable format.
     *
     * @param metricsHistory        A map containing the history of metrics collected during the test.
     * @param performanceLogPath    Path of performance logs, typically in /target/performance
     * @throws IOException          If an I/O error occurs during the file writing process.
     */
    private void serializeMetricsHistory(Map<Long, Map<String, List<Double>>> metricsHistory, Path performanceLogPath) throws IOException {
        final Path useCasePathDir = prepareUseCaseDirectory(performanceLogPath);
        final String newLine = System.lineSeparator();

        metricsHistory.forEach((time, metrics) -> {
            // Including milliseconds in the time format
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss-SSS");
            dateTimeFormatter = dateTimeFormatter.withZone(ZoneId.systemDefault());
            final String formattedTime = dateTimeFormatter.format(Instant.ofEpochMilli(time));

            metrics.forEach((metricName, values) -> {
                String content = String.format("Timestamp= %s, Values= %s%s", formattedTime, values, newLine);

                // Using just the metric name for the file name
                String fileName = metricName + ".txt";
                Path metricFilePath = useCasePathDir.resolve(fileName);

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
}
