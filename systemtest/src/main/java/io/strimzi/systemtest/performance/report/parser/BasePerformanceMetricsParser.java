/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.report.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.strimzi.api.kafka.model.kafka.KafkaSpec;
import io.strimzi.systemtest.performance.PerformanceConstants;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Abstract base class for parsing performance metrics from a structured directory of metrics files.
 * This class provides the framework for specific parsers that process metrics related to various components
 * or use cases within a system. Subclasses are expected to implement abstract methods tailored to their
 * specific metrics parsing and reporting requirements.
 */
public abstract class BasePerformanceMetricsParser {

    protected static final Logger LOGGER = LogManager.getLogger(BasePerformanceMetricsParser.class);


    protected Map<String, List<ExperimentMetrics>> useCaseExperiments;

    protected String parentPath; // Base path for metrics files

    public BasePerformanceMetricsParser() {
        this.parentPath = determineBasePathBasedOnEnvironment();
        this.useCaseExperiments = new HashMap<>();
    }

    /**
     * Abstract method to parse metrics from the filesystem. This method needs to be implemented by
     * subclasses to define how metrics are extracted from files.
     *
     * @throws IOException          If an I/O error occurs reading from the file.
     */
    protected abstract void parseMetrics() throws IOException;

    /**
     * Abstract method to display parsed metrics. Subclasses should implement this method to
     * define how metrics are presented to the user.
     */
    protected abstract void showMetrics();

    /**
     * Abstract method for extracting and formatting data from metrics into a row format.
     *
     * @param experimentNumber      The experiment number.
     * @param simpleMetrics         A map of simple metrics.
     * @param experimentMetrics     The experiment metrics object containing detailed metrics information.
     * @return                      An array of strings representing the formatted row data.
     */
    protected abstract String[] extractAndFormatRowData(int experimentNumber, Map<String, String> simpleMetrics, ExperimentMetrics experimentMetrics);

    /**
     * Abstract method to get headers for a use case. This allows different parsers to specify
     * what headers are relevant to their specific metrics.
     *
     * @param useCaseName           The name of the use case.
     * @return                      An array of strings representing the headers for the use case.
     */
    protected abstract String[] getHeadersForUseCase(String useCaseName);

    /**
     * Checks if the current execution context is a test environment.
     *
     * @return True                 if running within a test environment, otherwise false.
     */
    protected boolean isRunningInTest() {
        for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
            if (element.getClassName().startsWith("org.junit.")) {
                return true;
            }
        }
        return false;
    }

    /**
     * Parses the latest metrics from the structured directory, assuming that each parser knows
     * which components to parse.
     *
     * @throws IOException          If an I/O error occurs while accessing the filesystem.
     */
    protected void parseLatestMetrics() throws IOException {
        final Path basePath = findLatestDirectory();
        if (basePath == null) {
            LOGGER.error("No directories found in the specified path.");
            return;
        }

        // Assuming each parser knows which components to parse.
        final File[] componentDirs = basePath.toFile().listFiles(File::isDirectory);
        if (componentDirs != null) {
            for (File componentDir : componentDirs) {
                parseComponentMetrics(componentDir);
            }
        }
    }

    /**
     * Attempts to find the most recently modified directory within the base path.
     * This directory is assumed to contain the latest set of metrics to be parsed.
     *
     * @return                  Path to the latest directory, or null if no directory is found.
     * @throws IOException      If an error occurs while accessing the file system.
     */
    private Path findLatestDirectory() throws IOException {
        Optional<Path> latestDirectory = Files.list(Paths.get(this.parentPath))
            .filter(Files::isDirectory)
            .max(Comparator.comparingLong(file -> file.toFile().lastModified()));
        return latestDirectory.orElse(null);
    }

    /**
     * Parses metrics for a specific component identified by a directory.
     * This method iterates through use case directories within a component directory
     * and parses metrics files found within.
     *
     * @param componentDir      The directory of the component to parse metrics from.
     * @throws IOException      If an error occurs while reading the metrics files.
     */
    protected void parseComponentMetrics(File componentDir) throws IOException {
        File[] useCaseDirs = componentDir.listFiles(File::isDirectory);
        if (useCaseDirs != null) {
            for (File useCaseDir : useCaseDirs) {
                String useCaseName = useCaseDir.getName();
                List<ExperimentMetrics> experimentsList = new ArrayList<>();
                File[] experimentsFiles = useCaseDir.listFiles(File::isDirectory);
                if (experimentsFiles != null) {
                    for (File experimentFile : experimentsFiles) {
                        ExperimentMetrics experimentMetrics = new ExperimentMetrics();
                        File[] metricFiles = experimentFile.listFiles();
                        if (metricFiles != null) {
                            for (File metricFile : metricFiles) {
                                processMetricsFile(metricFile, experimentMetrics);
                            }
                        }
                        experimentsList.add(experimentMetrics);
                    }
                }
                this.useCaseExperiments.put(useCaseName, experimentsList);
            }
        }
    }

    /**
     * Processes a metrics file and updates the provided {@link ExperimentMetrics} object
     * accordingly. The method distinguishes between two types of metrics files:
     * a specific "performance metrics" file, identified by a predefined name
     * plus ".txt" extension, and other generic metrics files.
     * <p>
     * For the performance metrics file, it directly processes the file using
     * {@code processFile}, handling any {@code IOException} that may occur.
     * For other metrics files, it parses each line to extract and accumulate
     * numerical values which are then added to the {@code ExperimentMetrics}
     * object as a simple metric.
     * <p>
     * The method expects the metric values to be listed in each line following
     * the format "Key: Value, Values: [v1, v2, v3, ...]", where only the values
     * after "Values:" are processed.
     *
     * @param metricFile                the metrics file to process. This file can either be
     *                                  a designated "performance metrics" file or a generic metrics file.
     * @param experimentMetrics         the {@link ExperimentMetrics} object to update with
     *                                  the metrics extracted from {@code metricFile}. This object
     *                                  accumulates the metrics for a single experiment.
     * @throws IOException              if an I/O error occurs while reading from the metrics file.
     *                                  Note: For "performance metrics" files, I/O exceptions are caught
     *                                  and logged, but not rethrown. For other files, any I/O exception
     *                                  is propagated upwards.
     */
    protected void processMetricsFile(final File metricFile, final ExperimentMetrics experimentMetrics) throws IOException {
        if (metricFile.getName().endsWith(PerformanceConstants.PERFORMANCE_METRICS_FILE_NAME + ".txt")) {
            try {
                processFile(metricFile, experimentMetrics);
            } catch (IOException e) {
                LOGGER.error("Error processing file " + metricFile.getAbsolutePath());
                e.printStackTrace();
            }
            // these are most metrics
        } else {
            final List<Double> values = new ArrayList<>();
            final List<String> lines = Files.readAllLines(metricFile.toPath());

            for (String line : lines) {
                final String[] parts = line.split(", Values: ");
                if (parts.length > 1 && !parts[1].equals("[]")) {
                    final String valuesPart = parts[1].replaceAll("\\[|\\]", ""); // Remove brackets
                    final String[] valueStrings = valuesPart.split(", ");
                    for (String valueString : valueStrings) {
                        try {
                            double value = Double.parseDouble(valueString);
                            values.add(value);
                        } catch (NumberFormatException e) {
                            LOGGER.error("Error parsing value: " + valueString);
                        }
                    }
                }
            }
            experimentMetrics.addSimpleMetric(metricFile.getName(), values.toString());
        }
    }

    /**
     * Processes the contents of a metrics file containing YAML formatted data.
     * This method specifically looks for KafkaSpec configuration within the file.
     *
     * @param file                  The file containing YAML data.
     * @param experimentMetrics     The {@link ExperimentMetrics} object to update with parsed data.
     * @throws IOException          If an error occurs while reading from the file.
     */
    private void processFile(File file, ExperimentMetrics experimentMetrics) throws IOException {
        StringBuilder yamlBuilder = new StringBuilder();
        boolean yamlSectionStarted = false;

        try (BufferedReader br = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("---")) {
                    yamlSectionStarted = true; // Start collecting YAML content
                } else if (!yamlSectionStarted) {
                    // Process simple key-value pairs
                    String[] parts = line.split(": ", 2);
                    if (parts.length == 2) {
                        experimentMetrics.addSimpleMetric(parts[0].trim(), parts[1].trim());
                    }
                } else {
                    // Collect YAML content
                    yamlBuilder.append(line).append("\n");
                }
            }
        }

        if (yamlBuilder.length() > 0) {
            ObjectMapper yamlMapper = new YAMLMapper();
            KafkaSpec kafkaSpec = yamlMapper.readValue(yamlBuilder.toString(), KafkaSpec.class);
            experimentMetrics.setKafkaSpec(kafkaSpec);
        }
    }

    /**
     * Displays the values of parsed experiments in a formatted table.
     * This method organizes metrics into rows and columns based on headers and formats the output.
     */
    protected void showValuesOfExperiments() {
        // Populate data for each experiment
        this.useCaseExperiments.forEach((useCaseName, experimentsList) -> {
            System.out.println("Use Case: " + useCaseName);

            final String[] headers = getHeadersForUseCase(useCaseName);
            final List<String[]> allRows = new ArrayList<>();

            allRows.add(headers);

            // Determine max width for each column
            final int[] columnWidths = new int[headers.length];
            for (final String[] row : allRows) {
                for (int i = 0; i < row.length; i++) {
                    columnWidths[i] = Math.max(columnWidths[i], row[i].length());
                }
            }

            printSeparator(columnWidths);

            int experimentCounter = 1;

            for (final ExperimentMetrics experimentMetrics : experimentsList) {
                final Map<String, String> simpleMetrics = experimentMetrics.getSimpleMetrics();
                // Assume methods to extract and format metrics correctly are implemented
                final String[] rowData = extractAndFormatRowData(experimentCounter, simpleMetrics, experimentMetrics);
                allRows.add(rowData);
                experimentCounter++;
            }

            // Print table with dynamically adjusted widths
            allRows.forEach(row -> printRow(row, columnWidths));
            printSeparator(columnWidths);
        });
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Usage: java -jar performance-metrics-parser.jar [parserType]");
            System.exit(1);
        }

        LOGGER.info("Using user.dir: {}", TestUtils.USER_PATH);

        String parserType = args[0]; // [parser type as first argument]
        BasePerformanceMetricsParser parser = null;

        switch (parserType) {
            case PerformanceConstants.TOPIC_OPERATOR_PARSER:
                parser = new TopicOperatorMetricsParser();
                break;
            // ... more parsers here
            default:
                System.err.println("Unsupported parser type: " + parserType);
                System.exit(1);
        }

        LOGGER.info("Using path: {}", parser.getParentPath());

        try {
            // parse metrics and fill inner structures
            parser.parseMetrics();
            // show metrics (i.e., normal and derived) to end user
            parser.showMetrics();
        } catch (IOException e) {
            System.err.println("Error parsing metrics: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Determines the base path for metrics files based on the current environment.
     * Adjusts the path based on whether the execution is happening in a test environment
     * or a standard runtime environment.
     *
     * @return The determined base path as a String.
     */
    private String determineBasePathBasedOnEnvironment() {
        // resolve path for TestingFarm
        if (System.getenv().containsKey("TMT_PLAN_DATA")) {
            this.parentPath = System.getenv().get("TMT_PLAN_DATA") + "/../discover/default-0/tests/systemtest/target/performance";
            // If running in test, adjust the path accordingly
        } else if (isRunningInTest()) {
            this.parentPath = TestUtils.USER_PATH + "/target/performance";
        } else {
            // For standalone application run
            this.parentPath = TestUtils.USER_PATH + "/systemtest/target/performance";
        }
        return this.parentPath;
    }

    /**
     * Parses a string representing a list of doubles and returns the maximum value found.
     * The string should be formatted as "[value1, value2, ...]".
     *
     * @param listAsString          The string representation of the list of doubles.
     * @return                      The maximum double value in the list, or 0 if the list is empty or parsing fails.
     */
    protected double getMaxValueFromList(String listAsString) {
        // Assuming the list is formatted as "[value1, value2, ...]"
        String[] items = listAsString.substring(1, listAsString.length() - 1).split(", ");
        return Arrays.stream(items)
            .mapToDouble(Double::parseDouble)
            .max()
            .orElse(0); // Default to 0 if list is empty or parsing fails
    }

    /**
     * Appends the .txt file extension to a metric name.
     * @param metricName The name of the metric without the file extension.
     * @return The metric name with the .txt file extension.
     */
    protected String getMetricFileName(String metricName) {
        return metricName + ".txt";
    }

    private void printRow(String[] row, int[] columnWidths) {
        StringBuilder sb = new StringBuilder("|");
        for (int i = 0; i < row.length; i++) {
            sb.append(String.format(" %-" + columnWidths[i] + "s |", row[i]));
        }
        System.out.println(sb);
    }

    private void printSeparator(int[] columnWidths) {
        StringBuilder sb = new StringBuilder("+");
        for (int width : columnWidths) {
            sb.append("-".repeat(width + 2)).append("+"); // +2 for the padding on either side of the value
        }
        System.out.println(sb);
    }

    public String getParentPath() {
        return parentPath;
    }
}
