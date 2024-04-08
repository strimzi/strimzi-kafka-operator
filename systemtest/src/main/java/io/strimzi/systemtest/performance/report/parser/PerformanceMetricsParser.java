/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.report.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.strimzi.api.kafka.model.common.ContainerEnvVar;
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

// TODO: Each component should have it's own parser I would say (different metrics)
// TODO: We should have some base/common parser on top of the hierarchy to share common performance metrics
/**
 * Parses and displays performance metrics from experiment results in {@code TestUtils.USER_PATH + "/target/performance}
 * directory
 * <p>
 * This class is designed to read performance metrics stored in files, which are generated
 * from performance tests. It categorizes metrics by use case and experiment, processes them to
 * calculate various statistical measures, and outputs formatted results. The parser can
 * differentiate between running in a test environment and a standalone application mode,
 * adjusting file paths accordingly. Metrics include operation durations, system and JVM
 * performance, and custom metrics defined in {@code PerformanceConstants}.
 * <p>
 * Usage:
 * <ul>
 *     <li>To run as a standalone application, provide the directory path containing
 *     performance metrics files as a command-line argument. If no directory is specified,
 *     the parser attempts to find the latest directory under the default path.</li>
 *     <li>In a test environment, ensure the {@code sun.java.command} system property
 *     contains "JUnitStarter" to adjust the base path for metrics files.</li>
 * </ul>
 *
 * Key Methods:
 * <ul>
 *     <li>{@link #isRunningInTest()} - Checks if the application is running in a test environment.</li>
 *     <li>{@link #main(String[])} - Entry point for parsing and displaying performance metrics.</li>
 *     <li>{@link #processFile(File, ExperimentMetrics)} - Processes individual metrics files to extract metrics.</li>
 *     <li>{@link #showValuesOfExperiments()} - Displays formatted metrics for all experiments and use cases.</li>
 * </ul>
 */
public class PerformanceMetricsParser {

    private static final Map<String, List<ExperimentMetrics>> USE_CASE_EXPERIMENTS = new HashMap<>();
    private static final Logger LOGGER = LogManager.getLogger(PerformanceMetricsParser.class);

    public static boolean isRunningInTest() {
        for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
            if (element.getClassName().startsWith("org.junit.")) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity"})
    public static void main(String[] args) throws IOException {

        LOGGER.info("Using user.dir: {}", TestUtils.USER_PATH);

        String parentPath;
        // resolve path for TestingFarm
        if (System.getenv().containsKey("TMT_PLAN_DATA")) {
            parentPath = System.getenv().get("TMT_PLAN_DATA") + "/../discover/default-0/tests/systemtest/target/performance";
        // If running in test, adjust the path accordingly
        } else if (isRunningInTest()) {
            parentPath = TestUtils.USER_PATH + "/target/performance";
        } else {
            // For standalone application run
            parentPath = TestUtils.USER_PATH + "/systemtest/target/performance";
        }

        LOGGER.info("Using path: {}", parentPath);

        String specificDirectory = args != null && args.length > 0 ? args[0] : null; // Check for command-line argument

        Path basePath;

        if (specificDirectory != null && !specificDirectory.isEmpty()) {
            // Use the specific directory if provided
            basePath = Paths.get(parentPath, specificDirectory);
            if (!Files.exists(basePath) || !Files.isDirectory(basePath)) {
                System.err.println("Specified directory does not exist or is not a directory: " + basePath);
                return;
            }
        } else {
            // Find the latest directory as before
            Optional<Path> latestDirectory = Files.list(Paths.get(parentPath))
                    .filter(Files::isDirectory)
                    .max(Comparator.comparingLong(file -> file.toFile().lastModified()));

            if (latestDirectory.isPresent()) {
                basePath = latestDirectory.get();
            } else {
                System.err.println("No directories found in the specified path.");
                return;
            }
        }

        // e.g., topic-operator, user-operator, cluster-operator
        final File[] componentDirs = basePath.toFile().listFiles(File::isDirectory);

        if (componentDirs != null) {
            for (File componentDir : componentDirs) {
                // e.g., bulk batch, streaming  ...
                final File[] useCaseDirs = componentDir.listFiles();

                if (useCaseDirs != null) {
                    for (File useCaseDir : useCaseDirs) {
                        final String useCaseName = useCaseDir.getName();
                        final List<ExperimentMetrics> experimentsList = new ArrayList<>();
                        final File[] experimentsFiles = useCaseDir.listFiles();

                        if (experimentsFiles != null) {
                            for (File experimentFile : experimentsFiles) {
                                final File[] metricFiles = experimentFile.listFiles();
                                final ExperimentMetrics experimentMetrics = new ExperimentMetrics();

                                if (metricFiles != null) {
                                    for (File metricFile : metricFiles) {
                                        if (metricFile.getName().endsWith("test-performance-metrics.txt")) {
                                            try {
                                                processFile(metricFile, experimentMetrics);
                                            } catch (IOException e) {
                                                System.err.println("Error processing file " + metricFile.getAbsolutePath());
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
                                                            System.err.println("Error parsing value: " + valueString);
                                                        }
                                                    }
                                                }
                                            }
                                            // TODO: we can do post processing here.. (AVG, Q90, Q99 for strimzi_total_time_spend_on_uto_event_queue_duration_seconds f.e.)
                                            experimentMetrics.addSimpleMetric(metricFile.getName(), values.toString());
                                        }
                                    }
                                }
                                experimentsList.add(experimentMetrics);
                            }
                        }
                        USE_CASE_EXPERIMENTS.put(useCaseName, experimentsList);
                    }
                }
            }
        }
        showValuesOfExperiments();
    }

    private static void processFile(File file, ExperimentMetrics experimentMetrics) throws IOException {
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
     * Helper method to parse the list of doubles from a String and find the max value
     */
    private static double getMaxValueFromList(String listAsString) {
        // Assuming the list is formatted as "[value1, value2, ...]"
        String[] items = listAsString.substring(1, listAsString.length() - 1).split(", ");
        return Arrays.stream(items)
                .mapToDouble(Double::parseDouble)
                .max()
                .orElse(0); // Default to 0 if list is empty or parsing fails
    }

    public static void showValuesOfExperiments() {
        // Populate data for each experiment
        USE_CASE_EXPERIMENTS.forEach((useCaseName, experimentsList) -> {
            System.out.println("Use Case: " + useCaseName);

            String[] headers = getHeadersForUseCase(useCaseName);
            List<String[]> allRows = new ArrayList<>();

            allRows.add(headers);

            // Determine max width for each column
            int[] columnWidths = new int[headers.length];
            for (String[] row : allRows) {
                for (int i = 0; i < row.length; i++) {
                    columnWidths[i] = Math.max(columnWidths[i], row[i].length());
                }
            }

            printSeparator(columnWidths);

            int experimentCounter = 1;

            for (ExperimentMetrics experimentMetrics : experimentsList) {
                Map<String, String> simpleMetrics = experimentMetrics.getSimpleMetrics();
                // Assume methods to extract and format metrics correctly are implemented
                String[] rowData = extractAndFormatRowData(experimentCounter, simpleMetrics, experimentMetrics);
                allRows.add(rowData);
                experimentCounter++;
            }

            // Print table with dynamically adjusted widths
            allRows.forEach(row -> printRow(row, columnWidths));
            printSeparator(columnWidths);
        });
    }

    private static String[] extractAndFormatRowData(int experimentNumber, Map<String, String> simpleMetrics, ExperimentMetrics experimentMetrics) {
        final String numberOfTopics = simpleMetrics.get(PerformanceConstants.NUMBER_OF_TOPICS);
        final String numberOfTopicsToUpdate = simpleMetrics.getOrDefault(PerformanceConstants.BOB_NUMBER_OF_TOPICS_TO_UPDATE, "0");
        final int numberOfClientInstances = Integer.parseInt(simpleMetrics.get(PerformanceConstants.NUMBER_OF_CLIENT_INSTANCES)) * 2;
        final String numberOfMessages = simpleMetrics.get(PerformanceConstants.NUMBER_OF_MESSAGES);
        final double creationTimeSec = Double.parseDouble(simpleMetrics.getOrDefault(PerformanceConstants.CREATION_TIME, "0").split(" ")[0]) / 1000.0;
        final double sendAndRecvSec = Double.parseDouble(simpleMetrics.getOrDefault(PerformanceConstants.SEND_AND_RECV_TIME, "0").split(" ")[0]) / 1000.0;
        final double deletionTimeSec = Double.parseDouble(simpleMetrics.getOrDefault(PerformanceConstants.DELETION_TIME, "0").split(" ")[0]) / 1000.0;
        final double totalTestTimeSec = Double.parseDouble(simpleMetrics.getOrDefault(PerformanceConstants.TOTAL_TEST_TIME, "0").split(" ")[0]) / 1000.0;

        List<ContainerEnvVar> topicOperatorContainerEnvs = experimentMetrics.getKafkaSpec().getEntityOperator().getTemplate().getTopicOperatorContainer().getEnv();
        String strimziMaxBatchSize = "", maxBatchLingerMs = "";
        for (ContainerEnvVar envVar : topicOperatorContainerEnvs) {
            if ("STRIMZI_MAX_BATCH_SIZE".equals(envVar.getName())) {
                strimziMaxBatchSize = envVar.getValue();
            } else if ("MAX_BATCH_LINGER_MS".equals(envVar.getName())) {
                maxBatchLingerMs = envVar.getValue();
            }
        }

        // Inside your method or class where you are using these metrics
        final double maxReconciliationDuration = getMaxValueFromList(simpleMetrics.get(getMetricFileName(PerformanceConstants.RECONCILIATIONS_DURATION_SECONDS_MAX)));
        final double maxBatchSize = getMaxValueFromList(simpleMetrics.get(getMetricFileName(PerformanceConstants.RECONCILIATIONS_MAX_BATCH_SIZE)));
        final double maxQueueSize = getMaxValueFromList(simpleMetrics.get(getMetricFileName(PerformanceConstants.RECONCILIATIONS_MAX_QUEUE_SIZE)));
        final double maxUtoEventQueueTime = getMaxValueFromList(simpleMetrics.get(getMetricFileName(PerformanceConstants.TOTAL_TIME_SPEND_ON_UTO_EVENT_QUEUE_DURATION_SECONDS)));
        final double describeConfigsMaxDuration = getMaxValueFromList(simpleMetrics.get(getMetricFileName(PerformanceConstants.DESCRIBE_CONFIGS_DURATION_SECONDS_MAX)));
        final double createTopicsMaxDuration = getMaxValueFromList(simpleMetrics.get(getMetricFileName(PerformanceConstants.CREATE_TOPICS_DURATION_SECONDS_MAX)));
        final double reconciliationsMaxDuration = getMaxValueFromList(simpleMetrics.get(getMetricFileName(PerformanceConstants.RECONCILIATIONS_DURATION_SECONDS_MAX)));
        final double updateStatusMaxDuration = getMaxValueFromList(simpleMetrics.get(getMetricFileName(PerformanceConstants.UPDATE_STATUS_DURATION_SECONDS_MAX)));
        final double systemLoadAveragePerCore1m = getMaxValueFromList(simpleMetrics.get(getMetricFileName(PerformanceConstants.SYSTEM_LOAD_AVERAGE_PER_CORE_PERCENT))) * 100; // Convert to percentage
        final double jvmMemoryUsedMBs = getMaxValueFromList(simpleMetrics.get(getMetricFileName(PerformanceConstants.JVM_MEMORY_USED_MEGABYTES_TOTAL)));

        final List<String> rowData = new ArrayList<>();

        rowData.add(String.valueOf(experimentNumber));
        rowData.add(numberOfTopics);
        rowData.add(String.valueOf(numberOfClientInstances));
        rowData.add(numberOfMessages);

        final String bobUpdateTimes = formatBobUpdateTimes(simpleMetrics);
        if (!bobUpdateTimes.isEmpty()) {
            rowData.add(bobUpdateTimes);
        }
        if (!numberOfTopicsToUpdate.equals("0")) {
            rowData.add(numberOfTopicsToUpdate);
        }

        rowData.add(String.format("%.3f", creationTimeSec));
        rowData.add(String.format("%.3f", sendAndRecvSec));
        rowData.add(String.format("%.3f", deletionTimeSec));
        rowData.add(String.format("%.3f", totalTestTimeSec));
        rowData.add(strimziMaxBatchSize);
        rowData.add(maxBatchLingerMs);
        rowData.add(String.format("%.3f", maxReconciliationDuration));
        rowData.add(String.format("%.3f", maxBatchSize));
        rowData.add(String.format("%.3f", maxQueueSize));
        rowData.add(String.format("%.3f", maxUtoEventQueueTime));
        rowData.add(String.format("%.3f", describeConfigsMaxDuration));
        rowData.add(String.format("%.3f", createTopicsMaxDuration));
        rowData.add(String.format("%.3f", reconciliationsMaxDuration));
        rowData.add(String.format("%.3f", updateStatusMaxDuration));
        rowData.add(String.format("%.1f%%", systemLoadAveragePerCore1m));
        rowData.add(String.format("%.2f", jvmMemoryUsedMBs));

        return rowData.toArray(new String[0]);
    }

    private static String formatBobUpdateTimes(Map<String, String> simpleMetrics) {
        StringBuilder bobUpdateTimes = new StringBuilder();
        for (int i = 1; i <= 3; i++) { // Assuming up to 3 rounds for simplification
            String key = "Bob Update Times Round " + i;
            if (simpleMetrics.containsKey(key)) {
                if (bobUpdateTimes.length() > 0) bobUpdateTimes.append(", ");
                bobUpdateTimes.append(Double.parseDouble(simpleMetrics.get(key).split(" ")[0]) / 1000.0);
            }
        }
        return bobUpdateTimes.toString();
    }


    private static void printRow(String[] row, int[] columnWidths) {
        StringBuilder sb = new StringBuilder("|");
        for (int i = 0; i < row.length; i++) {
            sb.append(String.format(" %-" + columnWidths[i] + "s |", row[i]));
        }
        System.out.println(sb);
    }

    private static void printSeparator(int[] columnWidths) {
        StringBuilder sb = new StringBuilder("+");
        for (int width : columnWidths) {
            sb.append("-".repeat(width + 2)).append("+"); // +2 for the padding on either side of the value
        }
        System.out.println(sb);
    }

    private static String[] getHeadersForUseCase(String useCaseName) {
        return switch (useCaseName) {
            case PerformanceConstants.TOPIC_OPERATOR_ALICE_BULK_USE_CASE -> new String[]{
                "Experiment", PerformanceConstants.NUMBER_OF_TOPICS, PerformanceConstants.NUMBER_OF_CLIENT_INSTANCES,
                PerformanceConstants.NUMBER_OF_MESSAGES, PerformanceConstants.CREATION_TIME + " (s)",
                PerformanceConstants.SEND_AND_RECV_TIME + " (s)", PerformanceConstants.DELETION_TIME + " (s)",
                PerformanceConstants.TOTAL_TEST_TIME + " (s)",
                "STRIMZI_MAX_BATCH_SIZE", "MAX_BATCH_LINGER_MS", "Reconciliation Max Duration (s)",
                "Max Batch Size", "Max Queue Size", "UTO Event Queue Time (s)",
                "Describe Configs Max Duration (s)", "Create Topics Max Duration (s)",
                "Reconciliations Max Duration (s)", "Update Status Max Duration (s)",
                "Max System Load Average 1m Per Core (%)", "Max JVM Memory Used (MBs)"
            };
            case PerformanceConstants.TOPIC_OPERATOR_BOBS_STREAMING_USE_CASE -> new String[]{
                "Experiment", PerformanceConstants.NUMBER_OF_TOPICS, PerformanceConstants.NUMBER_OF_CLIENT_INSTANCES,
                PerformanceConstants.NUMBER_OF_MESSAGES, "Bob Update Times Round 1,2,3 (s)",
                PerformanceConstants.BOB_NUMBER_OF_TOPICS_TO_UPDATE,
                PerformanceConstants.CREATION_TIME + " (s)",
                PerformanceConstants.SEND_AND_RECV_TIME + " (s)", PerformanceConstants.DELETION_TIME + " (s)",
                PerformanceConstants.TOTAL_TEST_TIME + " (s)",
                "STRIMZI_MAX_BATCH_SIZE", "MAX_BATCH_LINGER_MS", "Reconciliation Max Duration (s)",
                "Max Batch Size", "Max Queue Size", "UTO Event Queue Time (s)",
                "Describe Configs Max Duration (s)", "Create Topics Max Duration (s)",
                "Reconciliations Max Duration (s)", "Update Status Max Duration (s)",
                "Max System Load Average 1m Per Core (%)", "Max JVM Memory Used (MBs)"
            };
            default -> throw new IllegalArgumentException("Unsupported use case: " + useCaseName);
        };
    }

    /**
     * Appends the .txt file extension to a metric name.
     * @param metricName The name of the metric without the file extension.
     * @return The metric name with the .txt file extension.
     */
    public static String getMetricFileName(String metricName) {
        return metricName + ".txt";
    }
}
