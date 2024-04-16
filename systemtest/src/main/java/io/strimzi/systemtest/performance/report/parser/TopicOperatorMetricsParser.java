/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.report.parser;

import io.strimzi.systemtest.performance.PerformanceConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Parses and displays performance metrics from experiment results located in the
 * {@code TestUtils.USER_PATH + "/target/performance"} directory. This parser is specialized for performance metrics
 * generated from Kafka Topic Operator tests, providing detailed insights into various operational characteristics.
 *
 * <p>This class categorizes metrics by use case and experiment, calculates various statistical measures, and
 * formats them for output. It is capable of functioning both in test environments and as a standalone application,
 * adjusting file paths and processing logic accordingly.</p>
 *
 * <p><strong>Usage:</strong></p>
 * <ul>
 *     <li><strong>Standalone Application:</strong> Run with the directory path containing performance metrics
 *     files as a command-line argument. If no directory is specified, the parser attempts to find and use the
 *     latest directory under the default path.</li>
 *     <li><strong>Test Environment:</strong> Ensure the {@code sun.java.command} system property contains
 *     "JUnitStarter". This adjusts the base path for metrics files to align with expected test directory structures.</li>
 * </ul>
 *
 * <p><strong>Key Features:</strong></p>
 * <ul>
 *     <li>Filtering of metrics based on predefined sets of interest specific to test scenarios.</li>
 *     <li>Dynamic extraction and formatting of row data for reporting, based on the content of experimental metrics.</li>
 *     <li>Generation of appropriate headers for data columns based on the actual metrics collected during tests.</li>
 * </ul>
 *
 * <p><strong>Key Methods:</strong></p>
 * <ul>
 *     <li>{@link #isRunningInTest()} - Checks if the application is running within a test environment.</li>
 *     <li>{@link #main(String[])} - Main entry point for parsing and displaying performance metrics when run as an application.</li>
 *     <li>{@link #buildResultTable()} - Aggregates and displays formatted metrics for all tracked experiments and use cases.</li>
 * </ul>
 *
 * @see BasePerformanceMetricsParser Base class for handling basic parsing operations and environment setup.
 */
public class TopicOperatorMetricsParser extends BasePerformanceMetricsParser {

    private static final Logger LOGGER = LogManager.getLogger(TopicOperatorMetricsParser.class);

    /**
     * A set of metric identifiers considered relevant for inclusion in reports. These identifiers are predefined
     * and are used to filter and process only the metrics that are of interest for specific test scenarios.
     */
    private static final Set<String> METRICS_OF_INTEREST = Set.of(
        getMetricFileName(PerformanceConstants.RECONCILIATIONS_DURATION_SECONDS_MAX),
        getMetricFileName(PerformanceConstants.RECONCILIATIONS_MAX_BATCH_SIZE),
        getMetricFileName(PerformanceConstants.RECONCILIATIONS_MAX_QUEUE_SIZE),
        getMetricFileName(PerformanceConstants.TOTAL_TIME_SPEND_ON_UTO_EVENT_QUEUE_DURATION_SECONDS),
        getMetricFileName(PerformanceConstants.DESCRIBE_CONFIGS_DURATION_SECONDS_MAX),
        getMetricFileName(PerformanceConstants.CREATE_TOPICS_DURATION_SECONDS_MAX),
        getMetricFileName(PerformanceConstants.UPDATE_STATUS_DURATION_SECONDS_MAX),
        getMetricFileName(PerformanceConstants.SYSTEM_LOAD_AVERAGE_PER_CORE_PERCENT),
        getMetricFileName(PerformanceConstants.JVM_MEMORY_USED_MEGABYTES_TOTAL)
    );

    private static final Set<String> COMPONENT_PARAMETERS_TO_IGNORE = Set.of(
        PerformanceConstants.KAFKA_IN_CONFIGURATION);

    public TopicOperatorMetricsParser() {
        super();
    }

    /**
     * Extracts and formats a row of data from experimental metrics for reporting. This method dynamically adjusts
     * the data extraction based on the content of the experiment metrics, ensuring that all relevant test and
     * component metrics are correctly captured and formatted.
     *
     * @param experimentNumber The unique identifier for the experiment whose metrics are being parsed.
     * @param experimentMetrics The {@link ExperimentMetrics} object containing both test and component metrics.
     * @return An array of strings, each representing a column value in the resultant data row.
     */
    @Override
    protected String[] extractAndFormatRowData(int experimentNumber, ExperimentMetrics experimentMetrics) {
        final List<String> rowData = new ArrayList<>();

        rowData.add(String.valueOf(experimentNumber));

        for (final Map.Entry<String, String> testMetric : experimentMetrics.getTestMetrics().entrySet()) {
            if (COMPONENT_PARAMETERS_TO_IGNORE.contains(testMetric.getKey())) {
                LOGGER.trace("Excluding parameter: {} for result table", testMetric.getKey());
                // ignore such parameter
                continue;
            }
            rowData.add(testMetric.getValue());
        }

        for (final Map.Entry<String, List<Double>> componentMetric : experimentMetrics.getComponentMetrics().entrySet()) {
            if (METRICS_OF_INTEREST.contains(componentMetric.getKey())) {
                // get max
                rowData.add(String.valueOf(getMaxValueFromList(componentMetric.getValue())));
            }
        }

        return rowData.toArray(new String[0]);
    }

    /**
     * Generates headers for the data columns based on the metrics present in the experiment metrics. This ensures
     * that each column in the output is accurately labeled according to the metrics it represents.
     *
     * @param experimentMetrics The {@link ExperimentMetrics} used to determine the headers based on its content.
     * @return An array of headers, each corresponding to a column in the data output.
     */
    @Override
    protected String[] getHeadersForUseCase(ExperimentMetrics experimentMetrics) {
        final List<String> headers = new ArrayList<>();

        headers.add("Experiment");

        // for test metrics
        for (final Map.Entry<String, String> testMetric : experimentMetrics.getTestMetrics().entrySet()) {
            if (COMPONENT_PARAMETERS_TO_IGNORE.contains(testMetric.getKey())) {
                LOGGER.trace("Excluding parameter: {} for result table", testMetric.getKey());
                // ignore such parameter
                continue;
            }
            headers.add(testMetric.getKey());
        }

        // for component metrics
        for (final Map.Entry<String, List<Double>> componentMetric : experimentMetrics.getComponentMetrics().entrySet()) {
            if (METRICS_OF_INTEREST.contains(componentMetric.getKey())) {
                headers.add(componentMetric.getKey());
            }
        }

        return headers.toArray(new String[0]); // Convert ArrayList back to array
    }

    @Override
    public void parseMetrics() throws IOException {
        this.parseLatestMetrics();
    }

    @Override
    protected void showMetrics() {
        System.out.println(this.buildResultTable());
    }
}
