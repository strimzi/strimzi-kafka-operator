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
 * The {@code UserOperatorMetricsParser} class extends {@link BasePerformanceMetricsParser} to parse and analyze
 * performance metrics specific to the User Operator within the Strimzi Kafka environment. It handles the extraction,
 * processing, and display of key metrics that are crucial for assessing the performance characteristics of the User
 * Operator under various test scenarios.
 *
 * <p>This parser is tailored to focus on metrics such as system load per core and JVM memory usage, which are pivotal
 * in understanding the resource utilization and efficiency of the User Operator. The class filters and processes
 * metrics based on predefined sets of interest to ensure that the analysis is relevant and concise.</p>
 *
 * <p><strong>Key Features:</strong></p>
 * <ul>
 *     <li><strong>Metrics Filtering:</strong> Only metrics of specific interest, defined in {@link #METRICS_OF_INTEREST},
 *     are processed. This targeted approach helps in focusing on the most impactful data for performance evaluation.</li>
 *     <li><strong>Dynamic Data     Extraction:</strong> Implements methods to dynamically extract and format metrics data
 *     from experimental results, accommodating variations in data availability and structure.</li>
 *     <li><strong>Result Presentation:</strong> Generates structured output that clearly presents the parsed metrics,
 *     making it easy to interpret the performance results.</li>
 * </ul>
 *
 * <p><strong>Usage:</strong> This parser can be utilized within automated test frameworks to evaluate the performance
 * of the User Operator, or it can be run as a standalone tool to parse existing metrics data from specified directories.</p>
 *
 * <p><strong>Methods Overview:</strong></p>
 * <ul>
 *     <li>{@link #extractAndFormatRowData(int, ExperimentMetrics)} - Extracts and formats a row of data for each
 *     experiment based on its metrics.</li>
 *     <li>{@link #getHeadersForUseCase(ExperimentMetrics)} - Generates the headers for the data output based on the
 *     metrics present in the experiment data.</li>
 *     <li>{@link #parseMetrics()} - Parses the metrics from the specified or default directory and processes them.</li>
 *     <li>{@link #showMetrics()} - Displays the formatted metrics data.</li>
 * </ul>
 */
public class UserOperatorMetricsParser extends BasePerformanceMetricsParser {

    private static final Logger LOGGER = LogManager.getLogger(UserOperatorMetricsParser.class);

    /**
     * A set of metric identifiers considered relevant for inclusion in reports. These identifiers are predefined
     * and are used to filter and process only the metrics that are of interest for specific test scenarios.
     */
    private static final Set<String> METRICS_OF_INTEREST = Set.of(
        getMetricFileName(PerformanceConstants.SYSTEM_LOAD_AVERAGE_PER_CORE_PERCENT),
        getMetricFileName(PerformanceConstants.JVM_MEMORY_USED_MEGABYTES_TOTAL)
    );

    private static final Set<String> COMPONENT_PARAMETERS_TO_IGNORE = Set.of(
        PerformanceConstants.KAFKA_IN_CONFIGURATION);

    public UserOperatorMetricsParser() {
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
