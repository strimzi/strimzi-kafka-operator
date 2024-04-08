/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.report.parser;

import io.strimzi.api.kafka.model.common.ContainerEnvVar;
import io.strimzi.systemtest.performance.PerformanceConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
 *     <li>{@link #showValuesOfExperiments()} - Displays formatted metrics for all experiments and use cases.</li>
 * </ul>
 */
public class TopicOperatorMetricsParser extends BasePerformanceMetricsParser {

    private static final Logger LOGGER = LogManager.getLogger(TopicOperatorMetricsParser.class);

    public TopicOperatorMetricsParser() {
        super();
    }

    @Override
    protected String[] extractAndFormatRowData(int experimentNumber, Map<String, String> simpleMetrics, ExperimentMetrics experimentMetrics) {
        final String numberOfTopics = simpleMetrics.get(PerformanceConstants.NUMBER_OF_TOPICS);
        final String numberOfTopicsToUpdate = simpleMetrics.getOrDefault(PerformanceConstants.TOPIC_OPERATOR_NUMBER_OF_TOPICS_TO_UPDATE, "0");
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

    @Override
    protected String[] getHeadersForUseCase(String useCaseName) {
        final List<String> headers = new ArrayList<>();
        headers.add("Experiment");
        headers.add(PerformanceConstants.NUMBER_OF_TOPICS);
        headers.add(PerformanceConstants.NUMBER_OF_CLIENT_INSTANCES);
        headers.add(PerformanceConstants.NUMBER_OF_MESSAGES);
        headers.add(PerformanceConstants.CREATION_TIME + " (s)");
        headers.add(PerformanceConstants.SEND_AND_RECV_TIME + " (s)");
        headers.add(PerformanceConstants.DELETION_TIME + " (s)");
        headers.add(PerformanceConstants.TOTAL_TEST_TIME + " (s)");
        headers.add("STRIMZI_MAX_BATCH_SIZE");
        headers.add("MAX_BATCH_LINGER_MS");
        headers.add("Reconciliation Max Duration (s)");
        headers.add("Max Batch Size");
        headers.add("Max Queue Size");
        headers.add("UTO Event Queue Time (s)");
        headers.add("Describe Configs Max Duration (s)");
        headers.add("Create Topics Max Duration (s)");
        headers.add("Reconciliations Max Duration (s)");
        headers.add("Update Status Max Duration (s)");
        headers.add("Max System Load Average 1m Per Core (%)");
        headers.add("Max JVM Memory Used (MBs)");

        // Now, customize for Bob's scenario
        if (PerformanceConstants.TOPIC_OPERATOR_BOBS_STREAMING_USE_CASE.equals(useCaseName)) {
            headers.add(4, "Bob Update Times Round 1,2,3 (s)"); // Insert at the correct position
            headers.add(5, PerformanceConstants.TOPIC_OPERATOR_NUMBER_OF_TOPICS_TO_UPDATE);
        }

        return headers.toArray(new String[0]); // Convert ArrayList back to array
    }

    @Override
    public void parseMetrics() throws IOException {
        this.parseLatestMetrics();
    }

    @Override
    protected void showMetrics() {
        this.showValuesOfExperiments();
    }
}
