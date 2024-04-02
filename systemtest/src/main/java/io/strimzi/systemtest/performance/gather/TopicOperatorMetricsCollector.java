/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.gather;

import io.strimzi.systemtest.metrics.MetricsCollector;
import io.strimzi.systemtest.performance.PerformanceConstants;
import io.strimzi.systemtest.resources.ComponentType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Collects metrics related to the Topic Operator within Strimzi, facilitating the monitoring of various operational aspects.
 * This class extends {@link MetricsCollector} to gather metrics specific to topic operations, reconciliations, JVM, and system performance.
 */
public class TopicOperatorMetricsCollector extends MetricsCollector {

    /**
     * Constructs a {@code TopicOperatorMetricsCollector} using the builder pattern for configuration.
     *
     * @param builder the {@link MetricsCollector.Builder} used to configure the collector
     */
    protected TopicOperatorMetricsCollector(MetricsCollector.Builder builder) {
        super(builder);
    }

    // -----------------------------------------------------------------------------------------------------
    // -------------------------------- SUM OPERATION METRICS ----------------------------------------------
    // -----------------------------------------------------------------------------------------------------
    public List<Double> getAlterConfigsDurationSecondsSum(String selector) {
        return collectMetricValues(PerformanceConstants.ALTER_CONFIGS_DURATION_SECONDS_SUM, selector);
    }

    public List<Double> getRemoveFinalizerDurationSecondsSum(String selector) {
        return collectMetricValues(PerformanceConstants.REMOVE_FINALIZER_DURATION_SECONDS_SUM, selector);
    }

    public List<Double> getReconciliationsDurationSecondsSum(String selector) {
        return collectMetricValues(PerformanceConstants.RECONCILIATIONS_DURATION_SECONDS_SUM, selector);
    }

    public List<Double> getCreateTopicsDurationSecondsSum(String selector) {
        return collectMetricValues(PerformanceConstants.CREATE_TOPICS_DURATION_SECONDS_SUM, selector);
    }

    public List<Double> getDescribeTopicsDurationSecondsSum(String selector) {
        return collectMetricValues(PerformanceConstants.DESCRIBE_TOPICS_DURATION_SECONDS_SUM, selector);
    }

    public List<Double> getCreatePartitionsDurationSecondsSum(String selector) {
        return collectMetricValues(PerformanceConstants.CREATE_PARTITIONS_DURATION_SECONDS_SUM, selector);
    }

    public List<Double> getAddFinalizerDurationSecondsSum(String selector) {
        return collectMetricValues(PerformanceConstants.ADD_FINALIZER_DURATION_SECONDS_SUM, selector);
    }

    public List<Double> getUpdateStatusDurationSecondsSum(String selector) {
        return collectMetricValues(PerformanceConstants.UPDATE_STATUS_DURATION_SECONDS_SUM, selector);
    }

    public List<Double> getDescribeConfigsDurationSecondsSum(String selector) {
        return collectMetricValues(PerformanceConstants.DESCRIBE_CONFIGS_DURATION_SECONDS_SUM, selector);
    }

    public List<Double> getDeleteTopicsDurationSecondsSum(String selector) {
        return collectMetricValues(PerformanceConstants.DELETE_TOPICS_DURATION_SECONDS_SUM, selector);
    }

    // -----------------------------------------------------------------------------------------------------
    // --------------------------------- RECONCILIATIONS METRICS -------------------------------------------
    // -----------------------------------------------------------------------------------------------------

    public List<Double> getReconciliationsDurationSecondsMax(String selector) {
        return collectMetricValues(PerformanceConstants.RECONCILIATIONS_DURATION_SECONDS_MAX, selector);
    }

    public List<Double> getReconciliationsMaxQueueSize(String selector) {
        return collectMetricValues(PerformanceConstants.RECONCILIATIONS_MAX_QUEUE_SIZE, selector);
    }

    public List<Double> getReconciliationsMaxBatchSize(String selector) {
        return collectMetricValues(PerformanceConstants.RECONCILIATIONS_MAX_BATCH_SIZE, selector);
    }

    public List<Double> getReconciliationsSuccessfulTotal(String selector) {
        return collectMetricValues(PerformanceConstants.RECONCILIATIONS_SUCCESSFUL_TOTAL, selector);
    }

    public List<Double> getReconciliationsTotal(String selector) {
        return collectMetricValues(PerformanceConstants.RECONCILIATIONS_TOTAL, selector);
    }

    public List<Double> getReconciliationsFailedTotal(String selector) {
        return collectMetricValues(PerformanceConstants.RECONCILIATIONS_FAILED_TOTAL, selector);
    }

    public List<Double> getReconciliationsLockedTotal(String selector) {
        return collectMetricValues(PerformanceConstants.RECONCILIATIONS_LOCKED_TOTAL, selector);
    }

    // -----------------------------------------------------------------------------------------------------
    // -------------------------------- CREATE OPERATION METRICS -------------------------------------------
    // -----------------------------------------------------------------------------------------------------

    public List<Double> getCreateTopicsDurationSecondsMax(String selector) {
        return collectMetricValues(PerformanceConstants.CREATE_TOPICS_DURATION_SECONDS_MAX, selector);
    }

    // -----------------------------------------------------------------------------------------------------
    // -------------------------------- DELETE OPERATION METRICS -------------------------------------------
    // -----------------------------------------------------------------------------------------------------

    public List<Double> getDeleteTopicsDurationSecondsMax(String selector) {
        return collectMetricValues(PerformanceConstants.DELETE_TOPICS_DURATION_SECONDS_MAX, selector);
    }

    // -----------------------------------------------------------------------------------------------------
    // -------------------------------- UPDATE OPERATION METRICS -------------------------------------------
    // -----------------------------------------------------------------------------------------------------

    public List<Double> getUpdateStatusDurationSecondsMax(String selector) {
        return collectMetricValues(PerformanceConstants.UPDATE_STATUS_DURATION_SECONDS_MAX, selector);
    }

    // -----------------------------------------------------------------------------------------------------
    // ------------------------------- DESCRIBE OPERATION METRICS ------------------------------------------
    // -----------------------------------------------------------------------------------------------------

    public List<Double> getDescribeTopicsDurationSecondsMax(String selector) {
        return collectMetricValues(PerformanceConstants.DESCRIBE_TOPICS_DURATION_SECONDS_MAX, selector);
    }

    // -----------------------------------------------------------------------------------------------------
    // -------------------------------- CONFIGS OPERATION METRICS ------------------------------------------
    // -----------------------------------------------------------------------------------------------------

    public List<Double> getAlterConfigsDurationSecondsMax(String selector) {
        return collectMetricValues(PerformanceConstants.ALTER_CONFIGS_DURATION_SECONDS_MAX, selector);
    }

    public List<Double> getDescribeConfigsDurationSecondsMax(String selector) {
        return collectMetricValues(PerformanceConstants.DESCRIBE_CONFIGS_DURATION_SECONDS_MAX, selector);
    }

    // -----------------------------------------------------------------------------------------------------
    // --------------------------------  RESOURCE METRICS ----------------------------------------------
    // -----------------------------------------------------------------------------------------------------

    public List<Double> getResourcesKafkaTopics(String selector) {
        return collectMetricValues(PerformanceConstants.RESOURCES, selector);
    }

    // -----------------------------------------------------------------------------------------------------
    // ---------------------------------- FINALIZERS METRICS -----------------------------------------------
    // -----------------------------------------------------------------------------------------------------

    public List<Double> getAddFinalizerDurationSecondsMax(String selector) {
        return collectMetricValues(PerformanceConstants.ADD_FINALIZER_DURATION_SECONDS_MAX, selector);
    }

    public List<Double> getRemoveFinalizerDurationSecondsMax(String selector) {
        return collectMetricValues(PerformanceConstants.REMOVE_FINALIZER_DURATION_SECONDS_MAX, selector);
    }

    // -----------------------------------------------------------------------------------------------------
    // ---------------------------------- JVM and System METRICS -------------------------------------------
    // -----------------------------------------------------------------------------------------------------

    public List<Double> getSystemCpuCount() {
        return collectMetricSimpleValues(PerformanceConstants.SYSTEM_CPU_COUNT);
    }

    public List<Double> getJvmGcMemoryAllocatedBytesTotal() {
        return collectMetricSimpleValues(PerformanceConstants.JVM_GC_MEMORY_ALLOCATED_BYTES_TOTAL);
    }

    /**
     * Parses out the 'jvm_memory_used_bytes' metric from the collected data.
     * @return A map with unique keys for each label combination and their corresponding metric values.
     */
    public Map<String, Double> getJvmMemoryUsedBytes() {
        String metricName = "jvm_memory_used_bytes";
        // Call the collectSpecificMetric method with the pattern to get the metric values.
        Map<String, Double> jvmMemoryUsedBytes = collectMetricWithLabels(metricName);

        return jvmMemoryUsedBytes;
    }

    public List<Double> getJvmThreadsLiveThreads() {
        return collectMetricSimpleValues(PerformanceConstants.JVM_THREADS_LIVE_THREADS);
    }

    public List<Double> getSystemCpuUsage() {
        return collectMetricSimpleValues(PerformanceConstants.SYSTEM_CPU_USAGE);
    }

    public Map<String, Double>  getJvmGcPauseSecondsMax() {
        String metricName = "jvm_gc_pause_seconds_max";
        // Call the collectSpecificMetric method with the pattern to get the metric values.
        Map<String, Double> jvmGcPauseSecondsMax = collectMetricWithLabels(metricName);

        return jvmGcPauseSecondsMax;
    }

    public List<Double> getJvmMemoryMaxBytes() {
        return collectMetricValues(PerformanceConstants.JVM_MEMORY_MAX_BYTES);
    }

    public List<Double> getProcessCpuUsage() {
        return collectMetricSimpleValues(PerformanceConstants.PROCESS_CPU_USAGE);
    }

    public List<Double> getSystemLoadAverage1m() {
        return collectMetricSimpleValues(PerformanceConstants.SYSTEM_LOAD_AVERAGE_1M);
    }

    // -----------------------------------------------------------------------------------------------------

    private List<Double> collectMetricValues(String metricName, String selector) {
        Pattern pattern = Pattern.compile(metricName + "\\{kind=\"KafkaTopic\",namespace=\"" + this.getNamespaceName() + "\",selector=\"" + selector + "\",.*\\}\\s(\\d+\\.?\\d*)");
        return collectSpecificMetric(pattern);
    }

    private List<Double> collectMetricValues(String metricName) {
        Pattern pattern = Pattern.compile(metricName + "\\{.*\\}\\s(\\d+\\.?\\d*)");
        return collectSpecificMetric(pattern);
    }

    private List<Double> collectMetricSimpleValues(String metricName) {
        // Updated pattern to match numbers in standard or scientific notation
        Pattern pattern = Pattern.compile("^" + Pattern.quote(metricName) + "\\s+([\\d.]+(?:E-?\\d+)?)$", Pattern.MULTILINE);
        return collectSpecificMetric(pattern);
    }

    // Example of a bucketed metric method
    public List<Double> getAlterConfigsDurationSecondsBucket(String selector, String le) {
        Pattern pattern = Pattern.compile("strimzi_alter_configs_duration_seconds_bucket\\{kind=\"KafkaTopic\",namespace=\"" + this.getNamespaceName() + "\",selector=\"" + selector + "\",le=\"" + le + "\",\\}\\s(\\d+\\.?\\d*)");
        return collectSpecificMetric(pattern);
    }

    // Example for a method collecting multiple buckets at once
    public Map<String, List<Double>> getAlterConfigsDurationSecondsBuckets(String selector, List<String> les) {
        Map<String, List<Double>> buckets = new HashMap<>();
        les.forEach(le -> buckets.put(le, getAlterConfigsDurationSecondsBucket(selector, le)));
        return buckets;
    }

    /**
     * Builder for {@link TopicOperatorMetricsCollector}, allowing for customizable configuration.
     */
    public static class Builder extends MetricsCollector.Builder {
        @Override
        public TopicOperatorMetricsCollector build() {
            return new TopicOperatorMetricsCollector(this);
        }

        // Override the builder methods to return the type of this Builder, allowing method chaining
        @Override
        public Builder withNamespaceName(String namespaceName) {
            super.withNamespaceName(namespaceName);
            return this;
        }

        @Override
        public Builder withScraperPodName(String scraperPodName) {
            super.withScraperPodName(scraperPodName);
            return this;
        }

        @Override
        public Builder withComponentType(ComponentType componentType) {
            super.withComponentType(componentType);
            return this;
        }

        @Override
        public Builder withComponentName(String componentName) {
            super.withComponentName(componentName);
            return this;
        }

        @Override
        public Builder withMetricsPort(int metricsPort) {
            super.withMetricsPort(metricsPort);
            return this;
        }

        @Override
        public Builder withMetricsPath(String metricsPath) {
            super.withMetricsPath(metricsPath);
            return this;
        }
    }
}