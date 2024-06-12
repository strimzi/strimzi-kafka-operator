/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.gather.collectors;

import io.skodjob.testframe.MetricsCollector;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.systemtest.performance.PerformanceConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Abstract base class for metrics collection tailored to gather various JVM and system metrics.
 * This class extends {@link MetricsCollector} to provide specialized methods that retrieve specific
 * metrics values, useful in performance analysis contexts within Strimzi system tests.
 *
 * Metrics gathered include system CPU count, JVM memory allocation, thread details, and other
 * system performance indicators.
 */
public abstract class BaseMetricsCollector extends MetricsCollector {

    private static final Logger LOGGER = LogManager.getLogger(BaseMetricsCollector.class);

    /**
     * Constructs a new {@code BaseMetricsCollector} instance configured via the provided builder.
     * @param builder       The builder used to configure this collector.
     */
    protected BaseMetricsCollector(Builder builder) {
        super(builder);
    }

    // -----------------------------------------------------------------------------------------------------
    // ---------------------------------- JVM and System METRICS -------------------------------------------
    // -----------------------------------------------------------------------------------------------------

    /**
     * Retrieves the count of CPUs available to the JVM.
     * @return              A list of {@link Double} values representing the count of available CPUs.
     */
    public List<Double> getSystemCpuCount() {
        return collectMetricSimpleValues(PerformanceConstants.SYSTEM_CPU_COUNT);
    }

    /**
     * Retrieves the total bytes allocated by the JVM garbage collector.
     * @return              A list of {@link Double} values representing the total bytes allocated.
     */
    public List<Double> getJvmGcMemoryAllocatedBytesTotal() {
        return collectMetricSimpleValues(PerformanceConstants.JVM_GC_MEMORY_ALLOCATED_BYTES_TOTAL);
    }

    /**
     * Retrieves the used JVM memory bytes.
     * @return              A map with keys representing unique label combinations and their corresponding metric values for used memory.
     */
    public Map<String, Double> getJvmMemoryUsedBytes() {
        return collectMetricWithLabels(PerformanceConstants.JVM_MEMORY_USED_BYTES);
    }

    /**
     * Retrieves the number of live threads in the JVM.
     * @return              A list of {@link Double} values representing the count of live JVM threads.
     */
    public List<Double> getJvmThreadsLiveThreads() {
        return collectMetricSimpleValues(PerformanceConstants.JVM_THREADS_LIVE_THREADS);
    }

    /**
     * Retrieves the CPU usage of the system.
     * @return              A list of {@link Double} values representing the system CPU usage.
     */
    public List<Double> getSystemCpuUsage() {
        return collectMetricSimpleValues(PerformanceConstants.SYSTEM_CPU_USAGE);
    }

    /**
     * Retrieves the maximum duration of garbage collection pauses in the JVM.
     * @return              A map with keys representing unique label combinations and their corresponding maximum GC pause times.
     */
    public Map<String, Double>  getJvmGcPauseSecondsMax() {
        return collectMetricWithLabels(PerformanceConstants.JVM_GC_PAUSE_SECONDS_MAX);
    }

    /**
     * Retrieves the maximum bytes of memory that can be used by the JVM.
     * @return              A list of {@link Double} values representing the maximum memory bytes of the JVM.
     */
    public List<Double> getJvmMemoryMaxBytes() {
        return collectMetricValues(PerformanceConstants.JVM_MEMORY_MAX_BYTES);
    }

    /**
     * Retrieves the CPU usage by the process running the JVM.
     * @return              A list of {@link Double} values representing the process CPU usage.
     */
    public List<Double> getProcessCpuUsage() {
        return collectMetricSimpleValues(PerformanceConstants.PROCESS_CPU_USAGE);
    }

    /**
     * Retrieves the one-minute load average of the system similar to the 'uptime' command.
     * @return              A list of {@link Double} values representing the one-minute system load average.
     */
    public List<Double> getSystemLoadAverage1m() {
        return collectMetricSimpleValues(PerformanceConstants.SYSTEM_LOAD_AVERAGE_1M);
    }

    /**
     * Helper method to collect metrics values based on a specified metric name and selector.
     * @param metricName    The name of the metric to collect.
     * @param selector      The selector to narrow down the metric collection.
     * @return              A list of {@link Double} values collected based on the specified metric name and selector.
     */
    protected List<Double> collectMetricValues(String metricName, String selector) {
        Pattern pattern = Pattern.compile(metricName + "\\{kind=\"" + KafkaTopic.RESOURCE_KIND + "\",namespace=\"" + this.getNamespaceName() + "\",selector=\"" + selector + "\",.*\\}\\s(\\d+\\.?\\d*)");
        return collectSpecificMetric(pattern);
    }

    /**
     * Helper method to collect metrics values based on a specified metric name without additional selectors.
     * @param metricName    The name of the metric to collect.
     * @return              A list of {@link Double} values collected based on the specified metric name.
     */
    protected List<Double> collectMetricValues(String metricName) {
        Pattern pattern = Pattern.compile(metricName + "\\{.*\\}\\s(\\d+\\.?\\d*)");
        return collectSpecificMetric(pattern);
    }

    /**
     * Simplifies metric collection by directly collecting simple metric values without complex labels or selectors.
     * @param metricName    The name of the simple metric to collect.
     * @return              A list of {@link Double} values for the specified simple metric.
     */
    protected List<Double> collectMetricSimpleValues(String metricName) {
        // Updated pattern to match numbers in standard or scientific notation
        Pattern pattern = Pattern.compile("^" + Pattern.quote(metricName) + "\\s+([\\d.]+(?:E-?\\d+)?)$", Pattern.MULTILINE);
        return collectSpecificMetric(pattern);
    }

}
