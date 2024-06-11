/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.gather.schedulers;

import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.performance.PerformanceConstants;
import io.strimzi.systemtest.performance.gather.collectors.UserOperatorMetricsCollector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * This class extends {@link BaseMetricsCollectionScheduler} to provide a scheduled metrics collection
 * specifically for the User Operator within Strimzi Kafka environments. It leverages the {@link UserOperatorMetricsCollector}
 * to gather metrics relevant to the user operations, ensuring that detailed performance data is periodically captured and stored.
 *
 * <p>The scheduler orchestrates the execution of metrics gathering and the structured storage of these metrics,
 * allowing for detailed analysis of the User Operator's performance over time. This includes data on reconciliation processes,
 * JVM usage, system CPU usage, and other critical performance metrics.</p>
 *
 * <p>Usage involves configuring an instance with a specific selector to target the desired metrics,
 * and then starting the metrics collection process, which will run at intervals defined by the scheduler's configuration.</p>
 *
 * <p>This class is essential for performance testing and monitoring in environments where the User Operator's efficiency and
 * scalability are of concern. By extending {@link BaseMetricsCollectionScheduler}, it inherits a methodical approach to scheduling
 * and data management, ensuring consistent and reliable metrics collection.</p>
 */
public class UserOperatorMetricsCollectionScheduler extends BaseMetricsCollectionScheduler {

    private static final Logger LOGGER = LogManager.getLogger(UserOperatorMetricsCollectionScheduler.class);
    private final UserOperatorMetricsCollector userOperatorMetricsCollector;

    /**
     * Constructs a {@code UserOperatorMetricsCollectionScheduler} with a specified metrics collector and selector.
     *
     * @param userOperatorMetricsCollector  The {@link UserOperatorMetricsCollector} used for gathering metrics.
     * @param selector                      The selector string that specifies the target resources for metrics collection.
     */
    public UserOperatorMetricsCollectionScheduler(UserOperatorMetricsCollector userOperatorMetricsCollector, String selector) {
        super(selector);
        this.userOperatorMetricsCollector = userOperatorMetricsCollector;
    }

    /**
     * Collects metrics related to the User Operator. This method is called periodically according to the schedule
     * established by {@link #startCollecting(long, long, TimeUnit)}. It delegates the actual metrics collection to
     * the {@link UserOperatorMetricsCollector} instance.
     */
    @Override
    protected void collectMetrics() {
        LOGGER.debug("Collecting metrics with selector: {}", this.selector);
        this.userOperatorMetricsCollector.collectMetricsFromPods(TestConstants.METRICS_COLLECT_TIMEOUT);
        LOGGER.debug("Metrics collected.");
    }

    public Map<Long, Map<String, List<Double>>> getMetricsStore() {
        return metricsStore;
    }

    /**
     * Builds a structured map of metrics data collected by the {@link UserOperatorMetricsCollector}. This map includes
     * various metrics such as total reconciliations, JVM memory usage, CPU usage, and more. The structure and content
     * of the map are dictated by the specific metrics being gathered and the format required for downstream processing
     * and analysis.
     *
     * @return A map where each key represents a specific metric identifier and the value is a list of data points
     *         associated with that metric.
     */
    @Override
    protected Map<String, List<Double>> buildMetricsMap() {
        Map<String, List<Double>> metrics = new HashMap<>();

        // reconciliation metrics
        metrics.put(PerformanceConstants.RECONCILIATIONS_TOTAL, this.userOperatorMetricsCollector.getReconciliationsTotal(this.selector));

        // JVM and system metrics stored
        metrics.put(PerformanceConstants.JVM_GC_MEMORY_ALLOCATED_BYTES_TOTAL, this.userOperatorMetricsCollector.getJvmGcMemoryAllocatedBytesTotal());

        // ALL jvm_memory_used_bytes with labels
        for (Map.Entry<String, Double> entry : this.userOperatorMetricsCollector.getJvmMemoryUsedBytes().entrySet()) {
            metrics.put(entry.getKey(), Collections.singletonList(entry.getValue()));
        }

        metrics.put(PerformanceConstants.JVM_THREADS_LIVE_THREADS, this.userOperatorMetricsCollector.getJvmThreadsLiveThreads());
        metrics.put(PerformanceConstants.SYSTEM_CPU_USAGE, this.userOperatorMetricsCollector.getSystemCpuUsage());
        metrics.put(PerformanceConstants.SYSTEM_CPU_COUNT, this.userOperatorMetricsCollector.getSystemCpuCount());

        // jvm_gc_pause_seconds_max with labels
        for (Map.Entry<String, Double> entry : this.userOperatorMetricsCollector.getJvmGcPauseSecondsMax().entrySet()) {
            metrics.put(entry.getKey(), Collections.singletonList(entry.getValue()));
        }

        metrics.put(PerformanceConstants.JVM_MEMORY_MAX_BYTES, this.userOperatorMetricsCollector.getJvmMemoryMaxBytes());
        metrics.put(PerformanceConstants.PROCESS_CPU_USAGE, this.userOperatorMetricsCollector.getProcessCpuUsage());
        metrics.put(PerformanceConstants.SYSTEM_LOAD_AVERAGE_1M, this.userOperatorMetricsCollector.getSystemLoadAverage1m());

        // derived metrics
        metrics.put(PerformanceConstants.SYSTEM_LOAD_AVERAGE_PER_CORE_PERCENT,
            this.calculateLoadAveragePerCore(
                metrics.get(PerformanceConstants.SYSTEM_LOAD_AVERAGE_1M),
                metrics.get(PerformanceConstants.SYSTEM_CPU_COUNT)
            ));

        // Assuming this.topicOperatorMetricsCollector.getJvmMemoryUsedBytes() returns the map as described
        Map<String, Double> jvmMemoryUsedBytes = this.userOperatorMetricsCollector.getJvmMemoryUsedBytes();

        // Sum all values from the map for a total JVM memory used bytes (but converting to MBs)
        double totalJvmMemoryUsedBytesInMB = jvmMemoryUsedBytes.values().stream()
            .mapToDouble(Double::doubleValue)
            .sum() / 1_000_000; // Convert from bytes to MB

        // Now, put the sum into metrics with a specific key indicating it's the total
        metrics.put(PerformanceConstants.JVM_MEMORY_USED_MEGABYTES_TOTAL, Collections.singletonList(totalJvmMemoryUsedBytesInMB));

        for (Map.Entry<String, List<Double>> entry : metrics.entrySet()) {
            if (!entry.getKey().startsWith("strimzi_")) {
                LOGGER.debug("Metric: " + entry.getKey() + " - Values: " + entry.getValue());
            }
        }

        return metrics;
    }
}
