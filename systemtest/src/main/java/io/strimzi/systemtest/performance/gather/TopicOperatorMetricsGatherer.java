/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.gather;

import io.strimzi.systemtest.performance.PerformanceConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * This class represents a polling for collecting metrics related to the Topic Operator.
 * It periodically collects metrics from pods based on a given selector and maintains a history
 * of these metrics, indexed by timestamp. The metrics history is stored in a TreeMap to preserve
 * the temporal order of the data.
 *
 * <p>Note: This class is designed to be run as a single thread because it uses a TreeMap to store
 * metrics history. TreeMap is not thread-safe, and concurrent modifications from multiple threads
 * can result in undefined behavior. Therefore, instances of this class should not be shared across
 * multiple threads.</p>
 */
public class TopicOperatorMetricsGatherer {

    private static final Logger LOGGER = LogManager.getLogger(TopicOperatorMetricsGatherer.class);
    private final TopicOperatorMetricsCollector topicOperatorMetricsCollector;
    private final String selector;
    // make sure that order of metrics with timestamp
    private final Map<Long, Map<String, List<Double>>> metricsHistory = new TreeMap<>();
    private ScheduledExecutorService scheduler;

    public TopicOperatorMetricsGatherer(TopicOperatorMetricsCollector topicOperatorMetricsCollector, String selector) {
        this.topicOperatorMetricsCollector = topicOperatorMetricsCollector;
        this.selector = selector;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    public void startCollecting() {
        final Runnable task = this::collectMetrics;
        this.scheduler.scheduleAtFixedRate(task, 0, 5, TimeUnit.SECONDS);
    }

    public void stopCollecting() {
        if (this.scheduler != null) {
            this.scheduler.shutdownNow();
            try {
                if (!this.scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOGGER.error("Scheduler did not terminate in the specified time.");
                }
            } catch (InterruptedException e) {
                LOGGER.error("Interrupted during shutdown.", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    private void collectMetrics() {
        LOGGER.info("Collecting metrics with selector: {}", this.selector);

        this.topicOperatorMetricsCollector.collectMetricsFromPods();
        // record specific time when metrics were collected
        Long timeWhenMetricsWereCollected = System.currentTimeMillis();

        Map<String, List<Double>> currentMetrics = this.buildMetricsMap();

        // Store metrics with current timestamp as key
        this.metricsHistory.put(timeWhenMetricsWereCollected, currentMetrics);

        this.printCurrentMetrics();

        LOGGER.debug("Collected metrics:\n{}", currentMetrics.toString());
    }

    public Map<Long, Map<String, List<Double>>> getMetricsHistory() {
        return metricsHistory;
    }

    private void printCurrentMetrics() {
        this.metricsHistory.forEach((key, valueList) -> {
            if (valueList.isEmpty()) {
                LOGGER.debug(key + " => [No data]");
            } else {
                LOGGER.debug(key + " => " + valueList);
            }
        });
    }

    private Map<String, List<Double>> buildMetricsMap() {
        Map<String, List<Double>> metrics = new HashMap<>();

        metrics.put(PerformanceConstants.ALTER_CONFIGS_DURATION_SECONDS_SUM, this.topicOperatorMetricsCollector.getAlterConfigsDurationSecondsSum(this.selector));
        metrics.put(PerformanceConstants.REMOVE_FINALIZER_DURATION_SECONDS_SUM, this.topicOperatorMetricsCollector.getRemoveFinalizerDurationSecondsSum(this.selector));
        metrics.put(PerformanceConstants.RECONCILIATIONS_DURATION_SECONDS_SUM, this.topicOperatorMetricsCollector.getReconciliationsDurationSecondsSum(this.selector));
        metrics.put(PerformanceConstants.CREATE_TOPICS_DURATION_SECONDS_SUM, this.topicOperatorMetricsCollector.getCreateTopicsDurationSecondsSum(this.selector));
        metrics.put(PerformanceConstants.DESCRIBE_TOPICS_DURATION_SECONDS_SUM, this.topicOperatorMetricsCollector.getDescribeTopicsDurationSecondsSum(this.selector));
        metrics.put(PerformanceConstants.CREATE_PARTITIONS_DURATION_SECONDS_SUM, this.topicOperatorMetricsCollector.getCreatePartitionsDurationSecondsSum(this.selector));
        metrics.put(PerformanceConstants.ADD_FINALIZER_DURATION_SECONDS_SUM, this.topicOperatorMetricsCollector.getAddFinalizerDurationSecondsSum(this.selector));
        metrics.put(PerformanceConstants.UPDATE_STATUS_DURATION_SECONDS_SUM, this.topicOperatorMetricsCollector.getUpdateStatusDurationSecondsSum(this.selector));
        metrics.put(PerformanceConstants.DESCRIBE_CONFIGS_DURATION_SECONDS_SUM, this.topicOperatorMetricsCollector.getDescribeConfigsDurationSecondsSum(this.selector));
        metrics.put(PerformanceConstants.DELETE_TOPICS_DURATION_SECONDS_SUM, this.topicOperatorMetricsCollector.getDeleteTopicsDurationSecondsSum(this.selector));

        metrics.put(PerformanceConstants.RECONCILIATIONS_DURATION_SECONDS_MAX, this.topicOperatorMetricsCollector.getReconciliationsDurationSecondsMax(this.selector));
        metrics.put(PerformanceConstants.RECONCILIATIONS_MAX_QUEUE_SIZE, this.topicOperatorMetricsCollector.getReconciliationsMaxQueueSize(this.selector));
        metrics.put(PerformanceConstants.RECONCILIATIONS_MAX_BATCH_SIZE, this.topicOperatorMetricsCollector.getReconciliationsMaxBatchSize(this.selector));
        metrics.put(PerformanceConstants.RECONCILIATIONS_SUCCESSFUL_TOTAL, this.topicOperatorMetricsCollector.getReconciliationsSuccessfulTotal(this.selector));
        metrics.put(PerformanceConstants.RECONCILIATIONS_TOTAL, this.topicOperatorMetricsCollector.getReconciliationsTotal(this.selector));
        metrics.put(PerformanceConstants.RECONCILIATIONS_FAILED_TOTAL, this.topicOperatorMetricsCollector.getReconciliationsFailedTotal(this.selector));
        metrics.put(PerformanceConstants.RECONCILIATIONS_LOCKED_TOTAL, this.topicOperatorMetricsCollector.getReconciliationsLockedTotal(this.selector));

        // Operational Metrics (i.e., creation, delete, update, describe...)
        metrics.put(PerformanceConstants.CREATE_TOPICS_DURATION_SECONDS_MAX, this.topicOperatorMetricsCollector.getCreateTopicsDurationSecondsMax(this.selector));
        metrics.put(PerformanceConstants.DELETE_TOPICS_DURATION_SECONDS_MAX, this.topicOperatorMetricsCollector.getDeleteTopicsDurationSecondsMax(this.selector));
        metrics.put(PerformanceConstants.UPDATE_STATUS_DURATION_SECONDS_MAX, this.topicOperatorMetricsCollector.getUpdateStatusDurationSecondsMax(this.selector));
        metrics.put(PerformanceConstants.DESCRIBE_TOPICS_DURATION_SECONDS_MAX, this.topicOperatorMetricsCollector.getDescribeTopicsDurationSecondsMax(this.selector));
        metrics.put(PerformanceConstants.ALTER_CONFIGS_DURATION_SECONDS_MAX, this.topicOperatorMetricsCollector.getAlterConfigsDurationSecondsMax(this.selector));
        metrics.put(PerformanceConstants.DESCRIBE_CONFIGS_DURATION_SECONDS_MAX, this.topicOperatorMetricsCollector.getDescribeConfigsDurationSecondsMax(this.selector));

        // Resource Metrics
        metrics.put(PerformanceConstants.RESOURCES, this.topicOperatorMetricsCollector.getResourcesKafkaTopics(this.selector));

        // Finalizers Metrics
        metrics.put(PerformanceConstants.ADD_FINALIZER_DURATION_SECONDS_MAX, this.topicOperatorMetricsCollector.getAddFinalizerDurationSecondsMax(this.selector));
        metrics.put(PerformanceConstants.REMOVE_FINALIZER_DURATION_SECONDS_MAX, this.topicOperatorMetricsCollector.getRemoveFinalizerDurationSecondsMax(this.selector));

        // JVM and system metrics stored
        metrics.put(PerformanceConstants.JVM_GC_MEMORY_ALLOCATED_BYTES_TOTAL, this.topicOperatorMetricsCollector.getJvmGcMemoryAllocatedBytesTotal());

        // ALL jvm_memory_used_bytes with labels
        for (Map.Entry<String, Double> entry : this.topicOperatorMetricsCollector.getJvmMemoryUsedBytes().entrySet()) {
            metrics.put(entry.getKey(), Collections.singletonList(entry.getValue()));
        }

        metrics.put(PerformanceConstants.JVM_THREADS_LIVE_THREADS, this.topicOperatorMetricsCollector.getJvmThreadsLiveThreads());
        metrics.put(PerformanceConstants.SYSTEM_CPU_USAGE, this.topicOperatorMetricsCollector.getSystemCpuUsage());
        metrics.put(PerformanceConstants.SYSTEM_CPU_COUNT, this.topicOperatorMetricsCollector.getSystemCpuCount());

        // jvm_gc_pause_seconds_max with labels
        for (Map.Entry<String, Double> entry : this.topicOperatorMetricsCollector.getJvmGcPauseSecondsMax().entrySet()) {
            metrics.put(entry.getKey(), Collections.singletonList(entry.getValue()));
        }

        metrics.put(PerformanceConstants.JVM_MEMORY_MAX_BYTES, this.topicOperatorMetricsCollector.getJvmMemoryMaxBytes());
        metrics.put(PerformanceConstants.PROCESS_CPU_USAGE, this.topicOperatorMetricsCollector.getProcessCpuUsage());
        metrics.put(PerformanceConstants.SYSTEM_LOAD_AVERAGE_1M, this.topicOperatorMetricsCollector.getSystemLoadAverage1m());

        // derived Metrics
        // the total time spent on the UTO's event queue. -> (reconciliations_duration - (internal_op0_duration + internal_op1_duration,...)).
        metrics.put(PerformanceConstants.TOTAL_TIME_SPEND_ON_UTO_EVENT_QUEUE_DURATION_SECONDS,
            this.calculateTotalTimeSpendOnToEventQueueDurationSeconds(this.selector));
        metrics.put(PerformanceConstants.SYSTEM_LOAD_AVERAGE_PER_CORE_PERCENT,
            this.calculateLoadAveragePerCore(
                metrics.get(PerformanceConstants.SYSTEM_LOAD_AVERAGE_1M),
                metrics.get(PerformanceConstants.SYSTEM_CPU_COUNT)
            ));
        // Assuming this.topicOperatorMetricsCollector.getJvmMemoryUsedBytes() returns the map as described
        Map<String, Double> jvmMemoryUsedBytes = this.topicOperatorMetricsCollector.getJvmMemoryUsedBytes();

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

    private List<Double> calculateTotalTimeSpendOnToEventQueueDurationSeconds(String selector) {
        // Assuming getReconciliationsDurationSecondsSum returns the total reconciliations_duration
        Double reconciliationsDuration = this.topicOperatorMetricsCollector.getReconciliationsDurationSecondsSum(selector).stream().mapToDouble(Double::doubleValue).sum();

        // Sum of durations for all listed internal operations
        Double sumInternalOperationsDuration = Stream.of(
                this.topicOperatorMetricsCollector.getAlterConfigsDurationSecondsSum(selector),
                this.topicOperatorMetricsCollector.getRemoveFinalizerDurationSecondsSum(selector),
                this.topicOperatorMetricsCollector.getCreateTopicsDurationSecondsSum(selector),
                this.topicOperatorMetricsCollector.getDescribeTopicsDurationSecondsSum(selector),
                this.topicOperatorMetricsCollector.getCreatePartitionsDurationSecondsSum(selector),
                this.topicOperatorMetricsCollector.getAddFinalizerDurationSecondsSum(selector),
                this.topicOperatorMetricsCollector.getUpdateStatusDurationSecondsSum(selector),
                this.topicOperatorMetricsCollector.getDescribeConfigsDurationSecondsSum(selector),
                this.topicOperatorMetricsCollector.getDeleteTopicsDurationSecondsSum(selector)
        ).flatMap(List::stream).mapToDouble(Double::doubleValue).sum();

        // Calculate the total time spent on UTO's event queue
        double totalTimeSpentOnUtoEventQueue = reconciliationsDuration - sumInternalOperationsDuration;

        // Return this as a List<Double>
        return Collections.singletonList(totalTimeSpentOnUtoEventQueue);
    }

    private List<Double> calculateLoadAveragePerCore(List<Double> systemLoadAverage1m, List<Double> systemCpuCount) {
        // Ensure the lists are not empty to avoid IndexOutOfBoundsException
        if (!systemLoadAverage1m.isEmpty() && !systemCpuCount.isEmpty()) {
            double loadAverage = systemLoadAverage1m.get(0);
            double cpuCount = systemCpuCount.get(0);

            // Calculate load average per core
            double loadAveragePerCore = loadAverage / cpuCount;

            // Return this as a List<Double>
            return Collections.singletonList(loadAveragePerCore);
        } else {
            return Collections.emptyList();
        }
    }
}
