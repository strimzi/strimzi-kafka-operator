/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.gather.schedulers;

import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.performance.PerformanceConstants;
import io.strimzi.systemtest.performance.gather.collectors.TopicOperatorMetricsCollector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * The TopicOperatorMetricsCollectionScheduler class extends the MetricsCollectionScheduler abstract class,
 * providing specific implementations for collecting and storing metrics related to the topic operator in a Strimzi Kafka environment.
 * This class orchestrates the gathering of various performance metrics from Kafka topic operations, such as creating, updating, and deleting topics.
 *
 * <p>This class utilizes a TopicOperatorMetricsCollector to perform the actual collection of metrics from Kafka topic operations. It manages
 * a scheduled task that periodically triggers metric collection, storing the results along with a timestamp to maintain a history of metrics over time.</p>
 *
 * <p>Metrics collected include durations and counts of various Kafka topic operations, JVM metrics, and system performance data. These metrics
 * are stored in a structured map with timestamps as keys, making it easy to track changes over time.</p>
 *
 * <p>Usage involves creating an instance with a selector string that helps identify the specific topics or resources from which metrics are to be gathered.
 * The scheduler can be started and stopped to control the collection process based on the application's lifecycle.</p>
 */
public class TopicOperatorMetricsCollectionScheduler extends BaseMetricsCollectionScheduler {

    private static final Logger LOGGER = LogManager.getLogger(TopicOperatorMetricsCollectionScheduler.class);
    private final TopicOperatorMetricsCollector topicOperatorMetricsCollector;

    public TopicOperatorMetricsCollectionScheduler(TopicOperatorMetricsCollector topicOperatorMetricsCollector, String selector) {
        super(selector);
        this.topicOperatorMetricsCollector = topicOperatorMetricsCollector;
    }

    @Override
    protected void collectMetrics() {
        LOGGER.debug("Collecting metrics with selector: {}", this.selector);
        this.topicOperatorMetricsCollector.collectMetricsFromPods(TestConstants.METRICS_COLLECT_TIMEOUT);
        LOGGER.debug("Metrics collected.");
    }

    public Map<Long, Map<String, List<Double>>> getMetricsStore() {
        return metricsStore;
    }

    @Override
    protected Map<String, List<Double>> buildMetricsMap() {
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
        // the total time spent on the Topic Operator's event queue. -> (reconciliations_duration - (internal_op0_duration + internal_op1_duration,...)).
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
}
