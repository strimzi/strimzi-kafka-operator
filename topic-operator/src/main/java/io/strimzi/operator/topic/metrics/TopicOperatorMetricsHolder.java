/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.metrics;

import io.micrometer.core.instrument.Timer;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.metrics.MetricsHolder;
import io.strimzi.operator.common.model.Labels;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A metrics holder for the Topic Operator.
 */
public class TopicOperatorMetricsHolder extends MetricsHolder {
    private final Map<String, AtomicInteger> reconciliationsMaxQueueMap = new ConcurrentHashMap<>(1);
    private final Map<String, AtomicInteger> reconciliationsMaxBatchMap = new ConcurrentHashMap<>(1);

    // additional metrics, useful for tuning or monitoring specific internal operations
    private final Map<String, Timer> addFinalizerTimerMap = new ConcurrentHashMap<>(1);
    private final Map<String, Timer> removeFinalizerTimerMap = new ConcurrentHashMap<>(1);
    private final Map<String, Timer> createTopicsTimerMap = new ConcurrentHashMap<>(1);
    private final Map<String, Timer> updateStatusTimerMap = new ConcurrentHashMap<>(1);
    private final Map<String, Timer> listReassignmentsTimerMap = new ConcurrentHashMap<>(1);
    private final Map<String, Timer> alterConfigsTimerMap = new ConcurrentHashMap<>(1);
    private final Map<String, Timer> createPartitionsTimerMap = new ConcurrentHashMap<>(1);
    private final Map<String, Timer> describeTopicsTimerMap = new ConcurrentHashMap<>(1);
    private final Map<String, Timer> describeConfigsTimerMap = new ConcurrentHashMap<>(1);
    private final Map<String, Timer> deleteTopicsTimerMap = new ConcurrentHashMap<>(1);

    /**
     * Constructs the operator metrics holder.
     *
     * @param kind              Kind of the resources for which these metrics apply
     * @param selectorLabels    Selector labels to select the controller resources
     * @param metricsProvider   Topic Operator metrics provider
     */
    public TopicOperatorMetricsHolder(String kind,
                                      Labels selectorLabels,
                                      TopicOperatorMetricsProvider metricsProvider) {
        super(kind, selectorLabels, metricsProvider);
    }

    /**
     * Creates or gets a fine-grained timer-type metric.
     * This can be used to measure the duration of internal operations.
     *
     * @param namespace         Namespace of the resource
     * @param kind              Kind of the resource
     * @param metricName        Name of the metric
     * @param metrics           Metrics provider
     * @param selectorLabels    Selector labels used to filter the resources
     * @param timerMap          Map with timers
     * @param metricHelp        Help description of the metric
     *
     * @return  Timer metric
     */
    private static Timer getFineGrainedTimer(String namespace,
                                               String kind,
                                               String metricName,
                                               MetricsProvider metrics,
                                               Labels selectorLabels,
                                               Map<String, Timer> timerMap,
                                               String metricHelp) {
        return metric(namespace, kind, selectorLabels, timerMap,
            tags -> ((TopicOperatorMetricsProvider) metrics).fineGrainedTimer(metricName, metricHelp, tags));
    }

    /**
     * Gauge metric for the max size recorded for the event queue.
     *
     * @param namespace Namespace of the resources being reconciled
     *
     * @return Metrics gauge
     */
    public AtomicInteger reconciliationsMaxQueueSize(String namespace) {
        return getGauge(namespace, kind, METRICS_PREFIX + "reconciliations.max.queue.size",
            metricsProvider, selectorLabels, reconciliationsMaxQueueMap, "Max size recorded for the shared event queue");
    }

    /**
     * Gauge metric for the max size recorded for the event batch.
     *
     * @param namespace Namespace of the resources being reconciled
     *
     * @return Metrics gauge
     */
    public AtomicInteger reconciliationsMaxBatchSize(String namespace) {
        return getGauge(namespace, kind, METRICS_PREFIX + "reconciliations.max.batch.size",
            metricsProvider, selectorLabels, reconciliationsMaxBatchMap, "Max size recorded for a single event batch");
    }

    /**
     * Timer which measures how long the addFinalizer Kubernetes operations take.
     *
     * @param namespace Namespace of the resources being reconciled
     *
     * @return Metrics timer
     */
    public Timer addFinalizerTimer(String namespace) {
        return getFineGrainedTimer(namespace, kind, METRICS_PREFIX + "add.finalizer.duration", metricsProvider, selectorLabels, addFinalizerTimerMap,
            "The time the addFinalizer Kubernetes operation takes to complete");
    }

    /**
     * Timer which measures how long the removeFinalizer Kubernetes operations take.
     *
     * @param namespace Namespace of the resources being reconciled
     *
     * @return Metrics timer
     */
    public Timer removeFinalizerTimer(String namespace) {
        return getFineGrainedTimer(namespace, kind, METRICS_PREFIX + "remove.finalizer.duration", metricsProvider, selectorLabels, removeFinalizerTimerMap,
            "The time the removeFinalizer Kubernetes operation takes to complete");
    }

    /**
     * Timer which measures how long the createTopics Kafka operations take.
     *
     * @param namespace Namespace of the resources being reconciled
     *
     * @return Metrics timer
     */
    public Timer createTopicsTimer(String namespace) {
        return getFineGrainedTimer(namespace, kind, METRICS_PREFIX + "create.topics.duration", metricsProvider, selectorLabels, createTopicsTimerMap,
            "The time the createTopics Kafka operation takes to complete");
    }

    /**
     * Timer which measures how long the updateStatus Kubernetes operations take.
     *
     * @param namespace Namespace of the resources being reconciled
     *
     * @return Metrics timer
     */
    public Timer updateStatusTimer(String namespace) {
        return getFineGrainedTimer(namespace, kind, METRICS_PREFIX + "update.status.duration", metricsProvider, selectorLabels, updateStatusTimerMap,
            "The time the updateStatus Kubernetes operation takes to complete");
    }

    /**
     * Timer which measures how long the listReassignments Kafka operations take.
     *
     * @param namespace Namespace of the resources being reconciled
     *
     * @return Metrics timer
     */
    public Timer listReassignmentsTimer(String namespace) {
        return getFineGrainedTimer(namespace, kind, METRICS_PREFIX + "list.reassignments.duration", metricsProvider, selectorLabels, listReassignmentsTimerMap,
            "The time the listPartitionReassignments Kafka operation takes to complete");
    }

    /**
     * Timer which measures how long the incrementalAlterConfigs Kafka operations take.
     *
     * @param namespace Namespace of the resources being reconciled
     *
     * @return Metrics timer
     */
    public Timer alterConfigsTimer(String namespace) {
        return getFineGrainedTimer(namespace, kind, METRICS_PREFIX + "alter.configs.duration", metricsProvider, selectorLabels, alterConfigsTimerMap,
            "The time the incrementalAlterConfigs Kafka operation takes to complete");
    }

    /**
     * Timer which measures how long the createPartitions Kafka operations take.
     *
     * @param namespace Namespace of the resources being reconciled
     *
     * @return Metrics timer
     */
    public Timer createPartitionsTimer(String namespace) {
        return getFineGrainedTimer(namespace, kind, METRICS_PREFIX + "create.partitions.duration", metricsProvider, selectorLabels, createPartitionsTimerMap,
            "The time the createPartitions Kafka operation takes to complete");
    }

    /**
     * Timer which measures how long the describeTopics Kafka operations take.
     *
     * @param namespace Namespace of the resources being reconciled
     *
     * @return Metrics timer
     */
    public Timer describeTopicsTimer(String namespace) {
        return getFineGrainedTimer(namespace, kind, METRICS_PREFIX + "describe.topics.duration", metricsProvider, selectorLabels, describeTopicsTimerMap,
            "The time the describeTopics Kafka operation takes to complete");
    }

    /**
     * Timer which measures how long the describeConfigs Kafka operations take.
     *
     * @param namespace Namespace of the resources being reconciled
     *
     * @return Metrics timer
     */
    public Timer describeConfigsTimer(String namespace) {
        return getFineGrainedTimer(namespace, kind, METRICS_PREFIX + "describe.configs.duration", metricsProvider, selectorLabels, describeConfigsTimerMap,
            "The time the describeConfigs Kafka operation takes to complete");
    }

    /**
     * Timer which measures how long the deleteTopics Kafka operations take.
     *
     * @param namespace Namespace of the resources being reconciled
     *
     * @return Metrics timer
     */
    public Timer deleteTopicsTimer(String namespace) {
        return getFineGrainedTimer(namespace, kind, METRICS_PREFIX + "delete.topics.duration", metricsProvider, selectorLabels, deleteTopicsTimerMap,
            "The time the deleteTopics Kafka operation takes to complete");
    }
}
