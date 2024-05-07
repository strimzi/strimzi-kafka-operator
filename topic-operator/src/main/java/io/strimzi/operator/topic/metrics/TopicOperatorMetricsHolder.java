/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.metrics;

import io.micrometer.core.instrument.Timer;
import io.strimzi.operator.common.metrics.MetricKey;
import io.strimzi.operator.common.metrics.MetricsHolder;
import io.strimzi.operator.common.model.Labels;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A metrics holder for the Topic Operator.
 */
public class TopicOperatorMetricsHolder extends MetricsHolder {
    /**
     * Metric name for reconciliations max queue size.
     */
    public static final String METRICS_RECONCILIATIONS_MAX_QUEUE_SIZE = METRICS_RECONCILIATIONS + ".max.queue.size";
    /**
     * Metric name for reconciliations max batch size.
     */
    public static final String METRICS_RECONCILIATIONS_MAX_BATCH_SIZE = METRICS_RECONCILIATIONS + ".max.batch.size";
    /**
     * Metric name for add finalizer duration.
     */
    public static final String METRICS_ADD_FINALIZER_DURATION = METRICS_PREFIX + "add.finalizer.duration";
    /**
     * Metric name for removing finalizer duration.
     */
    public static final String METRICS_REMOVE_FINALIZER_DURATION = METRICS_PREFIX + "remove.finalizer.duration";
    /**
     * Metric name for create topics duration.
     */
    public static final String METRICS_CREATE_TOPICS_DURATION = METRICS_PREFIX + "create.topics.duration";
    /**
     * Metric name for update status duration.
     */
    public static final String METRICS_UPDATE_TOPICS_DURATION = METRICS_PREFIX + "update.status.duration";
    /**
     * Metric name for list reassignments duration.
     */
    public static final String METRICS_LIST_REASSIGNMENTS_DURATION = METRICS_PREFIX + "list.reassignments.duration";
    /**
     * Metric name for alter configs duration.
     */
    public static final String METRICS_ALTER_CONFIGS_DURATION = METRICS_PREFIX + "alter.configs.duration";
    /**
     * Metric name for create partitions duration.
     */
    public static final String METRICS_CREATE_PARTITIONS_DURATION = METRICS_PREFIX + "create.partitions.duration";
    /**
     * Metric name for describe topics duration.
     */
    public static final String METRICS_DESCRIBE_TOPICS_DURATION = METRICS_PREFIX + "describe.topics.duration";
    /**
     * Metric name for describe configs duration.
     */
    public static final String METRICS_DESCRIBE_CONFIGS_DURATION = METRICS_PREFIX + "describe.configs.duration";
    /**
     * Metric name for delete topics duration.
     */
    public static final String METRICS_DELETE_TOPICS_DURATION = METRICS_PREFIX + "delete.topics.duration";

    private final Map<MetricKey, AtomicInteger> reconciliationsMaxQueueMap = new ConcurrentHashMap<>(1);
    private final Map<MetricKey, AtomicInteger> reconciliationsMaxBatchMap = new ConcurrentHashMap<>(1);

    // additional metrics, useful for tuning or monitoring specific internal operations
    private final Map<MetricKey, Timer> addFinalizerTimerMap = new ConcurrentHashMap<>(1);
    private final Map<MetricKey, Timer> removeFinalizerTimerMap = new ConcurrentHashMap<>(1);
    private final Map<MetricKey, Timer> createTopicsTimerMap = new ConcurrentHashMap<>(1);
    private final Map<MetricKey, Timer> updateStatusTimerMap = new ConcurrentHashMap<>(1);
    private final Map<MetricKey, Timer> listReassignmentsTimerMap = new ConcurrentHashMap<>(1);
    private final Map<MetricKey, Timer> alterConfigsTimerMap = new ConcurrentHashMap<>(1);
    private final Map<MetricKey, Timer> createPartitionsTimerMap = new ConcurrentHashMap<>(1);
    private final Map<MetricKey, Timer> describeTopicsTimerMap = new ConcurrentHashMap<>(1);
    private final Map<MetricKey, Timer> describeConfigsTimerMap = new ConcurrentHashMap<>(1);
    private final Map<MetricKey, Timer> deleteTopicsTimerMap = new ConcurrentHashMap<>(1);

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
     * @param namespace         Namespace of the resources
     * @param metricName        Name of the metric
     * @param metricHelp        Help description of the metric
     * @param selectorLabels    Selector labels to select the controller resources
     * @param timerMap          Map with timers
     *
     * @return  Timer metric
     */
    private Timer getFineGrainedTimer(String namespace, String metricName, String metricHelp, Optional<String> selectorLabels,
                                      Map<MetricKey, Timer> timerMap) {
        return metric(new MetricKey(kind, namespace), selectorLabels, timerMap,
                tags -> ((TopicOperatorMetricsProvider) metricsProvider).fineGrainedTimer(metricName, metricHelp, tags));
    }

    /**
     * Gauge metric for the max size recorded for the event queue.
     *
     * @param namespace Namespace of the resources being reconciled
     *
     * @return Metrics gauge
     */
    public AtomicInteger reconciliationsMaxQueueSize(String namespace) {
        return getGauge(new MetricKey(kind, namespace), METRICS_RECONCILIATIONS_MAX_QUEUE_SIZE,
                "Max size recorded for the shared event queue",
                Optional.of(getLabelSelectorValues()), reconciliationsMaxQueueMap);
    }

    /**
     * Gauge metric for the max size recorded for the event batch.
     *
     * @param namespace Namespace of the resources being reconciled
     *
     * @return Metrics gauge
     */
    public AtomicInteger reconciliationsMaxBatchSize(String namespace) {
        return getGauge(new MetricKey(kind, namespace), METRICS_RECONCILIATIONS_MAX_BATCH_SIZE,
                "Max size recorded for a single event batch",
                Optional.of(getLabelSelectorValues()), reconciliationsMaxBatchMap);
    }

    /**
     * Timer which measures how long the addFinalizer Kubernetes operations take.
     *
     * @param namespace Namespace of the resources being reconciled
     *
     * @return Metrics timer
     */
    public Timer addFinalizerTimer(String namespace) {
        return getFineGrainedTimer(namespace, METRICS_ADD_FINALIZER_DURATION,
            "The time the addFinalizer Kubernetes operation takes to complete",
                Optional.of(getLabelSelectorValues()), addFinalizerTimerMap);
    }

    /**
     * Timer which measures how long the removeFinalizer Kubernetes operations take.
     *
     * @param namespace Namespace of the resources being reconciled
     *
     * @return Metrics timer
     */
    public Timer removeFinalizerTimer(String namespace) {
        return getFineGrainedTimer(namespace, METRICS_REMOVE_FINALIZER_DURATION,
            "The time the removeFinalizer Kubernetes operation takes to complete",
                Optional.of(getLabelSelectorValues()), removeFinalizerTimerMap);
    }

    /**
     * Timer which measures how long the createTopics Kafka operations take.
     *
     * @param namespace Namespace of the resources being reconciled
     *
     * @return Metrics timer
     */
    public Timer createTopicsTimer(String namespace) {
        return getFineGrainedTimer(namespace, METRICS_CREATE_TOPICS_DURATION,
            "The time the createTopics Kafka operation takes to complete",
                Optional.of(getLabelSelectorValues()), createTopicsTimerMap);
    }

    /**
     * Timer which measures how long the updateStatus Kubernetes operations take.
     *
     * @param namespace Namespace of the resources being reconciled
     *
     * @return Metrics timer
     */
    public Timer updateStatusTimer(String namespace) {
        return getFineGrainedTimer(namespace, METRICS_UPDATE_TOPICS_DURATION,
            "The time the updateStatus Kubernetes operation takes to complete",
                Optional.of(getLabelSelectorValues()), updateStatusTimerMap);
    }

    /**
     * Timer which measures how long the listReassignments Kafka operations take.
     *
     * @param namespace Namespace of the resources being reconciled
     *
     * @return Metrics timer
     */
    public Timer listReassignmentsTimer(String namespace) {
        return getFineGrainedTimer(namespace, METRICS_LIST_REASSIGNMENTS_DURATION,
            "The time the listPartitionReassignments Kafka operation takes to complete",
                Optional.of(getLabelSelectorValues()), listReassignmentsTimerMap);
    }

    /**
     * Timer which measures how long the incrementalAlterConfigs Kafka operations take.
     *
     * @param namespace Namespace of the resources being reconciled
     *
     * @return Metrics timer
     */
    public Timer alterConfigsTimer(String namespace) {
        return getFineGrainedTimer(namespace, METRICS_ALTER_CONFIGS_DURATION,
            "The time the incrementalAlterConfigs Kafka operation takes to complete",
                Optional.of(getLabelSelectorValues()), alterConfigsTimerMap);
    }

    /**
     * Timer which measures how long the createPartitions Kafka operations take.
     *
     * @param namespace Namespace of the resources being reconciled
     *
     * @return Metrics timer
     */
    public Timer createPartitionsTimer(String namespace) {
        return getFineGrainedTimer(namespace, METRICS_CREATE_PARTITIONS_DURATION,
            "The time the createPartitions Kafka operation takes to complete",
                Optional.of(getLabelSelectorValues()), createPartitionsTimerMap);
    }

    /**
     * Timer which measures how long the describeTopics Kafka operations take.
     *
     * @param namespace Namespace of the resources being reconciled
     *
     * @return Metrics timer
     */
    public Timer describeTopicsTimer(String namespace) {
        return getFineGrainedTimer(namespace, METRICS_DESCRIBE_TOPICS_DURATION,
            "The time the describeTopics Kafka operation takes to complete",
                Optional.of(getLabelSelectorValues()), describeTopicsTimerMap);
    }

    /**
     * Timer which measures how long the describeConfigs Kafka operations take.
     *
     * @param namespace Namespace of the resources being reconciled
     *
     * @return Metrics timer
     */
    public Timer describeConfigsTimer(String namespace) {
        return getFineGrainedTimer(namespace, METRICS_DESCRIBE_CONFIGS_DURATION,
            "The time the describeConfigs Kafka operation takes to complete",
                Optional.of(getLabelSelectorValues()), describeConfigsTimerMap);
    }

    /**
     * Timer which measures how long the deleteTopics Kafka operations take.
     *
     * @param namespace Namespace of the resources being reconciled
     *
     * @return Metrics timer
     */
    public Timer deleteTopicsTimer(String namespace) {
        return getFineGrainedTimer(namespace, METRICS_DELETE_TOPICS_DURATION,
            "The time the deleteTopics Kafka operation takes to complete",
                Optional.of(getLabelSelectorValues()), deleteTopicsTimerMap);
    }
}