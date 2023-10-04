/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.metrics;

import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.model.Labels;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A metrics holder for batch operators.
 */
public class BatchOperatorMetricsHolder extends MetricsHolder {
    private final Map<String, AtomicInteger> reconciliationsMaxQueueMap = new ConcurrentHashMap<>(1);
    private final Map<String, AtomicInteger> reconciliationsMaxBatchMap = new ConcurrentHashMap<>(1);

    /**
     * Constructs the operator metrics holder
     *
     * @param kind              Kind of the resources for which these metrics apply
     * @param selectorLabels    Selector labels to select the controller resources
     * @param metricsProvider   Metrics provider
     */
    public BatchOperatorMetricsHolder(String kind, Labels selectorLabels, MetricsProvider metricsProvider) {
        super(kind, selectorLabels, metricsProvider);
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
}
