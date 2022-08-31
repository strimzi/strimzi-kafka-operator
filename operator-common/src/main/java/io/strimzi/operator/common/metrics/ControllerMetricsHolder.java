/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.metrics;

import io.micrometer.core.instrument.Counter;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.model.Labels;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A metrics holder for controllers.
 */
public class ControllerMetricsHolder extends MetricsHolder {
    private final Map<String, Counter> alreadyQueuedReconciliationsCounterMap = new ConcurrentHashMap<>(1);

    /**
     * Constructs the controller metrics holder
     *
     * @param kind              Kind of the resources for which these metrics apply
     * @param selectorLabels    Selector labels to select the controller resources
     * @param metricsProvider   Metrics provider
     */
    public ControllerMetricsHolder(String kind, Labels selectorLabels, MetricsProvider metricsProvider) {
        super(kind, selectorLabels, metricsProvider);
    }

    /**
     * Counter metric for number of reconciliations which are already queued when we try to enqueue them again. This
     * might indicate for example that the periodic reconciliations are triggering too often (faster than the operator
     * reconciles them).
     *
     * @param namespace     Namespace of the resources being reconciled
     *
     * @return  Metrics counter
     */
    public Counter alreadyEnqueuedReconciliationsCounter(String namespace) {
        return getCounter(namespace, kind, METRICS_PREFIX + "reconciliations.already.enqueued", metricsProvider, selectorLabels, alreadyQueuedReconciliationsCounterMap,
                "Number of reconciliations skipped because another reconciliation for the same resource was still running");
    }
}
