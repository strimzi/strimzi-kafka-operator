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
 * A metrics holder for operators.
 */
public class OperatorMetricsHolder extends MetricsHolder {
    private final Map<String, Counter> lockedReconciliationsCounterMap = new ConcurrentHashMap<>(1);

    /**
     * Constructs the operator metrics holder
     *
     * @param kind              Kind of the resources for which these metrics apply
     * @param selectorLabels    Selector labels to select the controller resources
     * @param metricsProvider   Metrics provider
     */
    public OperatorMetricsHolder(String kind, Labels selectorLabels, MetricsProvider metricsProvider) {
        super(kind, selectorLabels, metricsProvider);
    }

    /**
     * Counter metric for number of reconciliations which did not happen because they did not get the lock (which means
     * that other reconciliation for the same resource was in progress).
     *
     * @param namespace     Namespace of the resources being reconciled
     *
     * @return  Metrics counter
     */
    public Counter lockedReconciliationsCounter(String namespace) {
        return getCounter(namespace, kind, METRICS_PREFIX + "reconciliations.locked", metricsProvider, selectorLabels, lockedReconciliationsCounterMap,
                "Number of reconciliations skipped because another reconciliation for the same resource was still running");
    }

    /**
     * Resets all values in the resource counter map and paused resource counter map to 0. This is used to handle
     * removed resources from various namespaces during the periodical reconciliation in operators.
     */
    public void resetResourceAndPausedResourceCounters() {
        resourceCounterMap.forEach((key, value) -> value.set(0));
        pausedResourceCounterMap.forEach((key, value) -> value.set(0));
    }
}
