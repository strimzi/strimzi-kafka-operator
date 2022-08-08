/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.metrics;

import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.model.Labels;

/**
 * A metrics holder for operators.
 */
public class OperatorMetricsHolder extends MetricsHolder {


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
     * Resets all values in the resource counter map and paused resource counter map to 0. This is used to handle
     * removed resources from various namespaces during the periodical reconciliation in operators.
     */
    public void resetResourceAndPausedResourceCounters() {
        resourceCounterMap.forEach((key, value) -> value.set(0));
        pausedResourceCounterMap.forEach((key, value) -> value.set(0));
    }
}
