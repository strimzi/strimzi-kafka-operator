/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.metrics.OperatorMetricsHolder;
import io.strimzi.operator.common.model.Labels;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Util class which holds the different metrics used by operators which deal with connectors
 */
public class ConnectOperatorMetricsHolder extends OperatorMetricsHolder {
    private final Map<String, Counter> connectorsReconciliationsCounterMap = new ConcurrentHashMap<>(1);
    private final Map<String, Counter> connectorsFailedReconciliationsCounterMap = new ConcurrentHashMap<>(1);
    private final Map<String, Counter> connectorsSuccessfulReconciliationsCounterMap = new ConcurrentHashMap<>(1);
    private final Map<String, Timer> connectorsReconciliationsTimerMap = new ConcurrentHashMap<>(1);
    private final Map<String, AtomicInteger> connectorsResourceCounterMap = new ConcurrentHashMap<>(1);
    private final Map<String, AtomicInteger> pausedConnectorsResourceCounterMap = new ConcurrentHashMap<>(1);

    /**
     * Constructs the operator metrics holder for connect operators
     *
     * @param kind              Kind of the resources for which these metrics apply
     * @param selectorLabels    Selector labels to select the controller resources
     * @param metricsProvider   Metrics provider
     */
    public ConnectOperatorMetricsHolder(String kind, Labels selectorLabels, MetricsProvider metricsProvider) {
        super(kind, selectorLabels, metricsProvider);
    }

    /**
     * Counter metric for number of connector reconciliations. Each reconciliation should increment it (i.e. it increments
     * once per resource). This metric is implemented in the AbstractController.
     *
     * @param namespace     Namespace of the resources being reconciled
     *
     * @return  Metrics counter
     */
    public Counter connectorsReconciliationsCounter(String namespace) {
        return getCounter(namespace, KafkaConnector.RESOURCE_KIND, METRICS_PREFIX + "reconciliations", metricsProvider, null, connectorsReconciliationsCounterMap,
                "Number of reconciliations done by the operator for individual resources");
    }

    /**
     * Counter metric for number of failed connector reconciliations.
     *
     * @param namespace     Namespace of the resources being reconciled
     *
     * @return  Metrics counter
     */
    public Counter connectorsFailedReconciliationsCounter(String namespace) {
        return getCounter(namespace, KafkaConnector.RESOURCE_KIND, METRICS_PREFIX + "reconciliations.failed", metricsProvider, null, connectorsFailedReconciliationsCounterMap,
                "Number of reconciliations done by the operator for individual resources which failed");
    }

    /**
     * Counter metric for number of successful connector reconciliations.
     *
     * @param namespace     Namespace of the resources being reconciled
     *
     * @return  Metrics counter
     */
    public Counter connectorsSuccessfulReconciliationsCounter(String namespace) {
        return getCounter(namespace, KafkaConnector.RESOURCE_KIND, METRICS_PREFIX + "reconciliations.successful", metricsProvider, null, connectorsSuccessfulReconciliationsCounterMap,
                "Number of reconciliations done by the operator for individual resources which were successful");
    }

    /**
     * Counter metric for number of connector resources.
     *
     * @param namespace     Namespace of the resources being reconciled
     *
     * @return  Metrics counter
     */
    public AtomicInteger connectorsResourceCounter(String namespace) {
        return getGauge(namespace, KafkaConnector.RESOURCE_KIND, METRICS_PREFIX + "resources",
                metricsProvider, null, connectorsResourceCounterMap,
                "Number of custom resources the operator sees");
    }

    /**
     * Counter metric for number of paused connector resources which are not reconciled.
     *
     * @param namespace     Namespace of the resources being reconciled
     *
     * @return  Metrics counter
     */
    public AtomicInteger pausedConnectorsResourceCounter(String namespace) {
        return getGauge(namespace, KafkaConnector.RESOURCE_KIND, METRICS_PREFIX + "resources.paused",
                metricsProvider, null, pausedConnectorsResourceCounterMap,
                "Number of connectors the connect operator sees but does not reconcile due to paused reconciliations");
    }

    /**
     * Timer which measures how long do the connector reconciliations take.
     *
     * @param namespace     Namespace of the resources being reconciled
     *
     * @return  Metrics timer
     */
    public Timer connectorsReconciliationsTimer(String namespace) {
        return getTimer(namespace, KafkaConnector.RESOURCE_KIND, METRICS_PREFIX + "reconciliations.duration",
                metricsProvider, null, connectorsReconciliationsTimerMap,
                "The time the reconciliation takes to complete");
    }

    /**
     * Resets all values in the connector resource counter map and paused resource counter map to 0. This is used to
     * handle removed connector resources from various namespaces during the periodical reconciliation in operators.
     *
     * @param namespace Namespace for which should the metrics be reset to 0
     */
    public void resetConnectorsCounters(String namespace) {
        if (namespace.equals("*")) {
            connectorsResourceCounterMap.forEach((key, counter) -> counter.set(0));
            pausedConnectorsResourceCounterMap.forEach((key, counter) -> counter.set(0));
        } else {
            connectorsResourceCounter(namespace).set(0);
            pausedConnectorsResourceCounter(namespace).set(0);
        }
    }
}
