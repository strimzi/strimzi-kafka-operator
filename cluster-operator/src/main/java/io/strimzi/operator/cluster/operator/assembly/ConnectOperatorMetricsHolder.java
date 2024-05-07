/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.config.ConfigParameter;
import io.strimzi.operator.common.metrics.MetricKey;
import io.strimzi.operator.common.metrics.OperatorMetricsHolder;
import io.strimzi.operator.common.model.Labels;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Util class which holds the different metrics used by operators which deal with connectors
 */
public class ConnectOperatorMetricsHolder extends OperatorMetricsHolder {
    /**
     * Metric name for auto restarts.
     */
    public static final String METRIC_AUTO_RESTARTS = METRICS_PREFIX + "auto.restarts";

    private final Map<MetricKey, Counter> connectorsReconciliationsCounterMap = new ConcurrentHashMap<>(1);
    private final Map<MetricKey, Counter> connectorsFailedReconciliationsCounterMap = new ConcurrentHashMap<>(1);
    private final Map<MetricKey, Counter> connectorsSuccessfulReconciliationsCounterMap = new ConcurrentHashMap<>(1);
    private final Map<MetricKey, Counter> connectorsAutoRestartsCounterMap = new ConcurrentHashMap<>(1);
    private final Map<MetricKey, Timer> connectorsReconciliationsTimerMap = new ConcurrentHashMap<>(1);
    private final Map<MetricKey, AtomicInteger> connectorsResourceCounterMap = new ConcurrentHashMap<>(1);
    private final Map<MetricKey, AtomicInteger> pausedConnectorsResourceCounterMap = new ConcurrentHashMap<>(1);

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
        return getCounter(new MetricKey(KafkaConnector.RESOURCE_KIND, namespace), METRICS_RECONCILIATIONS,
                "Number of reconciliations done by the operator for individual resources",
                Optional.of(getLabelSelectorValues()), connectorsReconciliationsCounterMap);
    }

    /**
     * Counter metric for number of failed connector reconciliations.
     *
     * @param namespace     Namespace of the resources being reconciled
     *
     * @return  Metrics counter
     */
    public Counter connectorsFailedReconciliationsCounter(String namespace) {
        return getCounter(new MetricKey(KafkaConnector.RESOURCE_KIND, namespace), METRICS_RECONCILIATIONS,
                "Number of reconciliations done by the operator for individual resources which failed",
                Optional.of(getLabelSelectorValues()), connectorsFailedReconciliationsCounterMap);
    }

    /**
     * Counter metric for number of successful connector reconciliations.
     *
     * @param namespace     Namespace of the resources being reconciled
     *
     * @return  Metrics counter
     */
    public Counter connectorsSuccessfulReconciliationsCounter(String namespace) {
        return getCounter(new MetricKey(KafkaConnector.RESOURCE_KIND, namespace), METRICS_RECONCILIATIONS_SUCCESSFUL,
                "Number of reconciliations done by the operator for individual resources which were successful",
                Optional.of(getLabelSelectorValues()), connectorsSuccessfulReconciliationsCounterMap);
    }

    /**
     * Counter metric for number of auto restarts.
     *
     * @param namespace     Namespace of the resources being reconciled
     *
     * @return  Metrics counter
     */
    public Counter connectorsAutoRestartsCounter(String namespace) {
        return getCounter(new MetricKey(KafkaConnector.RESOURCE_KIND, namespace), METRIC_AUTO_RESTARTS,
                "Number of auto restarts of the connector",
                Optional.of(getLabelSelectorValues()), connectorsAutoRestartsCounterMap);
    }

    /**
     * Counter metric for number of connector resources.
     *
     * @param namespace     Namespace of the resources being reconciled
     *
     * @return  Metrics counter
     */
    public AtomicInteger connectorsResourceCounter(String namespace) {
        return getGauge(new MetricKey(KafkaConnector.RESOURCE_KIND, namespace), METRICS_RESOURCES,
                "Number of custom resources the operator sees",
                Optional.of(getLabelSelectorValues()), connectorsResourceCounterMap);
    }

    /**
     * Counter metric for number of paused connector resources which are not reconciled.
     *
     * @param namespace     Namespace of the resources being reconciled
     *
     * @return  Metrics counter
     */
    public AtomicInteger pausedConnectorsResourceCounter(String namespace) {
        return getGauge(new MetricKey(KafkaConnector.RESOURCE_KIND, namespace), METRICS_RESOURCES_PAUSED,
                "Number of connectors the connect operator sees but does not reconcile due to paused reconciliations",
                Optional.of(getLabelSelectorValues()), pausedConnectorsResourceCounterMap);
    }

    /**
     * Timer which measures how long do the connector reconciliations take.
     *
     * @param namespace     Namespace of the resources being reconciled
     *
     * @return  Metrics timer
     */
    public Timer connectorsReconciliationsTimer(String namespace) {
        return getTimer(new MetricKey(KafkaConnector.RESOURCE_KIND, namespace), METRICS_RECONCILIATIONS_DURATION,
                "The time the reconciliation takes to complete",
                Optional.of(getLabelSelectorValues()), connectorsReconciliationsTimerMap);
    }

    /**
     * Resets all values in the connector resource counter map and paused resource counter map to 0. This is used to
     * handle removed connector resources from various namespaces during the periodical reconciliation in operators.
     *
     * @param namespace Namespace for which should the metrics be reset to 0
     */
    public void resetConnectorsCounters(String namespace) {
        if (namespace.equals(ConfigParameter.ANY_NAMESPACE)) {
            connectorsResourceCounterMap.forEach((key, counter) -> counter.set(0));
            pausedConnectorsResourceCounterMap.forEach((key, counter) -> counter.set(0));
        } else {
            connectorsResourceCounter(namespace).set(0);
            pausedConnectorsResourceCounter(namespace).set(0);
        }
    }
}
