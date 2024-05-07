/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.model.Labels;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Abstract base class holding common metrics used by operators and controllers.
 * Subclasses can add more specialized metrics.
 */
public abstract class MetricsHolder {
    /**
     * Prefix used for metrics provided by Strimzi operators
     */
    public static final String METRICS_PREFIX = "strimzi.";
    /**
     * Metric name for number of reconciliations.
     */
    public static final String METRICS_RECONCILIATIONS = METRICS_PREFIX + "reconciliations";
    /**
     * Metric name for number of periodic reconciliations.
     */
    public static final String METRICS_RECONCILIATIONS_PERIODICAL = METRICS_RECONCILIATIONS + ".periodical";
    /**
     * Metric name for number of failed reconciliations.
     */
    public static final String METRICS_RECONCILIATIONS_FAILED = METRICS_RECONCILIATIONS + ".failed";
    /**
     * Metric name for number of successful reconciliations.
     */
    public static final String METRICS_RECONCILIATIONS_SUCCESSFUL = METRICS_RECONCILIATIONS + ".successful";
    /**
     * Metric name for duration of reconciliations.
     */
    public static final String METRICS_RECONCILIATIONS_DURATION = METRICS_RECONCILIATIONS + ".duration";
    /**
     * Metric name for number of locked reconciliations.
     */
    public static final String METRICS_RECONCILIATIONS_LOCKED = METRICS_RECONCILIATIONS + ".locked";
    /**
     * Metric name for number of resources managed by the operator.
     */
    public static final String METRICS_RESOURCES = METRICS_PREFIX + "resources";
    /**
     * Metric name for the resource state.
     */
    public static final String METRICS_RESOURCE_STATE = METRICS_PREFIX + "resource.state";
    /**
     * Metric name for number of paused resources.
     */
    public static final String METRICS_RESOURCES_PAUSED = METRICS_RESOURCES + ".paused";

    protected final String kind;
    protected final Labels selectorLabels;
    protected final MetricsProvider metricsProvider;

    protected final Map<MetricKey, AtomicInteger> resourceCounterMap = new ConcurrentHashMap<>(1);
    protected final Map<MetricKey, AtomicInteger> pausedResourceCounterMap = new ConcurrentHashMap<>(1);
    private final Map<MetricKey, Counter> periodicReconciliationsCounterMap = new ConcurrentHashMap<>(1);
    private final Map<MetricKey, Counter> reconciliationsCounterMap = new ConcurrentHashMap<>(1);
    private final Map<MetricKey, Counter> failedReconciliationsCounterMap = new ConcurrentHashMap<>(1);
    private final Map<MetricKey, Counter> successfulReconciliationsCounterMap = new ConcurrentHashMap<>(1);
    private final Map<MetricKey, Counter> lockedReconciliationsCounterMap = new ConcurrentHashMap<>(1);
    private final Map<MetricKey, Timer> reconciliationsTimerMap = new ConcurrentHashMap<>(1);

    /**
     * Constructs the metrics holder
     *
     * @param kind              Kind of the resources for which these metrics apply
     * @param selectorLabels    Selector labels to select the controller resources
     * @param metricsProvider   Metrics provider
     */
    public MetricsHolder(String kind, Labels selectorLabels, MetricsProvider metricsProvider) {
        this.kind = kind;
        this.selectorLabels = selectorLabels;
        this.metricsProvider = metricsProvider;
    }

    /**
     * Metrics provider used for the metrics by this holder class
     *
     * @return  Metrics provider
     */
    public MetricsProvider metricsProvider()    {
        return metricsProvider;
    }

    /**
     * Removing metric based on filters metricName and expectedTags.
     * Tags will be matched against the tags of the metric. If all tags are present and have the same values.
     * If expected tag for namespace is '*', then any value will be accepted.
     *
     * @param metricName    Name of the metric to remove
     * @param expectedTags  Tags of the metric to remove
     * @return  true if the metric was removed, false otherwise
     */
    public boolean removeMetric(String metricName, Tags expectedTags) {
        Optional<Meter> maybeMetric = metricsProvider()
                .meterRegistry()
                .getMeters()
                .stream()
                .filter(meter -> {
                    if (!MetricsUtils.isMatchingMetricName(meter, metricName)) {
                        return false;
                    }

                    return MetricsUtils.isMatchingMetricTags(
                            new HashSet<>(meter.getId().getTags()),
                            expectedTags.stream().collect(Collectors.toSet()));
                })
                .findFirst();

        if (maybeMetric.isPresent()) {
            metricsProvider().meterRegistry().remove(maybeMetric.get());
            return true;
        }

        return false;
    }

    ////////////////////
    // Methods for individual counters
    ////////////////////

    /**
     * Counter metric for number of periodic reconciliations. It should be incremented only once per timer-trigger. It
     * should not be incremented for every resource found by the periodical reconciliation.
     *
     * @param namespace     Namespace of the resources being reconciled
     *
     * @return  Metrics counter
     */
    public Counter periodicReconciliationsCounter(String namespace) {
        return getCounter(new MetricKey(kind, namespace), METRICS_RECONCILIATIONS_PERIODICAL,
                "Number of periodical reconciliations done by the operator",
                Optional.of(getLabelSelectorValues()), periodicReconciliationsCounterMap);
    }

    /**
     * Counter metric for number of reconciliations. Each reconciliation should increment it (i.e. it increments once
     * per resource).
     *
     * @param namespace     Namespace of the resources being reconciled
     *
     * @return  Metrics counter
     */
    public Counter reconciliationsCounter(String namespace) {
        return getCounter(new MetricKey(kind, namespace), METRICS_RECONCILIATIONS,
                "Number of reconciliations done by the operator for individual resources",
                Optional.of(getLabelSelectorValues()), reconciliationsCounterMap);
    }

    /**
     * Counter metric for number of failed reconciliations.
     *
     * @param namespace     Namespace of the resources being reconciled
     *
     * @return  Metrics counter
     */
    public Counter failedReconciliationsCounter(String namespace) {
        return getCounter(new MetricKey(kind, namespace), METRICS_RECONCILIATIONS_FAILED,
                "Number of reconciliations done by the operator for individual resources which failed",
                Optional.of(getLabelSelectorValues()), failedReconciliationsCounterMap);
    }

    /**
     * Counter metric for number of successful reconciliations.
     *
     * @param namespace     Namespace of the resources being reconciled
     *
     * @return  Metrics counter
     */
    public Counter successfulReconciliationsCounter(String namespace) {
        return getCounter(new MetricKey(kind, namespace), METRICS_RECONCILIATIONS_SUCCESSFUL,
                "Number of reconciliations done by the operator for individual resources which were successful",
                Optional.of(getLabelSelectorValues()), successfulReconciliationsCounterMap);
    }

    /**
     * Timer which measures how long do the reconciliations take.
     *
     * @param namespace     Namespace of the resources being reconciled
     *
     * @return  Metrics timer
     */
    public Timer reconciliationsTimer(String namespace) {
        return getTimer(new MetricKey(kind, namespace), METRICS_RECONCILIATIONS_DURATION,
                "The time the reconciliation takes to complete",
                Optional.of(getLabelSelectorValues()), reconciliationsTimerMap);
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
        return getCounter(new MetricKey(kind, namespace), METRICS_RECONCILIATIONS_LOCKED,
                "Number of reconciliations skipped because another reconciliation for the same resource was still running",
                Optional.of(getLabelSelectorValues()), lockedReconciliationsCounterMap);
    }

    /**
     * Counter metric for number of resources managed by this operator.
     *
     * @param namespace     Namespace of the resources being reconciled
     *
     * @return  Metrics counter
     */
    public AtomicInteger resourceCounter(String namespace) {
        return getGauge(new MetricKey(kind, namespace), METRICS_RESOURCES,
                "Number of custom resources the operator sees",
                Optional.of(getLabelSelectorValues()), resourceCounterMap);
    }

    /**
     * Counter metric for number of paused resources which are not reconciled.
     *
     * @param namespace     Namespace of the resources being reconciled
     *
     * @return  Metrics counter
     */
    public AtomicInteger pausedResourceCounter(String namespace) {
        return getGauge(new MetricKey(kind, namespace), METRICS_RESOURCES_PAUSED,
                "Number of custom resources the operator sees but does not reconcile due to paused reconciliations",
                Optional.of(getLabelSelectorValues()), pausedResourceCounterMap);
    }

    ////////////////////
    // Static methods for handling metrics
    ////////////////////

    /**
     * Utility method which gets or creates the metric.
     *
     * @param metricKey         Key of the metric
     * @param selectorLabels    Selector labels to select the controller resources
     * @param metricMap         The map with the metrics
     * @param fn                Method fo generating the metrics tags
     * @param optionalTags      Optional tags to be added to the metric
     *
     * @return  Metric
     *
     * @param <M>   Type of the metric
     */
    protected <M> M metric(MetricKey metricKey, Optional<String> selectorLabels, Map<MetricKey, M> metricMap, Function<Tags, M> fn, Tag... optionalTags) {
        Tags metricTags = MetricsUtils.getAllMetricTags(metricKey.getNamespace(), metricKey.getKind(), selectorLabels, optionalTags);
        return metricMap.computeIfAbsent(metricKey, k -> fn.apply(metricTags));
    }

    /**
     * Creates or gets a counter-type metric.
     *
     * @param metricKey         Key of the metric
     * @param metricName        Name of the metric
     * @param metricHelp        Help description of the metric
     * @param selectorLabels    Selector labels to select the controller resources
     * @param counterMap        Map with counters
     *
     * @return  Counter metric
     */
    protected Counter getCounter(MetricKey metricKey, String metricName, String metricHelp, Optional<String> selectorLabels, Map<MetricKey, Counter> counterMap) {
        return metric(metricKey, selectorLabels, counterMap, tags -> metricsProvider.counter(metricName, metricHelp, tags));
    }

    /**
     * Creates or gets a gauge-type metric.
     *
     * @param metricKey         Key of the metric
     * @param metricName        Name of the metric
     * @param metricHelp        Help description of the metric
     * @param selectorLabels    Selector labels to select the controller resources
     * @param gaugeMap          Map with gauges
     * @param optionalTags      Optional tags to be added to the metric
     *
     * @return  Gauge metric
     */
    protected AtomicLong getGaugeLong(MetricKey metricKey, String metricName, String metricHelp, Optional<String> selectorLabels, Map<MetricKey, AtomicLong> gaugeMap, Tag... optionalTags) {
        return metric(metricKey, selectorLabels, gaugeMap, tags -> metricsProvider.gaugeLong(metricName, metricHelp, tags), optionalTags);
    }

    /**
     * Creates or gets a gauge-type metric.
     *
     * @param metricKey         Key of the metric
     * @param metricName        Name of the metric
     * @param metricHelp        Help description of the metric
     * @param selectorLabels    Selector labels to select the controller resources
     * @param gaugeMap          Map with gauges
     *
     * @return  Gauge metric
     */
    protected AtomicInteger getGauge(MetricKey metricKey, String metricName, String metricHelp, Optional<String> selectorLabels, Map<MetricKey, AtomicInteger> gaugeMap) {
        return metric(metricKey, selectorLabels, gaugeMap, tags -> metricsProvider.gauge(metricName, metricHelp, tags));
    }

    /**
     * Creates or gets a timer-type metric.
     *
     * @param metricKey         Key of the metric
     * @param metricName        Name of the metric
     * @param metricHelp        Help description of the metric
     * @param selectorLabels    Selector labels to select the controller resources
     * @param timerMap          Map with timers
     *
     * @return  Timer metric
     */
    protected Timer getTimer(MetricKey metricKey, String metricName, String metricHelp, Optional<String> selectorLabels, Map<MetricKey, Timer> timerMap) {
        return metric(metricKey, selectorLabels, timerMap, tags -> metricsProvider.timer(metricName, metricHelp, tags));
    }

    protected String getLabelSelectorValues() {
        return selectorLabels != null ? selectorLabels.toSelectorString() : "";
    }
}