/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
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
    public static final String METRICS_RECONCILIATIONS_PERIODICAL = METRICS_PREFIX + "reconciliations.periodical";
    /**
     * Metric name for number of failed reconciliations.
     */
    public static final String METRICS_RECONCILIATIONS_FAILED = METRICS_PREFIX + "reconciliations.failed";
    /**
     * Metric name for number of successful reconciliations.
     */
    public static final String METRICS_RECONCILIATIONS_SUCCESSFUL = METRICS_PREFIX + "reconciliations.successful";
    /**
     * Metric name for duration of reconciliations.
     */
    public static final String METRICS_RECONCILIATIONS_DURATION = METRICS_PREFIX + "reconciliations.duration";
    /**
     * Metric name for number of locked reconciliations.
     */
    public static final String METRICS_RECONCILIATIONS_LOCKED = METRICS_PREFIX + "reconciliations.locked";
    /**
     * Metric name for number of resources managed by the operator.
     */
    public static final String METRICS_RESOURCES = METRICS_PREFIX + "resources";
    /**
     * Metric name for number of paused resources.
     */
    public static final String METRICS_RESOURCES_PAUSED = METRICS_PREFIX + "resources.paused";
    /**
     * Metric name for server certificate expiration timestamp in ms.
     */
    public static final String METRICS_SERVER_CERTIFICATE_EXPIRATION_MS = METRICS_PREFIX + "server.certificate.expiration.ms";
    /**
     * Metric name for client certificate expiration timestamp in ms.
     */
    public static final String METRICS_CLIENT_CERTIFICATE_EXPIRATION_MS = METRICS_PREFIX + "client.certificate.expiration.ms";

    protected final String kind;
    protected final Labels selectorLabels;
    protected final MetricsProvider metricsProvider;

    protected final Map<String, AtomicInteger> resourceCounterMap = new ConcurrentHashMap<>(1);
    protected final Map<String, AtomicInteger> pausedResourceCounterMap = new ConcurrentHashMap<>(1);
    protected final Map<String, AtomicLong> serverCertificateExpirationMap = new ConcurrentHashMap<>(1);
    protected final Map<String, AtomicLong> clientCertificateExpirationMap = new ConcurrentHashMap<>(1);
    private final Map<String, Counter> periodicReconciliationsCounterMap = new ConcurrentHashMap<>(1);
    private final Map<String, Counter> reconciliationsCounterMap = new ConcurrentHashMap<>(1);
    private final Map<String, Counter> failedReconciliationsCounterMap = new ConcurrentHashMap<>(1);
    private final Map<String, Counter> successfulReconciliationsCounterMap = new ConcurrentHashMap<>(1);
    private final Map<String, Counter> lockedReconciliationsCounterMap = new ConcurrentHashMap<>(1);
    private final Map<String, Timer> reconciliationsTimerMap = new ConcurrentHashMap<>(1);

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

    /**
     * Constructing the tags for the metrics
     *
     * @param clusterName       Name of the cluster
     * @param namespace         Namespace of the resources being reconciled
     * @param kind              Kind of the resources for which these metrics apply
     * @return  Tags
     */
    public Tags getTags(String clusterName, String namespace, String kind) {
        return MetricsUtils.getMetricTags(clusterName, namespace, kind, selectorLabels);
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
        return getCounter(namespace, kind, METRICS_RECONCILIATIONS_PERIODICAL, metricsProvider, selectorLabels, periodicReconciliationsCounterMap,
                "Number of periodical reconciliations done by the operator");
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
        return getCounter(namespace, kind, METRICS_RECONCILIATIONS, metricsProvider, selectorLabels, reconciliationsCounterMap,
                "Number of reconciliations done by the operator for individual resources");
    }

    /**
     * Counter metric for number of failed reconciliations.
     *
     * @param namespace     Namespace of the resources being reconciled
     *
     * @return  Metrics counter
     */
    public Counter failedReconciliationsCounter(String namespace) {
        return getCounter(namespace, kind, METRICS_RECONCILIATIONS_FAILED, metricsProvider, selectorLabels, failedReconciliationsCounterMap,
                "Number of reconciliations done by the operator for individual resources which failed");
    }

    /**
     * Counter metric for number of successful reconciliations.
     *
     * @param namespace     Namespace of the resources being reconciled
     *
     * @return  Metrics counter
     */
    public Counter successfulReconciliationsCounter(String namespace) {
        return getCounter(namespace, kind, METRICS_RECONCILIATIONS_SUCCESSFUL, metricsProvider, selectorLabels, successfulReconciliationsCounterMap,
                "Number of reconciliations done by the operator for individual resources which were successful");
    }

    /**
     * Timer which measures how long do the reconciliations take.
     *
     * @param namespace     Namespace of the resources being reconciled
     *
     * @return  Metrics timer
     */
    public Timer reconciliationsTimer(String namespace) {
        return getTimer(namespace, kind, METRICS_RECONCILIATIONS_DURATION, metricsProvider, selectorLabels, reconciliationsTimerMap,
                "The time the reconciliation takes to complete");
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
        return getCounter(namespace, kind, METRICS_RECONCILIATIONS_LOCKED, metricsProvider, selectorLabels, lockedReconciliationsCounterMap,
                "Number of reconciliations skipped because another reconciliation for the same resource was still running");
    }

    /**
     * Counter metric for number of resources managed by this operator.
     *
     * @param namespace     Namespace of the resources being reconciled
     *
     * @return  Metrics counter
     */
    public AtomicInteger resourceCounter(String namespace) {
        return getGauge(namespace, kind, METRICS_RESOURCES, metricsProvider, selectorLabels, resourceCounterMap,
                "Number of custom resources the operator sees");
    }

    /**
     * Counter metric for number of paused resources which are not reconciled.
     *
     * @param namespace     Namespace of the resources being reconciled
     *
     * @return  Metrics counter
     */
    public AtomicInteger pausedResourceCounter(String namespace) {
        return getGauge(namespace, kind, METRICS_RESOURCES_PAUSED, metricsProvider, selectorLabels, pausedResourceCounterMap,
                "Number of custom resources the operator sees but does not reconcile due to paused reconciliations");
    }

    /**
     * Time in milliseconds when the server certificate expiration timestamp in ms.
     *
     * @param clusterName   Name of the cluster
     * @param namespace     Namespace of the resources being reconciled
     * @return Metric gauge
     */
    public AtomicLong serverCertificateExpiration(String clusterName, String namespace) {
        return getGaugeLong(clusterName, namespace, kind, METRICS_SERVER_CERTIFICATE_EXPIRATION_MS, metricsProvider, selectorLabels, serverCertificateExpirationMap,
                "Time in milliseconds when the certificate expires");
    }

    /**
     * Time in milliseconds when the client certificate expiration timestamp in ms.
     *
     * @param clusterName   Name of the cluster
     * @param namespace     Namespace of the resources being reconciled
     * @return Metric gauge
     */
    public AtomicLong clientCertificateExpiration(String clusterName, String namespace) {
        return getGaugeLong(clusterName, namespace, kind, METRICS_CLIENT_CERTIFICATE_EXPIRATION_MS, metricsProvider, selectorLabels, clientCertificateExpirationMap,
                "Time in milliseconds when the certificate expires");
    }

    ////////////////////
    // Static methods for handling metrics
    ////////////////////

    /**
     * Utility method which gets or creates the metric.
     *
     * @param namespace         Namespace or the resource
     * @param kind              Kind of the resource
     * @param selectorLabels    Selector labels used to filter the resources
     * @param metricMap         The map with the metrics
     * @param fn                Method fo generating the metrics tags
     *
     * @return  Metric
     *
     * @param <M>   Type of the metric
     */
    protected static <M> M metric(String namespace, String kind, Labels selectorLabels, Map<String, M> metricMap, Function<Tags, M> fn) {
        return metric(null, namespace, kind, selectorLabels, metricMap, fn);
    }

    /**
     * Utility method which gets or creates the metric.
     *
     * @param clusterName       Strimzi cluster name
     * @param namespace         Namespace or the resource
     * @param kind              Kind of the resource
     * @param selectorLabels    Selector labels used to filter the resources
     * @param metricMap         The map with the metrics
     * @param fn                Method fo generating the metrics tags
     *
     * @return  Metric
     *
     * @param <M>   Type of the metric
     */
    protected static <M> M metric(String clusterName, String namespace, String kind, Labels selectorLabels, Map<String, M> metricMap, Function<Tags, M> fn) {
        Tags metricTags = MetricsUtils.getMetricTags(clusterName, namespace, kind, selectorLabels);
        String metricKey = MetricsUtils.getMetricKey(clusterName, namespace, kind);

        if (metricMap.containsKey(metricKey)) {
            return metricMap.get(metricKey);
        }
        return fn.apply(metricTags);
    }

    /**
     * Creates or gets a counter-type metric.
     *
     * @param namespace         Namespace of the resource
     * @param kind              Kind of the resource
     * @param metricName        Name of the metric
     * @param metrics           Metrics provider
     * @param selectorLabels    Selector labels used to filter the resources
     * @param counterMap        Map with counters
     * @param metricHelp        Help description of the metric
     *
     * @return  Counter metric
     */
    protected static Counter getCounter(String namespace, String kind, String metricName, MetricsProvider metrics, Labels selectorLabels, Map<String, Counter> counterMap, String metricHelp) {
        return metric(namespace, kind, selectorLabels, counterMap, tags -> metrics.counter(metricName, metricHelp, tags));
    }

    /**
     * Creates or gets a gauge-type metric.
     *
     * @param clusterName       Strimzi cluster name
     * @param namespace         Namespace of the resource
     * @param kind              Kind of the resource
     * @param metricName        Name of the metric
     * @param metrics           Metrics provider
     * @param selectorLabels    Selector labels used to filter the resources
     * @param gaugeMap          Map with gauges
     * @param metricHelp        Help description of the metric
     *
     * @return  Gauge metric
     */
    protected static AtomicLong getGaugeLong(String clusterName, String namespace, String kind, String metricName, MetricsProvider metrics, Labels selectorLabels, Map<String, AtomicLong> gaugeMap, String metricHelp) {
        return metric(clusterName, namespace, kind, selectorLabels, gaugeMap, tags -> metrics.gaugeLong(metricName, metricHelp, tags));
    }

    /**
     * Creates or gets a gauge-type metric.
     *
     * @param namespace         Namespace of the resource
     * @param kind              Kind of the resource
     * @param metricName        Name of the metric
     * @param metrics           Metrics provider
     * @param selectorLabels    Selector labels used to filter the resources
     * @param gaugeMap          Map with gauges
     * @param metricHelp        Help description of the metric
     *
     * @return  Gauge metric
     */
    protected static AtomicInteger getGauge(String namespace, String kind, String metricName, MetricsProvider metrics, Labels selectorLabels, Map<String, AtomicInteger> gaugeMap, String metricHelp) {
        return metric(namespace, kind, selectorLabels, gaugeMap, tags -> metrics.gauge(metricName, metricHelp, tags));
    }

    /**
     * Creates or gets a timer-type metric.
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
    protected static Timer getTimer(String namespace, String kind, String metricName, MetricsProvider metrics, Labels selectorLabels, Map<String, Timer> timerMap, String metricHelp) {
        return metric(namespace, kind, selectorLabels, timerMap, tags -> metrics.timer(metricName, metricHelp, tags));
    }
}
