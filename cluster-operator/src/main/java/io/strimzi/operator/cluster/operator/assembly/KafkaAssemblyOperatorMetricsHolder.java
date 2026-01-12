/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.config.ConfigParameter;
import io.strimzi.operator.common.metrics.CertificateMetricKey;
import io.strimzi.operator.common.metrics.MetricKey;
import io.strimzi.operator.common.metrics.MetricsUtils;
import io.strimzi.operator.common.metrics.OperatorMetricsHolder;
import io.strimzi.operator.common.model.Labels;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * Operator metrics holder for Kafka assembly operator.
 */
public class KafkaAssemblyOperatorMetricsHolder extends OperatorMetricsHolder {
    /**
     * Metric name for certificate expiration timestamp in ms.
     */
    public static final String METRICS_CERTIFICATE_EXPIRATION_MS = METRICS_PREFIX + "certificate.expiration.timestamp.ms";

    protected final Map<MetricKey, AtomicLong> certificateExpirationMap = new ConcurrentHashMap<>(1);
    protected final Map<MetricKey, AtomicInteger> nodePoolResourceCounterMap = new ConcurrentHashMap<>(1);

    /**
     * Constructs the operator metrics holder
     *
     * @param kind            Kind of the resources for which these metrics apply
     * @param selectorLabels  Selector labels to select the controller resources
     * @param metricsProvider Metrics provider
     */
    public KafkaAssemblyOperatorMetricsHolder(String kind, Labels selectorLabels, MetricsProvider metricsProvider) {
        super(kind, selectorLabels, metricsProvider);
    }


    /**
     * Time in milliseconds when the server certificate expiration timestamp in ms.
     *
     * @param clusterName   Name of the cluster
     * @param namespace     Namespace of the resources being reconciled
     * @return Metric gauge
     */
    public AtomicLong clusterCaCertificateExpiration(String clusterName, String namespace) {
        return getGaugeLong(new CertificateMetricKey(kind, namespace, clusterName, CertificateMetricKey.Type.CLUSTER_CA),
                METRICS_CERTIFICATE_EXPIRATION_MS, "Time in milliseconds when the certificate expires",
                Optional.empty(), certificateExpirationMap,
                Tag.of("cluster", clusterName),
                Tag.of("type", CertificateMetricKey.Type.CLUSTER_CA.getDisplayName()),
                Tag.of("resource-namespace", namespace));
    }

    /**
     * Time in milliseconds when the client certificate expiration timestamp in ms.
     *
     * @param clusterName   Name of the cluster
     * @param namespace     Namespace of the resources being reconciled
     * @return Metric gauge
     */
    public AtomicLong clientCaCertificateExpiration(String clusterName, String namespace) {
        return getGaugeLong(new CertificateMetricKey(kind, namespace, clusterName, CertificateMetricKey.Type.CLIENT_CA),
                METRICS_CERTIFICATE_EXPIRATION_MS, "Time in milliseconds when the certificate expires",
                Optional.empty(), certificateExpirationMap,
                Tag.of("cluster", clusterName),
                Tag.of("type", CertificateMetricKey.Type.CLIENT_CA.getDisplayName()),
                Tag.of("resource-namespace", namespace));
    }

    /**
     * Removing all metrics for the certificates which match the given predicate.
     *
     * @param shouldDelete  Predicate to filter the certificates which should be removed
     */
    public void removeMetricsForCertificates(Predicate<CertificateMetricKey> shouldDelete) {
        final List<CertificateMetricKey> removedKeys = new ArrayList<>();

        certificateExpirationMap.keySet().stream()
                .map(CertificateMetricKey.class::cast)
                .filter(shouldDelete)
                .forEach(key -> {
                    Tags tags = MetricsUtils.getAllMetricTags(key.getNamespace(), key.getKind(), Optional.empty(),
                            Tag.of("cluster", key.getClusterName()),
                            Tag.of("type", key.getCaType()));
                    removeMetric(METRICS_CERTIFICATE_EXPIRATION_MS, tags);
                    removedKeys.add(key);
                });

        removedKeys.forEach(certificateExpirationMap::remove);
    }

    /**
     * Counter metric for number of KafkaNodePool resources.
     *
     * @param namespace     Namespace of the resources being reconciled
     *
     * @return  Metrics gauge
     */
    public AtomicInteger nodePoolResourceCounter(String namespace) {
        return getGauge(new MetricKey(KafkaNodePool.RESOURCE_KIND, namespace), METRICS_RESOURCES,
                "Number of custom resources the operator sees",
                Optional.of(getLabelSelectorValues()), nodePoolResourceCounterMap);
    }

    /**
     * Resets all values in the node pool resource counter map to 0. This is used to
     * handle removed node pool resources from various namespaces during the periodical reconciliation.
     *
     * @param namespace Namespace for which should the metrics be reset to 0
     */
    public void resetNodePoolCounters(String namespace) {
        if (namespace.equals(ConfigParameter.ANY_NAMESPACE)) {
            nodePoolResourceCounterMap.forEach((key, counter) -> counter.set(0));
        } else {
            nodePoolResourceCounter(namespace).set(0);
        }
    }
}
