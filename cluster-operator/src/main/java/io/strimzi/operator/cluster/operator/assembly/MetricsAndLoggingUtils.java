/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.strimzi.api.kafka.model.common.ExternalLogging;
import io.strimzi.operator.cluster.model.MetricsAndLogging;
import io.strimzi.operator.cluster.model.logging.LoggingModel;
import io.strimzi.operator.cluster.model.metrics.JmxPrometheusExporterModel;
import io.strimzi.operator.cluster.model.metrics.MetricsModel;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.InvalidResourceException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Shared methods for working with Metrics and Logging configurations. These methods are bundled because we store both
 * logging and metrics in the same configuration map. So some parts of the logging and metrics processing are coupled.
 */
public class MetricsAndLoggingUtils {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(MetricsAndLoggingUtils.class.getName());

    private MetricsAndLoggingUtils() { }

    /**
     * Creates a Metrics and Logging holder based on the operand logging configuration
     *
     * @param reconciliation        Reconciliation marker
     * @param configMapOperations   ConfigMap operator
     * @param logging               Logging configuration
     * @param metrics               Metrics configuration
     *
     * @return CompletionStage with the metrics and logging configuration holder
     */
    public static CompletionStage<MetricsAndLogging> metricsAndLogging(Reconciliation reconciliation,
                                                                       ConfigMapOperator configMapOperations,
                                                                       LoggingModel logging,
                                                                       MetricsModel metrics) {
        CompletableFuture<ConfigMap> metricsFuture = metricsConfigMap(reconciliation, configMapOperations, metrics);
        CompletableFuture<ConfigMap> loggingFuture = loggingConfigMap(reconciliation, configMapOperations, logging);
        return CompletableFuture.allOf(metricsFuture, loggingFuture)
                .thenApply(v -> new MetricsAndLogging(metricsFuture.join(), loggingFuture.join()));
    }

    private static CompletableFuture<ConfigMap> metricsConfigMap(Reconciliation reconciliation, ConfigMapOperator configMapOperations, MetricsModel metrics) {
        // this is only for JMX Prometheus Exporter, because the Strimzi Metrics Reporter configuration is in the Kafka configuration file
        if (metrics instanceof JmxPrometheusExporterModel model && model.getConfigMapName() != null) {
            return configMapOperations.getAsync(reconciliation.namespace(), model.getConfigMapName()).toCompletableFuture();
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private static CompletableFuture<ConfigMap> loggingConfigMap(Reconciliation reconciliation, ConfigMapOperator configMapOperations, LoggingModel logging) {
        if (logging != null && logging.getLogging() instanceof ExternalLogging externalLogging) {
            if (externalLogging.getValueFrom() != null
                    && externalLogging.getValueFrom().getConfigMapKeyRef() != null
                    && externalLogging.getValueFrom().getConfigMapKeyRef().getName() != null) {
                return configMapOperations.getAsync(reconciliation.namespace(), externalLogging.getValueFrom().getConfigMapKeyRef().getName()).toCompletableFuture();
            } else {
                LOGGER.warnCr(reconciliation, "External logging configuration does not specify logging ConfigMap");
                throw new InvalidResourceException("External logging configuration does not specify logging ConfigMap");
            }
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

}
