/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.operator.cluster.model.logging.LoggingModel;
import io.strimzi.operator.cluster.model.logging.SupportsLogging;
import io.strimzi.operator.cluster.model.metrics.MetricsModel;
import io.strimzi.operator.cluster.model.metrics.SupportsMetrics;
import io.strimzi.operator.common.MetricsAndLogging;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.vertx.core.Future;

import java.util.HashMap;
import java.util.Map;

/**
 * Shared methods for working with Metrics and Logging configurations. These methods are bundled because we store both
 * logging and metrics in the same configuration map. So some parts of the logging and metrics processing are coupled.
 */
public class MetricsAndLoggingUtils {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(MetricsAndLoggingUtils.class.getName());

    /**
     * Creates a Metrics and Logging holder based on the operand logging configuration
     *
     * @param reconciliation        Reconciliation marker
     * @param configMapOperations   ConfigMap operator
     * @param logging               Logging configuration
     * @param metrics               Metrics configuration
     *
     * @return Future with the metrics and logging configuration holder
     */
    public static Future<MetricsAndLogging> metricsAndLogging(Reconciliation reconciliation,
                                                              ConfigMapOperator configMapOperations,
                                                              LoggingModel logging,
                                                              MetricsModel metrics) {
        return Future
                .join(metricsConfigMap(reconciliation, configMapOperations, metrics), loggingConfigMap(reconciliation, configMapOperations, logging))
                .map(result -> new MetricsAndLogging(result.resultAt(0), result.resultAt(1)));
    }

    private static Future<ConfigMap> metricsConfigMap(Reconciliation reconciliation, ConfigMapOperator configMapOperations, MetricsModel metrics) {
        if (metrics != null && metrics.isEnabled()) {
            return configMapOperations.getAsync(reconciliation.namespace(), metrics.getConfigMapName());
        } else {
            return Future.succeededFuture(null);
        }
    }

    private static Future<ConfigMap> loggingConfigMap(Reconciliation reconciliation, ConfigMapOperator configMapOperations, LoggingModel logging) {
        if (logging != null && logging.getLogging() instanceof ExternalLogging externalLogging) {
            if (externalLogging.getValueFrom() != null
                    && externalLogging.getValueFrom().getConfigMapKeyRef() != null
                    && externalLogging.getValueFrom().getConfigMapKeyRef().getName() != null) {
                return configMapOperations.getAsync(reconciliation.namespace(), externalLogging.getValueFrom().getConfigMapKeyRef().getName());
            } else {
                LOGGER.warnCr(reconciliation, "External logging configuration does not specify logging ConfigMap");
                throw new InvalidResourceException("External logging configuration does not specify logging ConfigMap");
            }
        } else {
            return Future.succeededFuture(null);
        }
    }

    /**
     * Generates a metrics and logging ConfigMap according to configured defaults. This is used with most operands, but
     * not all of them. Kafka brokers have own methods in the KafkaCluster class. So does the Bridge. And Kafka Exporter
     * has no metrics or logging ConfigMap at all.
     *
     * @param reconciliation        Reconciliation marker
     * @param model                 AbstractModel instance possibly implementing SupportsLogging and SupportsMetrics interfaces
     * @param metricsAndLogging     ConfigMaps with external logging and metrics configuration
     *
     * @return The generated ConfigMap.
     */
    public static Map<String, String> generateMetricsAndLogConfigMapData(Reconciliation reconciliation, AbstractModel model, MetricsAndLogging metricsAndLogging) {
        Map<String, String> data = new HashMap<>(2);

        if (model instanceof SupportsLogging supportsLogging) {
            data.put(supportsLogging.logging().configMapKey(), supportsLogging.logging().loggingConfiguration(reconciliation, metricsAndLogging.loggingCm()));
        }

        if (model instanceof SupportsMetrics supportMetrics) {
            String parseResult = supportMetrics.metrics().metricsJson(reconciliation, metricsAndLogging.metricsCm());
            if (parseResult != null) {
                data.put(MetricsModel.CONFIG_MAP_KEY, parseResult);
            }
        }

        return data;
    }
}
