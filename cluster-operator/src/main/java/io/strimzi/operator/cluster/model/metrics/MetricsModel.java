/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.metrics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.strimzi.api.kafka.model.common.HasConfigurableMetrics;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetrics;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.InvalidResourceException;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a model for components with configurable metrics
 */
public class MetricsModel {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(MetricsModel.class.getName());

    /**
     * Name of the Prometheus metrics port
     */
    public static final String METRICS_PORT_NAME = "tcp-prometheus";

    /**
     * Number of the Prometheus metrics port
     */
    public static final int METRICS_PORT = 9404;

    /**
     * Key under which the metrics configuration is stored in the ConfigMap
     */
    public static final String CONFIG_MAP_KEY = "metrics-config.json";

    private final boolean isEnabled;
    private final String configMapName;
    private final String configMapKey;

    /**
     * Constructs the Metrics Model for managing configurable metrics to Strimzi
     *
     * @param specSection       Custom resource section configuring metrics
     */
    public MetricsModel(HasConfigurableMetrics specSection) {
        if (specSection.getMetricsConfig() != null) {
            if (specSection.getMetricsConfig() instanceof JmxPrometheusExporterMetrics jmxMetrics) {
                validateJmxPrometheusExporterMetricsConfiguration(jmxMetrics);

                this.isEnabled = true;
                this.configMapName = jmxMetrics.getValueFrom().getConfigMapKeyRef().getName();
                this.configMapKey = jmxMetrics.getValueFrom().getConfigMapKeyRef().getKey();
            } else {
                throw new InvalidResourceException("Unsupported metrics type " + specSection.getMetricsConfig().getType());
            }
        } else {
            this.isEnabled = false;
            this.configMapName = null;
            this.configMapKey = null;
        }
    }

    /**
     * @return  True if metrics are enabled. False otherwise.
     */
    public boolean isEnabled() {
        return isEnabled;
    }

    /**
     * @return  The name of the Config Map with the metrics configuration
     */
    public String getConfigMapName() {
        return configMapName;
    }

    /**
     * @return  The key under which the metrics configuration is stored in the Config Map
     */
    public String getConfigMapKey() {
        return configMapKey;
    }

    /**
     * Generates Prometheus metrics configuration based on the JMXExporter configuration from the user-provided
     * ConfigMap. When metrics are not enabled, returns null.
     *
     * @param reconciliation    Reconciliation marker
     * @param configMap         ConfigMap with the metrics exporter configuration
     *
     * @return  String with JSON formatted metrics configuration or null if metrics are not enabled
     */
    public String metricsJson(Reconciliation reconciliation, ConfigMap configMap) {
        if (isEnabled)  {
            if (configMap == null) {
                LOGGER.warnCr(reconciliation, "ConfigMap {} does not exist.", configMapName);
                throw new InvalidConfigurationException("ConfigMap " + configMapName + " does not exist");
            } else {
                String data = configMap.getData().get(configMapKey);

                if (data == null) {
                    LOGGER.warnCr(reconciliation, "ConfigMap {} does not contain specified key {}.", configMapName, configMapKey);
                    throw new InvalidConfigurationException("ConfigMap " + configMapName + " does not contain specified key " + configMapKey);
                } else {
                    if (data.isEmpty()) {
                        return "{}";
                    }

                    try {
                        ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
                        Object yaml = yamlReader.readValue(data, Object.class);
                        ObjectMapper jsonWriter = new ObjectMapper();

                        return jsonWriter.writeValueAsString(yaml);
                    } catch (JsonProcessingException e) {
                        throw new InvalidConfigurationException("Failed to parse metrics configuration", e);
                    }
                }
            }
        } else {
            return null;
        }
    }

    /**
     * Validates the JMX Prometheus Exporter Metrics configuration
     *
     * @param jmxMetrics    JMX Prometheus Exporter Metrics configuration which should be validated
     */
    /* test */ static void validateJmxPrometheusExporterMetricsConfiguration(JmxPrometheusExporterMetrics jmxMetrics)   {
        List<String> errors = new ArrayList<>();

        if (jmxMetrics.getValueFrom() != null
                && jmxMetrics.getValueFrom().getConfigMapKeyRef() != null)   {
            // The Config Map reference exists
            if (jmxMetrics.getValueFrom().getConfigMapKeyRef().getName() == null
                    || jmxMetrics.getValueFrom().getConfigMapKeyRef().getName().isEmpty())  {
                errors.add("Name of the Config Map with metrics configuration is missing");
            }

            if (jmxMetrics.getValueFrom().getConfigMapKeyRef().getKey() == null
                    || jmxMetrics.getValueFrom().getConfigMapKeyRef().getKey().isEmpty())  {
                errors.add("The key under which the metrics configuration is stored in the ConfigMap is missing");
            }
        } else {
            // The Config Map reference is missing
            errors.add("Config Map reference is missing");
        }

        if (!errors.isEmpty())  {
            throw new InvalidResourceException("Metrics configuration is invalid: " + errors);
        }
    }
}