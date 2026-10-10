/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.metrics;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.strimzi.api.kafka.model.common.HasConfigurableMetrics;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetrics;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.InvalidResourceException;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.dataformat.yaml.YAMLFactory;
import tools.jackson.dataformat.yaml.YAMLMapper;
import tools.jackson.dataformat.yaml.YAMLSchema;

import java.util.ArrayList;
import java.util.List;

/**
 * Model for the Prometheus JMX Exporter Java agent.
 */
public class JmxPrometheusExporterModel implements MetricsModel {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(JmxPrometheusExporterModel.class);
    // The YAML 1.2 Core schema resolves null, boolean, and number values closest to how Jackson 2 did. The trailing
    // tokens check is disabled so that only the first document is used when the YAML has multiple documents.
    private static final ObjectMapper YAML_READER = YAMLMapper.builder(YAMLFactory.builder().yamlSchema(YAMLSchema.CORE).build())
            .disable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .build();
    private static final ObjectMapper JSON_WRITER = new JsonMapper();

    /**
     * Key under which the metrics configuration is stored in the ConfigMap
     */
    public static final String CONFIG_MAP_KEY = "metrics-config.json";

    private final String configMapName;
    private final String configMapKey;

    /**
     * Constructs the Metrics Model for managing configurable metrics to Strimzi
     *
     * @param spec       Custom resource section configuring metrics
     */
    public JmxPrometheusExporterModel(HasConfigurableMetrics spec) {
        if (spec.getMetricsConfig() != null) {
            JmxPrometheusExporterMetrics config = (JmxPrometheusExporterMetrics) spec.getMetricsConfig();
            validateJmxExporterMetricsConfig(config);
            this.configMapName = config.getValueFrom().getConfigMapKeyRef().getName();
            this.configMapKey = config.getValueFrom().getConfigMapKeyRef().getKey();
        } else {
            throw new InvalidConfigurationException("Unexpected empty metrics config");
        }
    }

    /**
     * Gets the name of the ConfigMap with the metrics configuration.
     *
     * @return  The name of the ConfigMap with the metrics configuration
     */
    public String getConfigMapName() {
        return configMapName;
    }

    /**
     * Gets the key under which the metrics configuration is stored in the ConfigMap.
     *
     * @return  The key under which the metrics configuration is stored in the ConfigMap
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
                    Object yaml = YAML_READER.readValue(data, Object.class);

                    return JSON_WRITER.writeValueAsString(yaml);
                } catch (JacksonException e) {
                    throw new InvalidConfigurationException("Failed to parse metrics configuration", e);
                }
            }
        }
    }

    /**
     * Validates user configuration.
     *
     * @param config User config to be validated.
     */
    /* test */ static void validateJmxExporterMetricsConfig(JmxPrometheusExporterMetrics config) {
        List<String> errors = new ArrayList<>();

        if (config.getValueFrom() != null && config.getValueFrom().getConfigMapKeyRef() != null) {
            // The ConfigMap reference exists
            if (config.getValueFrom().getConfigMapKeyRef().getName() == null
                    || config.getValueFrom().getConfigMapKeyRef().getName().isEmpty()) {
                errors.add("Name of the ConfigMap with metrics configuration is missing");
            }

            if (config.getValueFrom().getConfigMapKeyRef().getKey() == null
                    || config.getValueFrom().getConfigMapKeyRef().getKey().isEmpty()) {
                errors.add("The key under which the metrics configuration is stored in the ConfigMap is missing");
            }
        } else {
            // The ConfigMap reference is missing
            errors.add("ConfigMap reference is missing");
        }

        if (!errors.isEmpty())  {
            throw new InvalidResourceException("Metrics configuration is invalid: " + errors);
        }
    }
}
