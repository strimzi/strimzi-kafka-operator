/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.metrics;

import io.strimzi.api.kafka.model.common.HasConfigurableMetrics;
import io.strimzi.api.kafka.model.common.metrics.StrimziMetricsReporterValues;
import io.strimzi.api.kafka.model.common.metrics.StrimziReporterMetrics;
import io.strimzi.kafka.oauth.common.ConfigException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Represents a model for components with configurable metrics using Strimzi Reporter
 */
public class StrimziReporterMetricsModel {

    /**
     * Name of the Strimzi metrics port
     */
    public static final String METRICS_PORT_NAME = "tcp-strimzi-reporter-metrics";

    /**
     * Number of the Strimzi metrics port
     */
    public static final int METRICS_PORT = 9404;
    private final boolean isEnabled;
    private final Optional<String> allowlist;

    /**
     * Constructs the StrimziReporterMetricsModel for managing configurable metrics with Strimzi Reporter
     *
     * @param specSection StrimziReporterMetrics object containing the metrics configuration
     */
    public StrimziReporterMetricsModel(HasConfigurableMetrics specSection) throws Exception {
        if (specSection.getMetricsConfig() != null) {
            if (specSection.getMetricsConfig() instanceof StrimziReporterMetrics strimziMetrics) {
                // Retrieve the values object
                StrimziMetricsReporterValues metricsConfig = strimziMetrics.getValues();
                // Check if the values object is null
                if (metricsConfig != null) {
                    validateStrimziReporterMetricsConfiguration(metricsConfig);
                    this.isEnabled = true;
                    this.allowlist = metricsConfig.getAllowlist();
                } else {
                    throw new ConfigException("StrimziReporterMetrics configuration values cannot be null.");
                }
            } else {
                throw new ConfigException("Unsupported metrics type " + specSection.getMetricsConfig().getType());
            }
        } else {
            this.isEnabled = false;
            this.allowlist = Optional.empty();
        }
    }

    /**
     * @return True if metrics are enabled. False otherwise.
     */
    public boolean isEnabled() {
        return isEnabled;
    }

    /**
     * Validates the Strimzi Reporter Metrics configuration
     *
     * @param metricsConfig StrimziReporterMetrics configuration to validate
     */
    private void validateStrimziReporterMetricsConfiguration(StrimziMetricsReporterValues metricsConfig) throws Exception {
        List<String> errors = new ArrayList<>();
        if (metricsConfig.getAllowlist().isEmpty()){
            errors.add("Allowlist configuration is missing");
        }

        if (!errors.isEmpty())  {
            throw new ConfigException("StrimziReporterMetrics configuration is invalid: " + errors);
        }
    }
}
