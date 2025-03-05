/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.metrics;

import io.strimzi.api.kafka.model.common.HasConfigurableMetrics;
import io.strimzi.api.kafka.model.common.metrics.StrimziMetricsReporter;
import io.strimzi.operator.common.model.InvalidResourceException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Model for the Strimzi Metrics Reporter Kafka plugin.
 */
public class StrimziMetricsReporterModel implements MetricsModel {
    /**
     * Fully qualified class name of the Strimzi Kafka Prometheus Metrics Reporter.
     */
    private final boolean isEnabled;
    private final List<String> allowList;

    /**
     * Constructs the Metrics Model for managing configurable metrics to Strimzi.
     *
     * @param spec Custom resource section configuring metrics.
     */
    public StrimziMetricsReporterModel(HasConfigurableMetrics spec) {
        this(spec, List.of(".*"));
    }

        /**
         * Constructs the Metrics Model for managing configurable metrics to Strimzi.
         *
         * @param spec Custom resource section configuring metrics.
         * @param defaultAllowList Default allow list to be used when no value is provided.
         */
    public StrimziMetricsReporterModel(HasConfigurableMetrics spec, List<String> defaultAllowList) {
        if (spec.getMetricsConfig() != null) {
            StrimziMetricsReporter config = (StrimziMetricsReporter) spec.getMetricsConfig();
            validate(config);
            this.isEnabled = true;
            this.allowList = config.getValues() != null && config.getValues().getAllowList() != null
                    ? config.getValues().getAllowList() : defaultAllowList;
        } else {
            this.isEnabled = false;
            this.allowList = null;
        }
    }

    @Override
    public boolean isEnabled() {
        return isEnabled;
    }

    /**
     * @return Comma separated list of allow regex expressions.
     */
    public Optional<String> getAllowList() {
        return allowList != null ? Optional.of(String.join(",", allowList)) : Optional.empty();
    }

    /**
     * Validates user configuration.
     *
     * @param config Config to be validated.
     *
     */
    /* test */ static void validate(StrimziMetricsReporter config) {
        List<String> errors = new ArrayList<>();
        if (config.getValues() != null && config.getValues().getAllowList() != null) {
            if (config.getValues().getAllowList().isEmpty()) {
                errors.add("Allowlist should contain at least one element");
            }

            for (String regex : config.getValues().getAllowList()) {
                try {
                    Pattern.compile(regex);
                } catch (PatternSyntaxException pse) {
                    errors.add(String.format("Invalid regex: %s, %s", regex, pse.getDescription()));
                }
            }
        }

        if (!errors.isEmpty()) {
            throw new InvalidResourceException("Metrics configuration is invalid: " + errors);
        }
    }
}
