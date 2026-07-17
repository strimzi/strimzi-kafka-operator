/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.metrics;

import io.strimzi.api.kafka.model.common.HasConfigurableMetrics;
import io.strimzi.api.kafka.model.common.metrics.StrimziMetricsReporter;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.model.InvalidResourceException;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Model for the Strimzi Metrics Reporter Kafka plugin.
 */
public class StrimziMetricsReporterModel implements MetricsModel {
    /**
     * The allow list of regex patterns for metrics collection.
     */
    private final List<String> allowList;

    /**
     * Whether the allow list was explicitly set by the user (true) or is a role-based default (false).
     */
    private final boolean customAllowList;

    /**
     * Constructs the Metrics Model from a custom resource spec.
     * If the user provided an explicit allowlist, it is used; otherwise the provided default is used.
     *
     * @param spec             Custom resource section configuring metrics.
     * @param defaultAllowList Default allow list to be used when no value is provided.
     */
    public StrimziMetricsReporterModel(HasConfigurableMetrics spec, List<String> defaultAllowList) {
        if (spec.getMetricsConfig() != null) {
            StrimziMetricsReporter config = (StrimziMetricsReporter) spec.getMetricsConfig();
            validate(config);
            boolean hasCustomList = config.getValues() != null && config.getValues().getAllowList() != null;
            this.allowList = hasCustomList ? config.getValues().getAllowList() : defaultAllowList;
            this.customAllowList = hasCustomList;
        } else {
            throw new InvalidConfigurationException("Unexpected empty metrics config");
        }
    }

    /**
     * Constructs the Metrics Model directly from a default allow list (used for role-based defaults).
     *
     * @param defaultAllowList The role-specific default allow list.
     */
    public StrimziMetricsReporterModel(List<String> defaultAllowList) {
        this.allowList = defaultAllowList;
        this.customAllowList = false;
    }

    /**
     * Returns whether the allow list was explicitly configured by the user.
     *
     * @return true if the user provided a custom allowlist, false if using a role-based default.
     */
    public boolean isCustomAllowList() {
        return customAllowList;
    }

    /**
     * Gets the comma-separated list of allow regex expressions.
     *
     * @return Comma separated list of allow regex expressions.
     */
    public String getAllowList() {
        return String.join(",", allowList);
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
