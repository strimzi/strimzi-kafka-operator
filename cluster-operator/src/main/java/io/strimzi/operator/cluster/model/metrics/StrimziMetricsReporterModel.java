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
     * The user-configured allow list, or {@code null} when the user did not set one.
     */
    private final List<String> allowList;

    /**
     * Stores only the user-configured allow list; the default is resolved later at the call site, as it can
     * depend on the node's role.
     *
     * @param spec Custom resource section configuring metrics.
     */
    public StrimziMetricsReporterModel(HasConfigurableMetrics spec) {
        if (spec.getMetricsConfig() != null) {
            StrimziMetricsReporter config = (StrimziMetricsReporter) spec.getMetricsConfig();
            validate(config);
            this.allowList = config.getValues() != null ? config.getValues().getAllowList() : null;
        } else {
            throw new InvalidConfigurationException("Unexpected empty metrics config");
        }
    }

    /**
     * Gets the comma-separated list of allow regex expressions, falling back to the given default when the user
     * did not configure one.
     *
     * @param defaultAllowList Default allow list to be used when the user did not configure one.
     * @return Comma separated list of allow regex expressions.
     */
    public String getAllowListOrDefault(List<String> defaultAllowList) {
        return String.join(",", allowList != null ? allowList : defaultAllowList);
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
