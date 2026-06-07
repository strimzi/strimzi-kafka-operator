/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.metrics;

import io.strimzi.api.kafka.model.common.HasConfigurableMetrics;
import io.strimzi.api.kafka.model.common.metrics.StrimziMetricsReporter;
import io.strimzi.operator.cluster.model.NodeRef;
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
     * Fully qualified class name of the Strimzi Metrics Reporter.
     */
    private final List<String> allowList;
    private final List<String> brokerAllowList;
    private final List<String> controllerAllowList;

    /**
     * Constructs the Metrics Model for managing configurable metrics to Strimzi.
     *
     * @param spec Custom resource section configuring metrics.
     * @param defaultAllowList Default allow list to be used when no value is provided.
     */
    public StrimziMetricsReporterModel(HasConfigurableMetrics spec, List<String> defaultAllowList) {
        this(spec, defaultAllowList, defaultAllowList, defaultAllowList);
    }

    /**
     * Constructs the Metrics Model for managing configurable metrics to Strimzi.
     *
     * @param spec Custom resource section configuring metrics.
     * @param defaultAllowList Default allow list to be used when no value is provided.
     * @param defaultBrokerAllowList Default allow list to be used for broker nodes when no value is provided.
     * @param defaultControllerAllowList Default allow list to be used for controller nodes when no value is provided.
     */
    public StrimziMetricsReporterModel(HasConfigurableMetrics spec,
                                       List<String> defaultAllowList,
                                       List<String> defaultBrokerAllowList,
                                       List<String> defaultControllerAllowList) {
        if (spec.getMetricsConfig() != null) {
            StrimziMetricsReporter config = (StrimziMetricsReporter) spec.getMetricsConfig();
            validate(config);
            if (config.getValues() != null && config.getValues().getAllowList() != null) {
                this.allowList = config.getValues().getAllowList();
                this.brokerAllowList = this.allowList;
                this.controllerAllowList = this.allowList;
            } else {
                this.allowList = defaultAllowList;
                this.brokerAllowList = defaultBrokerAllowList;
                this.controllerAllowList = defaultControllerAllowList;
            }
        } else {
            throw new InvalidConfigurationException("Unexpected empty metrics config");
        }
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
     * Gets the comma-separated list of allow regex expressions for a Kafka node.
     *
     * @param node Kafka node.
     *
     * @return Comma separated list of allow regex expressions.
     */
    public String getAllowList(NodeRef node) {
        if (node.broker() && !node.controller()) {
            return String.join(",", brokerAllowList);
        } else if (!node.broker() && node.controller()) {
            return String.join(",", controllerAllowList);
        } else {
            return getAllowList();
        }
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
