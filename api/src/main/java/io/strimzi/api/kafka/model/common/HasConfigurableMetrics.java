/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common;

import io.strimzi.api.kafka.model.common.metrics.MetricsConfig;

/**
 * This interface is used for sections of our custom resources which support configurable metrics using a reference to
 * a ConfigMp with the configuration.
 */
public interface HasConfigurableMetrics {
    /**
     * Gets the metrics configuration
     *
     * @return  Metrics configuration
     */
    MetricsConfig getMetricsConfig();

    /**
     * Sets the metrics configuration
     *
     * @param metricsConfig     Metrics configuration
     */
    void setMetricsConfig(MetricsConfig metricsConfig);
}
