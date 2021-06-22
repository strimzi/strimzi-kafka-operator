/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

/**
 * This interface is used for sections of our custom resources which support configurable metrics - either using the
 * inlined Map or using the reference to the ConfigMp with the configuration.
 */
public interface HasConfigurableMetrics {
    MetricsConfig getMetricsConfig();
    void setMetricsConfig(MetricsConfig metricsConfig);
}
