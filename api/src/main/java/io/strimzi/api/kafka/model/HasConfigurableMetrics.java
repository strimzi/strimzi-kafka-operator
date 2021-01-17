/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import java.util.Map;

public interface HasConfigurableMetrics {
    Map<String, Object> getMetrics();
    void setMetrics(Map<String, Object> metrics);
    MetricsConfig getMetricsConfig();
    void setMetricsConfig(MetricsConfig metricsConfig);
}
