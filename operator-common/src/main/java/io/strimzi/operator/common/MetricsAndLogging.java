/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.fabric8.kubernetes.api.model.ConfigMap;

/**
 * Holder class for Metrics and Logging configuration
 */
public class MetricsAndLogging {
    private ConfigMap metricsCm;
    private ConfigMap loggingCm;

    /**
     * Constructs the holder
     *
     * @param metricsCm The Config Map with metrics configuration
     * @param loggingCm The Config Map with logging configuration
     */
    public MetricsAndLogging(ConfigMap metricsCm, ConfigMap loggingCm) {
        this.setMetricsCm(metricsCm);
        this.setLoggingCm(loggingCm);
    }

    /**
     * @return  Returns the metrics config map
     */
    public ConfigMap getMetricsCm() {
        return metricsCm;
    }

    /**
     * Sets the metrics config map
     *
     * @param metricsCm     Config Map with metrics configuration
     */
    public void setMetricsCm(ConfigMap metricsCm) {
        this.metricsCm = metricsCm;
    }

    /**
     * @return  Logging config map
     */
    public ConfigMap getLoggingCm() {
        return loggingCm;
    }

    /**
     * Sets the logging config map
     *
     * @param loggingCm Config Map with logging configuration
     */
    public void setLoggingCm(ConfigMap loggingCm) {
        this.loggingCm = loggingCm;
    }
}
