/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.fabric8.kubernetes.api.model.ConfigMap;

public class MetricsAndLogging {
    private ConfigMap metricsCm;
    private ConfigMap loggingCm;
    public MetricsAndLogging(ConfigMap metricsCm, ConfigMap loggingCm) {
        this.setMetricsCm(metricsCm);
        this.setLoggingCm(loggingCm);
    }

    public ConfigMap getMetricsCm() {
        return metricsCm;
    }

    public void setMetricsCm(ConfigMap metricsCm) {
        this.metricsCm = metricsCm;
    }

    public ConfigMap getLoggingCm() {
        return loggingCm;
    }

    public void setLoggingCm(ConfigMap loggingCm) {
        this.loggingCm = loggingCm;
    }
}
