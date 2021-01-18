/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;

public class MetricsAndLoggingCm {
    private ConfigMap metricsCm;
    private ConfigMap loggingCm;
    public MetricsAndLoggingCm(ConfigMap metricsCm, ConfigMap loggingCm) {
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
