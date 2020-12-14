/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;

public class MetricsAndLoggingCm {
    public ConfigMap metricsCm;
    public ConfigMap loggingCm;
    public MetricsAndLoggingCm(ConfigMap metricsCm, ConfigMap loggingCm) {
        this.metricsCm = metricsCm;
        this.loggingCm = loggingCm;
    }
}
