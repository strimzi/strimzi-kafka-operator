/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetricsBuilder;
import io.strimzi.operator.cluster.model.AbstractModel;

import static java.util.Collections.singletonMap;

public class TestUtils {
    public static JmxPrometheusExporterMetrics getJmxPrometheusExporterMetrics(String key, String name) {
        JmxPrometheusExporterMetrics metricsConfig = new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                .withNewConfigMapKeyRef(key, name, true)
                .endValueFrom()
                .build();
        return metricsConfig;
    }

    public static ConfigMap getJmxMetricsCm(String data, String metricsCMName) {
        ConfigMap metricsCM = new ConfigMapBuilder()
                .withNewMetadata()
                .withName(metricsCMName)
                .endMetadata()
                .withData(singletonMap(AbstractModel.ANCILLARY_CM_KEY_METRICS, data))
                .build();
        return metricsCM;
    }
}
