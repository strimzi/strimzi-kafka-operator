/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetricsBuilder;

import static java.util.Collections.singletonMap;

public class TestUtils {
    public static JmxPrometheusExporterMetrics getJmxPrometheusExporterMetrics(String key, String name) {
        return new JmxPrometheusExporterMetricsBuilder()
                .withNewValueFrom()
                    .withConfigMapKeyRef(new ConfigMapKeySelectorBuilder()
                            .withName(name)
                            .withKey(key)
                            .withOptional(true)
                            .build())
                .endValueFrom()
                .build();
    }

    public static ConfigMap getJmxMetricsCm(String data, String metricsCMName, String metricsConfigYaml) {
        return new ConfigMapBuilder()
                .withNewMetadata()
                .withName(metricsCMName)
                .endMetadata()
                .withData(singletonMap(metricsConfigYaml, data))
                .build();
    }

}
