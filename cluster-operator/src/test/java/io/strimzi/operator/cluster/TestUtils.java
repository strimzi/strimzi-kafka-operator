/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetricsBuilder;

import java.util.Map;
import java.util.stream.Collectors;

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

    /**
     * Gets the given container's environment as a Map. This makes it easier to verify the environment variables in
     * unit tests.
     *
     * @param container The container to retrieve the EnvVars from
     *
     * @return A map of the environment variables of the given container. The Environmental variable values indexed by
     * their names
     */
    public static Map<String, String> containerEnvVars(Container container) {
        return container.getEnv().stream().collect(
                Collectors.toMap(EnvVar::getName, EnvVar::getValue,
                        // On duplicates, last-in wins
                        (u, v) -> v));
    }
}
