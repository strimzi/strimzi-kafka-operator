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
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetricsBuilder;

import java.util.List;
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

    /**
     * If present, adds the HTTP/HTTPS proxy environment variables to the list.
     *
     * @param envVars List of environment variables.
     */
    public static void maybeAddHttpProxyEnvVars(List<EnvVar> envVars) {
        if (System.getenv(ClusterOperatorConfig.HTTP_PROXY) != null) {
            envVars.add(new EnvVarBuilder()
                .withName(ClusterOperatorConfig.HTTP_PROXY)
                .withValue(System.getenv(ClusterOperatorConfig.NO_PROXY))
                .build());
        }
        if (System.getenv(ClusterOperatorConfig.HTTPS_PROXY) != null) {
            envVars.add(new EnvVarBuilder()
                .withName(ClusterOperatorConfig.HTTPS_PROXY)
                .withValue(System.getenv(ClusterOperatorConfig.HTTPS_PROXY))
                .build());
        }
        if (System.getenv(ClusterOperatorConfig.NO_PROXY) != null) {
            envVars.add(new EnvVarBuilder()
                .withName(ClusterOperatorConfig.NO_PROXY)
                .withValue(System.getenv(ClusterOperatorConfig.NO_PROXY))
                .build());
        }
    }
}
