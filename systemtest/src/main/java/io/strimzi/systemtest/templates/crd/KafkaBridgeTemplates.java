/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.crd;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeBuilder;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.utils.FileUtils;

public class KafkaBridgeTemplates {
    private static final String METRICS_BRIDGE_CONFIG_MAP_SUFFIX = "-bridge-metrics";
    private static final String CONFIG_MAP_KEY = "metrics-config.yml";

    private KafkaBridgeTemplates() {}

    private final static int DEFAULT_HTTP_PORT = 8080;

    public static KafkaBridgeBuilder kafkaBridge(
        String namespaceName,
        String bridgeName,
        String bootstrap,
        int kafkaBridgeReplicas
    ) {
        return defaultKafkaBridge(namespaceName, bridgeName, bootstrap, kafkaBridgeReplicas);
    }

    public static KafkaBridgeBuilder kafkaBridgeWithCors(
        String namespaceName,
        String bridgeName,
        String bootstrap,
        int kafkaBridgeReplicas,
        String allowedCorsOrigin,
        String allowedCorsMethods
    ) {
        return defaultKafkaBridge(namespaceName, bridgeName, bootstrap, kafkaBridgeReplicas)
            .editSpec()
                .editHttp()
                    .withNewCors()
                        .withAllowedOrigins(allowedCorsOrigin)
                        .withAllowedMethods(allowedCorsMethods != null ? allowedCorsMethods : "GET,POST,PUT,DELETE,OPTIONS,PATCH")
                    .endCors()
                .endHttp()
            .endSpec();
    }

    public static KafkaBridgeBuilder kafkaBridgeWithMetrics(
        String namespaceName,
        String bridgeName,
        String bootstrap,
        int kafkaBridgeReplicas
    ) {
        return defaultKafkaBridge(namespaceName, bridgeName, bootstrap, kafkaBridgeReplicas)
            .editSpec()
                .withNewJmxPrometheusExporterMetricsConfig()
                    .withNewValueFrom()
                        .withNewConfigMapKeyRef(CONFIG_MAP_KEY, getConfigMapName(bridgeName), false)
                    .endValueFrom()
                .endJmxPrometheusExporterMetricsConfig()
            .endSpec();
    }

    public static ConfigMap bridgeMetricsConfigMap(String namespaceName, String bridgeName) {
        return new ConfigMapBuilder(FileUtils.extractConfigMapFromYAMLWithResources(TestConstants.PATH_TO_KAFKA_BRIDGE_METRICS_CONFIG, "bridge-metrics"))
            .editOrNewMetadata()
                .withNamespace(namespaceName)
                .withName(getConfigMapName(bridgeName))
            .endMetadata()
            .build();
    }

    private static String getConfigMapName(String kafkaConnectClusterName) {
        return kafkaConnectClusterName + METRICS_BRIDGE_CONFIG_MAP_SUFFIX;
    }

    private static KafkaBridgeBuilder defaultKafkaBridge(
        String namespaceName,
        String bridgeName,
        String bootstrap,
        int kafkaBridgeReplicas
    ) {
        return new KafkaBridgeBuilder()
            .withNewMetadata()
                .withName(bridgeName)
                .withNamespace(namespaceName)
            .endMetadata()
            .withNewSpec()
                .withBootstrapServers(bootstrap)
                .withReplicas(kafkaBridgeReplicas)
                .withNewInlineLogging()
                    .addToLoggers("bridge.root.logger", "DEBUG")
                .endInlineLogging()
                .withNewHttp()
                    .withPort(DEFAULT_HTTP_PORT)
                .endHttp()
            .endSpec();
    }
}
