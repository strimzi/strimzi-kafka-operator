/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.crd;

import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeBuilder;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.test.TestUtils;

public class KafkaBridgeTemplates {

    private KafkaBridgeTemplates() {}

    public static KafkaBridgeBuilder kafkaBridge(String name, String bootstrap, int kafkaBridgeReplicas) {
        return kafkaBridge(name, name, bootstrap, kafkaBridgeReplicas);
    }

    public static KafkaBridgeBuilder kafkaBridge(String name, String clusterName, String bootstrap, int kafkaBridgeReplicas) {
        KafkaBridge kafkaBridge = getKafkaBridgeFromYaml(TestConstants.PATH_TO_KAFKA_BRIDGE_CONFIG);
        return defaultKafkaBridge(kafkaBridge, name, clusterName, bootstrap, kafkaBridgeReplicas);
    }

    public static KafkaBridgeBuilder kafkaBridgeWithCors(String name, String bootstrap, int kafkaBridgeReplicas,
                                                          String allowedCorsOrigin, String allowedCorsMethods) {
        return kafkaBridgeWithCors(name, name, bootstrap, kafkaBridgeReplicas, allowedCorsOrigin, allowedCorsMethods);
    }

    public static KafkaBridgeBuilder kafkaBridgeWithCors(String name, String clusterName, String bootstrap,
                                                  int kafkaBridgeReplicas, String allowedCorsOrigin,
                                                  String allowedCorsMethods) {
        KafkaBridge kafkaBridge = getKafkaBridgeFromYaml(TestConstants.PATH_TO_KAFKA_BRIDGE_CONFIG);

        KafkaBridgeBuilder kafkaBridgeBuilder = defaultKafkaBridge(kafkaBridge, name, clusterName, bootstrap, kafkaBridgeReplicas);

        kafkaBridgeBuilder
            .editSpec()
                .editHttp()
                    .withNewCors()
                        .withAllowedOrigins(allowedCorsOrigin)
                        .withAllowedMethods(allowedCorsMethods != null ? allowedCorsMethods : "GET,POST,PUT,DELETE,OPTIONS,PATCH")
                    .endCors()
                .endHttp()
            .endSpec();

        return kafkaBridgeBuilder;
    }

    public static KafkaBridgeBuilder kafkaBridgeWithMetrics(String name, String clusterName, String bootstrap) {
        return kafkaBridgeWithMetrics(name, clusterName, bootstrap, 1);
    }

    public static KafkaBridgeBuilder kafkaBridgeWithMetrics(String name, String clusterName, String bootstrap, int kafkaBridgeReplicas) {
        KafkaBridge kafkaBridge = getKafkaBridgeFromYaml(TestConstants.PATH_TO_KAFKA_BRIDGE_CONFIG);

        return defaultKafkaBridge(kafkaBridge, name, clusterName, bootstrap, kafkaBridgeReplicas)
            .editSpec()
                .withEnableMetrics(true)
            .endSpec();
    }

    private static KafkaBridgeBuilder defaultKafkaBridge(KafkaBridge kafkaBridge, String name, String kafkaClusterName, String bootstrap, int kafkaBridgeReplicas) {
        return new KafkaBridgeBuilder(kafkaBridge)
            .withNewMetadata()
                .withName(name)
                .withNamespace(ResourceManager.kubeClient().getNamespace())
            .endMetadata()
            .editSpec()
                .withBootstrapServers(bootstrap)
                .withReplicas(kafkaBridgeReplicas)
                .withNewInlineLogging()
                    .addToLoggers("bridge.root.logger", "DEBUG")
                .endInlineLogging()
            .endSpec();
    }

    private static KafkaBridge getKafkaBridgeFromYaml(String yamlPath) {
        return TestUtils.configFromYaml(yamlPath, KafkaBridge.class);
    }
}
