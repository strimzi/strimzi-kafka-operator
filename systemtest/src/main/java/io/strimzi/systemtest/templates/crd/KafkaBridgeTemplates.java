/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.crd;

import io.strimzi.api.kafka.model.bridge.KafkaBridgeBuilder;

public class KafkaBridgeTemplates {

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
                .withEnableMetrics(true)
            .endSpec();
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
